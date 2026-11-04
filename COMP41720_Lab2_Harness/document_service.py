from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import uvicorn, requests, pika, json, uuid, threading, time, os
import threading

app = FastAPI(title="Document Service")
DB_FILE = "database.json"

db_lock = threading.Lock() #for test 3, to avoid race conditions when reading/writing the database

def init_db():
    if not os.path.exists(DB_FILE):
        with open(DB_FILE, 'w') as f:
            json.dump({"documents": {}, "outbox": {}, "processed_msgs": []}, f)

def read_db():
    with open(DB_FILE, 'r') as f: return json.load(f)

def write_db(data):
    with open(DB_FILE + ".tmp", 'w') as f: json.dump(data, f)
    os.replace(DB_FILE + ".tmp", DB_FILE)

class DocumentRequest(BaseModel):
    text: str

@app.post("/documents", status_code=status.HTTP_202_ACCEPTED)
def create_document(doc: DocumentRequest):
    doc_id = str(uuid.uuid4())
    
    # 1. Synchronous Quota Reservation (with correlation ID)
    requests.post("http://127.0.0.1:8081/quota/reserve", 
                  json={"tenant_id": "tenant_1", "correlation_id": doc_id}).raise_for_status()

    # 2. Atomic Database Write (Document + Outbox)
    # Lock the entire transaction
    with db_lock:
        db = read_db()
        db["documents"][doc_id] = {"status": "PENDING", "text": doc.text}
        
        # Generate a unique Message ID for RabbitMQ deduplication downstream
        msg_id = str(uuid.uuid4())
        db["outbox"][msg_id] = {
            "event_type": "document.created",
            "payload": {"id": doc_id, "text": doc.text},
            "published": False
        }
        write_db(db)
    
    return {"id": doc_id, "status": "PENDING"}

@app.get("/documents/{id}/status")
def get_status(id: str):
    with db_lock:
        db = read_db()
    if id not in db["documents"]: raise HTTPException(status_code=404)
    return {"id": id, "status": db["documents"][id]["status"]}

def relay_poller():
    while True:
        try:
            with db_lock:
                db = read_db()
                for msg_id, record in db["outbox"].items():
                    if not record["published"]:
                        conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                        ch = conn.channel()
                        ch.exchange_declare(exchange='saga_events', exchange_type='topic')
                        ch.basic_publish(
                            exchange='saga_events',
                            routing_key=record["event_type"],
                            body=json.dumps(record["payload"]),
                            # Set standard AMQP properties for deduplication
                            properties=pika.BasicProperties(
                                message_id=msg_id, 
                                correlation_id=record["payload"]["id"],
                                delivery_mode=2
                            )
                        )
                        conn.close()

                        # TEMPORARY CRASH INJECTION FOR TEST 2
                        # print("[Relay] 💥 Crashing before marking published!")
                        # os._exit(1)

                        record["published"] = True
                write_db(db)
        except Exception: pass
        time.sleep(2)

def listen_for_saga_replies():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.exchange_declare(exchange='saga_events', exchange_type='topic')
    ch.queue_declare(queue='doc_saga_replies', durable=True)
    ch.queue_bind(exchange='saga_events', queue='doc_saga_replies', routing_key='embedding.*')

    def callback(ch, method, properties, body):
        with db_lock:
            db = read_db()
            msg_id = properties.message_id
            
            # IDEMPOTENCY CHECK
            if msg_id in db.get("processed_msgs", []):
                print(f"[Doc Service] Ignored duplicate saga reply {msg_id}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            event = json.loads(body)
            doc_id = properties.correlation_id # Read correlation ID from headers
            
            if method.routing_key == 'embedding.completed':
                db["documents"][doc_id]['status'] = "COMPLETED"
                print(f"[Saga] Success! Document {doc_id} COMPLETED")
                
            elif method.routing_key == 'embedding.failed':
                db["documents"][doc_id]['status'] = "FAILED"
                print(f"[Saga] Failed! Document {doc_id} FAILED. Compensating...")
                # SAGA COMPENSATION
                requests.post("http://127.0.0.1:8081/quota/release", 
                            json={"tenant_id": "tenant_1", "correlation_id": doc_id})
                
            db.setdefault("processed_msgs", []).append(msg_id)
            write_db(db)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue='doc_saga_replies', on_message_callback=callback)
    ch.start_consuming()

if __name__ == "__main__":
    init_db()
    threading.Thread(target=relay_poller, daemon=True).start()
    threading.Thread(target=listen_for_saga_replies, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8082)

# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# import uvicorn
# import requests
# import pika
# import json
# import uuid
# import threading
# import time
# import os

# app = FastAPI(title="Document Service")
# DB_FILE = "database.json"

# # Initialize our persistent local database
# def init_db():
#     if not os.path.exists(DB_FILE):
#         with open(DB_FILE, 'w') as f:
#             json.dump({"documents": {}, "outbox": {}, "search_projection": {}}, f)

# def read_db():
#     with open(DB_FILE, 'r') as f:
#         return json.load(f)

# def write_db(data):
#     # Atomic write to prevent file corruption during our intentional crashes
#     with open(DB_FILE + ".tmp", 'w') as f:
#         json.dump(data, f)
#     os.replace(DB_FILE + ".tmp", DB_FILE)

# class DocumentRequest(BaseModel):
#     text: str
#     use_outbox: bool = True          # Toggle between dual-write flaw and outbox fix
#     crash_dual_write: bool = False   # Exercise 2.1 & 2.2 flag
#     crash_relay: bool = False        # Exercise 2.2 flag

# def publish_event(routing_key, payload):
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()
#     channel.exchange_declare(exchange='saga_events', exchange_type='topic')
#     channel.basic_publish(
#         exchange='saga_events',
#         routing_key=routing_key,
#         body=json.dumps(payload),
#         properties=pika.BasicProperties(delivery_mode=2)
#     )
#     connection.close()

# @app.post("/documents")
# def create_document(doc: DocumentRequest):
#     requests.post("http://localhost:8081/quota/reserve").raise_for_status()

#     doc_id = str(uuid.uuid4())
#     db = read_db()
    
#     # 1. Write the document record
#     db["documents"][doc_id] = {
#         "status": "PENDING", 
#         "text": doc.text,
#         "created_at": time.time() # Used for Exercise 2.3
#     }

#     if doc.use_outbox:
#         # EXERCISE 2.2: The Transactional Outbox Fix
#         # We write the outbox record in the exact same local transaction (file write)
#         db["outbox"][doc_id] = {
#             "event_type": "document.created",
#             "payload": {"id": doc_id, "text": doc.text},
#             "published": False,
#             "crash_relay": doc.crash_relay
#         }
#         write_db(db)
        
#         if doc.crash_dual_write:
#             print("[Doc Service] 💥 CRASHING right after atomic write! (Simulated power loss)")
#             os._exit(1)
            
#     else:
#         # EXERCISE 2.1: The Dual-Write Problem
#         write_db(db) # Local transaction commits
        
#         if doc.crash_dual_write:
#             print("[Doc Service] 💥 CRASHING between DB write and RabbitMQ publish!")
#             os._exit(1)
            
#         # If we didn't crash, we do the unsafe second write over the network
#         publish_event('document.created', {"id": doc_id, "text": doc.text})

#     return {"id": doc_id, "status": "PENDING"}

# @app.get("/search/{id}")
# def get_search_projection(id: str):
#     db = read_db()
#     return db["search_projection"].get(id, {"error": "Not indexed yet"})

# # --- EXERCISE 2.2: RELAY POLLER ---
# def relay_poller():
#     while True:
#         try:
#             db = read_db()
#             for doc_id, record in db["outbox"].items():
#                 if not record["published"]:
#                     print(f"[Relay] Found unpublished event for {doc_id}. Publishing...")
#                     publish_event(record["event_type"], record["payload"])
                    
#                     if record.get("crash_relay"):
#                         print("[Relay] 💥 CRASHING after publish but before marking published!")
#                         os._exit(1)
                        
#                     # Mark published safely
#                     record["published"] = True
#                     write_db(db)
#                     print(f"[Relay] Event for {doc_id} marked as published.")
#         except Exception:
#             pass
#         time.sleep(2)

# # --- EXERCISE 2.3: SEARCH PROJECTION SAGA CONSUMER ---
# def listen_for_saga_replies():
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()
#     channel.exchange_declare(exchange='saga_events', exchange_type='topic')
#     channel.queue_declare(queue='doc_saga_replies', durable=True)
#     channel.queue_bind(exchange='saga_events', queue='doc_saga_replies', routing_key='embedding.*')

#     def callback(ch, method, properties, body):
#         event = json.loads(body)
#         doc_id = event['id']
#         db = read_db()
        
#         if method.routing_key == 'embedding.completed':
#             db["documents"][doc_id]['status'] = "COMPLETED"
            
#             # Record Eventual Consistency Lag
#             created_at = db["documents"][doc_id]["created_at"]
#             lag_ms = (time.time() - created_at) * 1000
            
#             db["search_projection"][doc_id] = {
#                 "vector": event["vector"],
#                 "lag_ms": lag_ms
#             }
#             write_db(db)
#             print(f"[Search] Document {doc_id} searchable. Inconsistency window: {lag_ms:.2f} ms")
            
#         elif method.routing_key == 'embedding.failed':
#             db["documents"][doc_id]['status'] = "FAILED"
#             write_db(db)
#             requests.post("http://localhost:8081/quota/release")
            
#         ch.basic_ack(delivery_tag=method.delivery_tag)

#     channel.basic_consume(queue='doc_saga_replies', on_message_callback=callback)
#     channel.start_consuming()

# if __name__ == "__main__":
#     init_db()
#     threading.Thread(target=relay_poller, daemon=True).start()
#     threading.Thread(target=listen_for_saga_replies, daemon=True).start()
#     uvicorn.run(app, host="0.0.0.0", port=8082)