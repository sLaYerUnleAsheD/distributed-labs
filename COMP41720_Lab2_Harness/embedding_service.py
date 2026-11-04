import pika, json, os, uuid, threading, time, hashlib
from fastapi import FastAPI
import uvicorn

app = FastAPI(title="Embedding Service")
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.0"))

# Idempotency store (in memory for this lab, usually Redis/DB in prod)
processed_messages = set()

def generate_mock_embedding(text: str):
    time.sleep(0.2)
    return [float(b)/255.0 for b in hashlib.sha256(text.encode('utf-8')).digest()[:8]]

def process_embeddings():
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.exchange_declare(exchange='saga_events', exchange_type='topic')
    ch.queue_declare(queue='embedding_tasks', durable=True)
    ch.queue_bind(exchange='saga_events', queue='embedding_tasks', routing_key='document.created')
    ch.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        msg_id = properties.message_id
        doc_id = properties.correlation_id

        # IDEMPOTENCY CHECK
        if msg_id in processed_messages:
            print(f"[Embedding] Ignored duplicate task {msg_id}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f"[Embedding] Processing {doc_id}...")
        event = json.loads(body)
        
        # Determine success or failure based on env var
        is_failure = (FAILURE_RATE == 1.0)
        
        # Prepare standard AMQP properties for the reply
        reply_props = pika.BasicProperties(
            message_id=str(uuid.uuid4()), # Generate unique ID for this reply event
            correlation_id=doc_id,        # Keep the original doc_id as correlation
            delivery_mode=2
        )

        if is_failure:
            print(f"[Embedding] ❌ FAULT INJECTED for {doc_id}!")
            ch.basic_publish(
                exchange='saga_events', routing_key='embedding.failed',
                body=json.dumps({"reason": "Simulated hardware fault"}),
                properties=reply_props
            )
        else:
            vector = generate_mock_embedding(event.get('text', ''))
            print(f"[Embedding] ✅ Success for {doc_id}.")
            ch.basic_publish(
                exchange='saga_events', routing_key='embedding.completed',
                body=json.dumps({"vector": vector}),
                properties=reply_props
            )
            
        processed_messages.add(msg_id)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue='embedding_tasks', on_message_callback=callback)
    print(f"[*] Embedding Service listening... (Failure Rate: {FAILURE_RATE*100}%)")
    ch.start_consuming()

if __name__ == "__main__":
    threading.Thread(target=process_embeddings, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8083)
# from fastapi import FastAPI
# import uvicorn
# import pika
# import json
# import os
# import random
# import threading
# import time
# import hashlib

# app = FastAPI(title="Embedding Service (Simulated)")

# # Read failure rate from env variable (default 0.5 for a 50% failure rate)
# FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.5"))

# def generate_mock_embedding(text: str) -> list:
#     """
#     Simulates an AI embedding model locally.
#     Hashes the text into a deterministic, fixed-size vector and sleeps for 200ms.
#     """
#     # Create a SHA-256 hash of the text
#     hash_bytes = hashlib.sha256(text.encode('utf-8')).digest()
    
#     # Take the first 8 bytes and normalize them to floats between 0.0 and 1.0
#     vector = [float(b) / 255.0 for b in hash_bytes[:8]]
    
#     # Simulate the compute time specified by the lab
#     time.sleep(0.2) 
    
#     return vector

# def process_embeddings():
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()
#     channel.exchange_declare(exchange='saga_events', exchange_type='topic')
    
#     channel.queue_declare(queue='embedding_tasks', durable=True)
#     channel.queue_bind(exchange='saga_events', queue='embedding_tasks', routing_key='document.created')

#     # Ensure round-robin load balancing works properly for multiple workers
#     channel.basic_qos(prefetch_count=1)

#     def callback(ch, method, properties, body):
#         event = json.loads(body)
#         doc_id = event['id']
#         text = event.get('text', '')
        
#         print(f"[Embedding Service] Processing document {doc_id}...")
        
#         # 1. Run the local simulation (No API dependency!)
#         vector = generate_mock_embedding(text)
#         print(f"[Embedding Service] Generated vector: {vector}")
        
#         # 2. Inject random failure to test the Saga compensation
#         if random.random() < FAILURE_RATE:
#             print(f"[Embedding Service] ❌ FAULT INJECTED for {doc_id}! Emitting failure event.")
#             ch.basic_publish(
#                 exchange='saga_events',
#                 routing_key='embedding.failed',
#                 body=json.dumps({"id": doc_id, "reason": "Simulated hardware fault"})
#             )
#         else:
#             print(f"[Embedding Service] ✅ Success for {doc_id}. Emitting completion event.")
#             ch.basic_publish(
#                 exchange='saga_events',
#                 routing_key='embedding.completed',
#                 body=json.dumps({"id": doc_id, "vector": vector})
#             )
            
#         ch.basic_ack(delivery_tag=method.delivery_tag)

#     channel.basic_consume(queue='embedding_tasks', on_message_callback=callback)
#     print(f"[*] Embedding Service listening... (Failure Rate: {FAILURE_RATE*100}%)")
#     channel.start_consuming()

# if __name__ == "__main__":
#     threading.Thread(target=process_embeddings, daemon=True).start()
#     uvicorn.run(app, host="0.0.0.0", port=8083)