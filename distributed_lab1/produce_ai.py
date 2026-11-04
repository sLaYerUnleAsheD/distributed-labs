from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pika
import json
import uuid
import os
import uvicorn

app = FastAPI()
DB_FILE = "results.json"

class ProcessRequest(BaseModel):
    text: str

def update_db(req_id, status, result=None):
    """Atomically updates the JSON file to prevent read/write collisions."""
    data = {}
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r') as f:
                data = json.load(f)
        except Exception:
            pass
    
    data[req_id] = {"status": status}
    if result:
        data[req_id]["result"] = result
        
    # Write to a temp file and replace to ensure atomic writes
    with open(DB_FILE + ".tmp", 'w') as f:
        json.dump(data, f)
    os.replace(DB_FILE + ".tmp", DB_FILE)

def get_db(req_id):
    if not os.path.exists(DB_FILE):
        return None
    with open(DB_FILE, 'r') as f:
        data = json.load(f)
    return data.get(req_id)

@app.post("/process")
def process_text(req: ProcessRequest):
    req_id = str(uuid.uuid4())
    
    # 1. Store initial status
    update_db(req_id, "processing")
    
    # 2. Enqueue the task
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # Must match the DLQ configuration established by the consumer
    queue_args = {
        'x-dead-letter-exchange': 'dlx',
        'x-dead-letter-routing-key': 'task_queue_dlq'
    }
    channel.queue_declare(queue='task_queue', durable=True, arguments=queue_args)
    
    message = {
        "id": req_id,
        "text": req.text,
        "retry_count": 0
    }
    
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        )
    )
    connection.close()
    
    return {"id": req_id}

@app.get("/result/{id}")
def get_result(id: str):
    record = get_db(id)
    if not record:
        raise HTTPException(status_code=404, detail="Request ID not found")
    return record

if __name__ == "__main__":
    # Must run on port 8080
    uvicorn.run(app, host="0.0.0.0", port=8080)