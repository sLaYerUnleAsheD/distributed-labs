import pika
import json
import os
import ollama
import time

DB_FILE = "results.json"
MAX_RETRIES = 3

# Choose the model you pulled via the CLI
MODEL_NAME = "llama3.2:1b" 

def setup_queues(channel):
    """Configures the DLQ topology and the main task queue."""
    # 1. Declare the Dead Letter Exchange (DLX)
    channel.exchange_declare(exchange='dlx', exchange_type='direct')
    
    # 2. Declare the Dead Letter Queue (DLQ)
    channel.queue_declare(queue='dead_letter_queue', durable=True)
    
    # 3. Bind the DLQ to the DLX
    channel.queue_bind(
        exchange='dlx', 
        queue='dead_letter_queue', 
        routing_key='task_queue_dlq'
    )
    
    # 4. Declare the main queue, pointing its dead letters to the DLX
    queue_args = {
        'x-dead-letter-exchange': 'dlx',
        'x-dead-letter-routing-key': 'task_queue_dlq'
    }
    channel.queue_declare(queue='task_queue', durable=True, arguments=queue_args)

def update_db(req_id, status, result=None):
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
    with open(DB_FILE + ".tmp", 'w') as f:
        json.dump(data, f)
    os.replace(DB_FILE + ".tmp", DB_FILE)

def process_message(ch, method, properties, body):
    payload = json.loads(body.decode('utf-8'))
    req_id = payload['id']
    text = payload['text']
    retry_count = payload.get('retry_count', 0)
    
    print(f" [x] Processing {req_id} (Attempt {retry_count + 1})")
    
    try:
        # Call local Ollama instance instead of OpenAI
        # for failure
        client = ollama.Client(host='http://localhost:9999')
        response = client.chat(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": text}]
        )
        # response = ollama.chat(
        #     model=MODEL_NAME,
        #     messages=[{"role": "user", "content": text}]
        # )
        
        # Extract the response text
        ai_result = response['message']['content']

        # for atleast-once-delivery, sleep and kill consumer to simulate a crash after processing but before acking
        print(f" [!] Simulating work for 10 seconds...")
        time.sleep(10)
        
        # Success: Update DB and Acknowledge
        update_db(req_id, "completed", ai_result)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f" [✓] Completed {req_id}")

    except Exception as e:
        print(f" [!] Ollama Error on {req_id}: {str(e)}")
        
        if retry_count < MAX_RETRIES:
            # Republish with incremented retry count
            payload['retry_count'] += 1
            ch.basic_publish(
                exchange='',
                routing_key='task_queue',
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2) # Persistent
            )
            # Ack the original message since we successfully requeued it
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f" [↻] Requeued {req_id} (Attempt {payload['retry_count']}/{MAX_RETRIES})")
        else:
            # EXHAUSTED RETRIES: Reject without requeuing.
            # Because of the queue arguments, RabbitMQ natively routes this to the DLQ!
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            print(f" [X] Sent {req_id} to Dead Letter Queue")
            
        # Acknowledge the original message so it doesn't loop infinitely
        # ch.basic_ack(delivery_tag=method.delivery_tag)
        # not required since we either acked or rejected above

def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    setup_queues(channel)
    
    # Process one message at a time so local RAM isn't overwhelmed by multiple LLM generations
    channel.basic_qos(prefetch_count=1)
    
    channel.basic_consume(queue='task_queue', on_message_callback=process_message)
    print(f' [*] Consumer running. Waiting for tasks to send to {MODEL_NAME}...')
    channel.start_consuming()

if __name__ == '__main__':
    start_consumer()

'''
requests or documentation purpose whose results are stored in a local JSON file. The consumer processes messages from RabbitMQ, interacts with a local Ollama instance for AI processing, and handles retries and dead-lettering for failed tasks.
curl -X POST http://localhost:8080/process      -H "Content-Type: application/json"      -d '{"text": "Translate this sentence into French."}'
curl -X POST http://localhost:8080/process -H "Content-Type: application/json" -d '{"text": "Define Apple."}'
curl -X POST http://localhost:8080/process -H "Content-Type: application/json" -d '{"text": "say hi in spanish"}'
'''