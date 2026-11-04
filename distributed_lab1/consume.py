import pika
import json
import time
from datetime import datetime, timezone

def callback(ch, method, properties, body):
    # Parse the incoming JSON payload
    message_data = json.loads(body.decode('utf-8'))
    
    # Extract message fields
    msg_id = message_data.get('id')
    msg_text = message_data.get('text')
    
    # Generate current processing timestamp
    processed_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Log the required information
    print(f" [x] ID: {msg_id} | Text: '{msg_text}' | Processed at: {processed_time}")
    
    # Throttle processing to 1 message per second
    time.sleep(1)
    
    # Manually acknowledge the message so RabbitMQ removes it from the queue
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # Declare the same durable queue as the producer
    channel.queue_declare(queue='task_queue', durable=True)

    # Prevent RabbitMQ from pushing all messages to this worker's memory at once
    channel.basic_qos(prefetch_count=1)

    # Bind the queue to the callback (auto_ack=False is the default in newer pika versions)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        start_consumer()
    except KeyboardInterrupt:
        print("\nConsumer cleanly stopped.")