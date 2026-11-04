import pika
import json
import datetime

def send_batch_tasks(num_messages=10):
    # 1. Establish a connection to RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    # 2. Declare the durable queue
    channel.queue_declare(queue='task_queue', durable=True)

    # 3. Generate and send the messages
    for i in range(1, num_messages + 1):
        # Construct the requested dictionary structure
        message_dict = {
            "id": i,
            "text": f"Process this request {i}",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        # Serialize the dictionary to a JSON string
        message_body = json.dumps(message_dict)

        # Publish to the queue
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                content_type='application/json' # Best practice for JSON payloads
            )
        )
        print(f" [x] Sent {message_body}")

    # 4. Close the connection
    connection.close()

if __name__ == '__main__':
    send_batch_tasks(10)