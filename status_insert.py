import sqlite3
import json
from datetime import datetime
import logging
from threading import Thread
from document_storage_service.config import RABBITMQ_HOST, STATUS_QUEUE
import pika
import json
from typing import Dict, Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMQClient:
    def __init__(self, host: str, queue: str):
        self.host = host
        self.queue = queue

    def _get_connection(self):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        except Exception as e:
            logger.error(f"Error connecting to RabbitMQ: {e}")
            raise

    def _ensure_queue(self, channel):
        try:
            # First, try to declare the queue as durable
            channel.queue_declare(queue=self.queue, durable=True)
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 406:  # PRECONDITION_FAILED
                logger.warning(f"Queue '{self.queue}' already exists with different properties. Using existing queue.")
                # Reopen the channel as it was closed due to the exception
                channel = channel.connection.channel()
                # Declare the queue with passive=True to ensure it exists without modifying its properties
                channel.queue_declare(queue=self.queue, passive=True)
            else:
                raise
        return channel

    def send_message(self, message: Dict):
        try:
            with self._get_connection() as connection:
                channel = connection.channel()
                channel = self._ensure_queue(channel)
                channel.basic_publish(
                    exchange='',
                    routing_key=self.queue,
                    body=json.dumps(message),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    )
                )
            logger.info(f"Sent message: {message}")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise

    def start_consuming(self, callback: Callable):
        def internal_callback(ch, method, properties, body):
            try:
                message = json.loads(body)
                logger.info(f"Received message: {message}")
                callback(ch, method, properties, body)  # Pass all args to the callback
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode message body: {body}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        while True:
            try:
                with self._get_connection() as connection:
                    channel = connection.channel()
                    channel = self._ensure_queue(channel)
                    channel.basic_qos(prefetch_count=1)
                    channel.basic_consume(queue=self.queue, on_message_callback=internal_callback)
                    logger.info('Started consuming messages. To exit press CTRL+C')
                    channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"AMQP Connection Error: {e}. Retrying...")
            except Exception as e:
                logger.error(f"Unexpected error in consumer: {e}. Retrying...")

    def check_health(self):
        try:
            with self._get_connection() as connection:
                channel = connection.channel()
                self._ensure_queue(channel)
            return True  # If successful, the service is healthy
        except Exception as e:
            logger.error(f"RabbitMQ health check failed: {e}")
            return False


# Database setup
def init_db():
    conn = sqlite3.connect('/root/code/resume-search-microservices/operations.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS operations
                 (id TEXT PRIMARY KEY, operation TEXT, status TEXT, details TEXT, timestamp DATETIME)''')
    conn.commit()
    conn.close()

init_db()

def update_status(operation_id, status, details):
    conn = sqlite3.connect('/root/code/resume-search-microservices/operations.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO operations (id, operation, status, details, timestamp) VALUES (?, ?, ?, ?, ?)",
              (operation_id, details.get('operation'), status, json.dumps(details), datetime.now()))
    conn.commit()
    conn.close()


# The callback must match the internal RabbitMQ callback signature
def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        update_status(message['id'], message['status'], message['details'])
    except Exception as e:
        logger.error(f"Error in callback: {e}")


def status_queue_consumer():
    status_queue_client = RabbitMQClient(
        host=RABBITMQ_HOST,
        queue=STATUS_QUEUE
    )
    status_queue_client.start_consuming(callback)


if __name__ == '__main__':
    # Start the status queue consumer
    status_thread = Thread(target=status_queue_consumer)
    status_thread.start()

    try:
        # Keep the main thread alive
        status_thread.join()
    except KeyboardInterrupt:
        logger.info("Shutting down status insert service")
