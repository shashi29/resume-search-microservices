import pika
import json
from typing import Dict, Callable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMQClient:
    def __init__(self, host: str, queue: str):
        self.host = host
        self.queue = queue

    def _get_connection(self):
        return pika.BlockingConnection(pika.ConnectionParameters(host=self.host))

    def send_message(self, message: Dict):
        try:
            with self._get_connection() as connection:
                channel = connection.channel()
                channel.queue_declare(queue=self.queue)
                channel.basic_publish(
                    exchange='',
                    routing_key=self.queue,
                    body=json.dumps(message)
                )
            logger.info(f"Sent message: {message}")
        except Exception as e:
            logger.error(f"Error sending message: {e}")

    def start_consumer(self, callback: Callable):
        def internal_callback(ch, method, properties, body):
            message = json.loads(body)
            logger.info(f"Received message: {message}")
            callback(message)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        try:
            with self._get_connection() as connection:
                channel = connection.channel()
                channel.queue_declare(queue=self.queue)
                channel.basic_consume(queue=self.queue, on_message_callback=internal_callback)
                logger.info('Waiting for messages. To exit press CTRL+C')
                channel.start_consuming()
        except Exception as e:
            logger.error(f"Error in consumer: {e}")

    def check_health(self):
        try:
            # Try to create a connection and declare the queue
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            channel = connection.channel()
            channel.queue_declare(queue=self.queue, passive=True)  # Check if the queue exists
            connection.close()
            return True  # If successful, the service is healthy
        except Exception as e:
            print(f"RabbitMQ health check failed: {e}")
            return False