import json
import logging
import time

import redis
from confluent_kafka import Producer
from faker import Faker
from pydantic import BaseModel

from aether_engine.models import PurchaseSchema, UserSchema

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

fake = Faker()
r = redis.Redis(host="localhost", port=6379, decode_responses=True)


class AetherProducer:
    def __init__(self, bootstrap_servers="localhost:9092"):
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "aether_ingestor",
            "acks": "all",
        }
        self.producer = Producer(self.config)

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Error delivering the message: {err}")
        else:
            logger.info(
                f"Kafka message: Topic '{msg.topic()}' | Partition [{msg.partition()}]"
            )

    def send_data(self, topic, data: BaseModel):
        try:
            payload = json.dumps(data.model_dump()).encode("utf-8")

            self.producer.produce(topic, value=payload, callback=self.delivery_report)
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error sent to Kafka: {e}")


def run_ingestor():
    producer = AetherProducer()
    logger.info("Initiating Aether Engine's Producer (Sent to Kafka)...")

    try:
        while True:
            user_data = UserSchema(
                name=fake.name(), email=fake.email(), city=fake.city()
            )

            purchase_data = PurchaseSchema(
                total_amount=float(
                    fake.pydecimal(left_digits=3, right_digits=2, positive=True)
                ),
                payment_method=fake.credit_card_provider(),
            )

            producer.send_data("user_registration", user_data)
            producer.send_data("user_purchases", purchase_data)

            r.incr("total_produced")
            r.set("last_produced_user", user_data.name)

            logger.info(f"Events sent. Total accumulated: {r.get('total_produced')}")

            producer.producer.flush()

            time.sleep(3)

    except KeyboardInterrupt:
        logger.info("Finishing Ingestion...")
    finally:
        producer.producer.flush()


if __name__ == "__main__":
    run_ingestor()
