import json
import logging
from datetime import datetime

from confluent_kafka import Consumer
from pydantic import ValidationError

from aether_engine.database import SessionLocal
from aether_engine.models import (
    ErrorsTable,
    PurchaseSchema,
    PurchaseTable,
    UserSchema,
    UserTable,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AetherProcessor:
    def __init__(self):
        self.config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "aether-processor-group",
            "auto.offset.reset": "earliest",
        }
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(["user_registration", "user_purchases"])
        self.last_user_id = None

    def run(self):
        logger.info("Processor Initiated (Users + Purchases)...")
        db = SessionLocal()

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                topic = msg.topic()
                data = json.loads(msg.value().decode("utf-8"))

                if topic == "user_registration":
                    try:
                        valid_user = UserSchema(**data)
                    except ValidationError as e:
                        logger.error(f"Error: {e}")
                        error_data = ErrorsTable(
                            topic_name=topic,
                            raw_data=data,
                            error_detail=str(e),
                            created_at=datetime.utcnow,
                        )

                        db.add(error_data)
                        db.commit()

                        continue

                    new_user = UserTable(**valid_user.model_dump())
                    db.add(new_user)
                    db.commit()
                    db.refresh(new_user)
                    self.last_user_id = new_user.id
                    logger.info(f"USER: {valid_user.name} saved (ID: {new_user.id})")

                elif topic == "user_purchases":
                    if self.last_user_id is None:
                        logger.warning(
                            "Purchase received without previous user in this session. Ignoring..."  # Translate
                        )
                        continue

                    try:
                        valid_purchase = PurchaseSchema(**data)
                    except ValidationError as e:
                        error_data = ErrorsTable(
                            topic_name=topic,
                            raw_data=data,
                            error_detail=str(e),
                            created_at=datetime.utcnow,
                        )
                        db.add(error_data)
                        db.commit()
                        continue

                    new_purchase = PurchaseTable(
                        total_amount=valid_purchase.total_amount,
                        payment_method=valid_purchase.payment_method,
                        user_id=self.last_user_id,
                    )
                    db.add(new_purchase)
                    db.commit()
                    logger.info(
                        f"PURCHASE: ${valid_purchase.total_amount} associated to ID: {self.last_user_id}"
                    )

        except Exception as e:
            logger.error(f"❌ Error: {e}")
            db.rollback()
        finally:
            self.consumer.close()
            db.close()


if __name__ == "__main__":
    AetherProcessor().run()
