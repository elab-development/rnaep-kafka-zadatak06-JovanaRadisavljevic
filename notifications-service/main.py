from fastapi import FastAPI
from typing import List
from models import Notification
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
import asyncio, json

@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer = AIOKafkaConsumer(
        "order-confirmed", 
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )
    consumer_not_found = AIOKafkaConsumer(
        "product_not_found_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )
    consumer_out_of_stock = AIOKafkaConsumer(
        "out_of_stock_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )
    await consumer.start()
    await consumer_not_found.start()
    await consumer_out_of_stock.start()
    task1 = asyncio.create_task(consume(consumer))
    task2 = asyncio.create_task(consume_error(consumer_not_found))
    task3 = asyncio.create_task(consume_error(consumer_out_of_stock))
    yield
    for task in [task1, task2, task3]:
        task.cancel()
    await consumer.stop()
    await consumer_not_found.stop()
    await consumer_out_of_stock.stop()

app = FastAPI(title="Notifications Service", lifespan=lifespan)

notifications_db: List[Notification] = []

async def consume(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            notification = Notification(order_id=data['order_id'], product_id=data['product_id'], message=f"Order {data['order_id']} for product {data['product_id']} has been placed.")
            notifications_db.append(notification)
    except asyncio.CancelledError:
        pass

async def consume_error(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            notification = Notification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                message=(
                    f"Narudzbina {data['order_id']} je odbijena. "
                    f"Razlog: {data['error_reason']}. "
                    f"Vreme: {data['timestamp']}"
                )
            )
            notifications_db.append(notification)
    except asyncio.CancelledError:
        pass

@app.get("/notifications", response_model=List[Notification])
def get_notifications():
    return notifications_db