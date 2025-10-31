# producer.py
import os
import uuid
import json
from dotenv import load_dotenv
from datetime import datetime
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage

load_dotenv()

SERVICE_BUS_CONN = os.getenv("SERVICE_BUS_CONNECTION_STR")
TOPIC_NAME = os.getenv("TOPIC_NAME", "orders")

app = FastAPI(title="Order Producer")

class Item(BaseModel):
    sku: str
    qty: int
    price: float

class Customer(BaseModel):
    customerId: str
    email: str

class Order(BaseModel):
    orderId: str = None
    createdAt: str = None
    customer: Customer
    items: List[Item]
    total: float
    payment: dict = {}
    metadata: dict = {}

@app.on_event("startup")
async def startup_event():
    if not SERVICE_BUS_CONN:
        raise RuntimeError("SERVICE_BUS_CONNECTION_STR env var not set")
    app.state.sb_client = ServiceBusClient.from_connection_string(SERVICE_BUS_CONN, logging_enable=False)

@app.on_event("shutdown")
async def shutdown_event():
    await app.state.sb_client.close()

@app.post("/orders")
async def create_order(order: Order):
    # fill defaults
    if not order.orderId:
        order.orderId = f"ORD-{uuid.uuid4().hex[:8]}"
    if not order.createdAt:
        order.createdAt = datetime.utcnow().isoformat() + "Z"

    msg_body = order.dict()
    msg = ServiceBusMessage(
        json.dumps(msg_body),
        application_properties={"messageType": "OrderCreated"},
        message_id=order.orderId,
        content_type="application/json"
    )

    async with app.state.sb_client.get_topic_sender(topic_name=TOPIC_NAME) as sender:
        await sender.send_messages(msg)

    return {"status": "sent", "orderId": order.orderId}
