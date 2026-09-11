from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI

from .kafka import flush, publish_order
from .models import create_order, create_tables
from .schemas import CreateOrderRequest


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    yield
    flush()


app = FastAPI(title="Kafka Order Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders")
def create_new_order(request: CreateOrderRequest):
    order_id = str(uuid4())

    create_order(
        order_id=order_id,
        customer_id=request.customer_id,
        product_id=request.product_id,
        quantity=request.quantity,
        amount=request.amount,
    )

    event = {
        "event_type": "order.created",
        "event_id": str(uuid4()),
        "order_id": order_id,
        "customer_id": request.customer_id,
        "product_id": request.product_id,
        "quantity": request.quantity,
        "amount": request.amount,
    }

    publish_order(event)

    return {
        "order_id": order_id,
        "status": "CREATED",
    }
