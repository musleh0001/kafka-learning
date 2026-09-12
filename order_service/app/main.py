from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI

from .models import create_order_with_event, create_tables
from .schemas import CreateOrderRequest


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    yield


app = FastAPI(
    title="Kafka Order Service",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders")
def create_new_order(
    request: CreateOrderRequest,
):
    order_id = str(uuid4())

    event = {
        "event_id": str(uuid4()),
        "event_type": "order.created",
        "event_version": 1,
        "correlation_id": order_id,
        "data": {
            "order_id": order_id,
            "customer_id": request.customer_id,
            "product_id": request.product_id,
            "quantity": request.quantity,
            "amount": request.amount,
        },
    }

    create_order_with_event(
        order_id=order_id,
        customer_id=request.customer_id,
        product_id=request.product_id,
        quantity=request.quantity,
        amount=request.amount,
        event=event,
    )

    return {
        "order_id": order_id,
        "status": "CREATED",
    }
