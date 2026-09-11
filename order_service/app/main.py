from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI

from common.events import create_event

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
    order_id = request.order_id or str(uuid4())

    create_order(
        order_id=order_id,
        customer_id=request.customer_id,
        product_id=request.product_id,
        quantity=request.quantity,
        amount=request.amount,
    )

    data = {
        "order_id": order_id,
        "customer_id": request.customer_id,
        "product_id": request.product_id,
        "quantity": request.quantity,
        "amount": request.amount,
    }
    event = create_event(event_type="order.created", correlation_id=order_id, data=data)

    print("Order created")
    publish_order(event)
    print("Published order.created")

    return {
        "order_id": order_id,
        "status": "CREATED",
    }
