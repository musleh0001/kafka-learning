from pydantic import BaseModel


class CreateOrderRequest(BaseModel):
    order_id: str | None = None
    customer_id: str
    product_id: str
    quantity: int
    amount: float
