from pydantic import BaseModel


class CreateOrderRequest(BaseModel):
    customer_id: str
    product_id: str
    quantity: int
    amount: float
