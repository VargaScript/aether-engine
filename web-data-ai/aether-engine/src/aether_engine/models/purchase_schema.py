from pydantic import BaseModel


class PurchaseSchema(BaseModel):
    total_amount: float
    payment_method: str
