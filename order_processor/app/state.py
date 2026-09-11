orders = {}


def mark_payment_completed(order_id: str):
    order = orders.setdefault(
        order_id, {"payment": False, "inventory": False, "completed": False}
    )
    order["payment"] = True


def mark_inventory_reserved(order_id: str):
    order = orders.setdefault(
        order_id, {"payment": False, "inventory": False, "completed": False}
    )
    order["inventory"] = True


def is_order_completed(order_id: str) -> bool:
    order = orders.get(order_id)
    if not order:
        return False

    if order["payment"] and order["inventory"] and not order.get("completed"):
        order["completed"] = True
        return True

    return False
