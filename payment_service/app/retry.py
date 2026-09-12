def get_retry_topic(retry_count: int) -> str:
    match retry_count:
        case 0:
            return "payment-retry-1"
        case 1:
            return "payment-retry-2"
        case 2:
            return "payment-retry-3"
        case _:
            return "payment-dlt"
