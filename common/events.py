from datetime import datetime, timezone
from uuid import uuid4


def create_event(event_type: str, correlation_id: str, data: dict) -> dict:
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": correlation_id,
        "data": data,
    }


def delivery_report(err, msg):
    if err:
        print(f">>> [ERROR] Message delivery failed: {err}")
