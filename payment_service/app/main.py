import sys
from pathlib import Path

if __name__ == "__main__" and __package__ is None:
    root = Path(__file__).resolve().parent.parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from payment_service.app.consumer import run

    run()
else:
    from .consumer import run

    if __name__ == "__main__":
        run()
