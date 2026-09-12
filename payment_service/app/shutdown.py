import signal

_running = True


def _shutdown_handler(signum, frame):
    global _running
    print("\nShutdown signal received. Shutting down gracefully...")
    _running = False


def is_running() -> bool:
    return _running


def setup_graceful_shutdown() -> None:
    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)
