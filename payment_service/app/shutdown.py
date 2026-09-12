import signal

running = True


def shutdown_handler(signum, frame):
    global running
    print("Shutdown signal received")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)
