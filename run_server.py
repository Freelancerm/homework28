from typing import Optional

import uvicorn
from queue import Queue

from app import app, set_event_bus
from core.event_bus import EventBus

from ecommerce.worker import start_worker, stop_worker, EventWorker
from ecommerce.notification_service import email_sender, sms_sender
from ecommerce.analytics_service import analytics_counter

event_queue = Queue()
bus = EventBus(event_queue)
set_event_bus(bus)

bus.subscribe("order.created", email_sender)
bus.subscribe("order.created", analytics_counter)
bus.subscribe("order.paid", sms_sender)
bus.subscribe("order.paid", analytics_counter)


def run_worker():
    """
    Запускає фоновий потік EventWorker, який буде безперервно
    обробляти завдання з `event_queue`.

    Зберігає об'єкт потоку у глобальній змінній `worker_thread` для коректної зупинки.
    """
    global worker_thread
    print("SYSTEM: Запуск Worker'a...")
    worker_thread = start_worker(event_queue)


def run_server():
    """
    Запускає основний веб-сервер FastAPI за допомогою Uvicorn.
    Це блокуюча операція, яка триватиме, поки не буде перервана (наприклад, Ctrl+C).
    """
    print("SYSTEM: Запуск FastAPI-сервера на http://127.0.0.1:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    worker_thread: Optional[EventWorker] = None
    run_worker()

    try:
        run_server()
    except KeyboardInterrupt:
        print("\nSYSTEM: Отримано сигнал зупинки сервера. Завершення Worker'а...")
        if worker_thread:
            stop_worker(event_queue, worker_thread)
        print("SYSTEM: Програма завершена.")
