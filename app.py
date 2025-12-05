from fastapi import FastAPI
from pydantic import BaseModel
from typing import Literal, Optional

from core.event_bus import EventBus


class OrderWebhook(BaseModel):
    """
    Pydantic модель, що визначає очікувану структуру вхідного JSON
    для вебхука замовлень.
    """
    order_id: int
    status: Literal["created", "paid", "shipped"]


app = FastAPI(title="Event-Driven Webhook Handler")

event_bus: Optional[EventBus] = None


def set_event_bus(bus_instance: EventBus):
    """
    Встановлює глобальний інстанс EventBus для використання обробниками вебхуків.
    Ця функція викликається під час ініціалізації сервера.

    :param bus_instance: Інстанс EventBus.
    :type bus_instance: EventBus
    """
    global event_bus
    event_bus = bus_instance
    print("SERVER: EventBus успішно підключено")


@app.post("/webhook/order")
def handle_order_book(order: OrderWebhook):
    """
    Обробляє вхідні HTTP POST запити на вебхук замовлень.

    Перетворює вхідні дані OrderWebhook на подію EventBus та емітує її:
    - Ім'я події: 'order.{status}' (наприклад, 'order.created').
    - Дані: Корисне навантаження з ID замовлення.

    Якщо EventBus не ініціалізовано, повертає помилку 500.

    :param order: Валідовані дані вебхука.
    :type order: OrderWebhook
    :return: Словник зі статусом обробки.
    :rtype: Dict[str, Any]
    """
    if not event_bus:
        return {"status": "error", "message": "EventBus не ініціалізовано"}, 500

    event_name = f"order.{order.status}"
    data = {"order_id": order.order_id, "amount": 0}

    event_bus.emit(event_name, data)

    print(f"SERVER: Отримано Webhook. Подія '{event_name}' додана до черги.")

    return {"status": "success", "event_name": event_name, "message": "Подія прийнята в обробку."}
