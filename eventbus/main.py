from core.event_bus import EventBus
from .listeners import *
from queue import Queue

event_queue = Queue()
bus = EventBus(event_queue)

bus.subscribe("user.registered", email_sender)
bus.subscribe("order.created", analytics)

bus.subscribe("user.*", logger)
bus.subscribe("user.*", analytics)

bus.emit("user.registered", {"user_id": 101, "username": "Alice"})
bus.emit("user.deleted", {"user_id": 101, "reason": "Spam"})
bus.emit("order.created", {"order_id": 500, "amount": 150.00})

print("\n Історія всіх подій (history)")
for entry in bus.history:
    print(f"[{entry['timestamp']}] {entry['event']}: {entry['data']}")

print("\n--- Демонстрація відписки ---")
bus.unsubscribe("user.*", analytics)
bus.emit("user.registered", {"user_id": 102, "username": "Bob"})

print("\n--- Фінальний стан підписок ---")
# 1. Визначимо, що є точними підписками, а що wildcard
exact_subscriptions = {k: v for k, v in bus.subscribers.items() if '*' not in k}
wildcard_subscriptions = {k: v for k, v in bus.subscribers.items() if '*' in k}

print("\nТочні Підписки (Exact Matches)")
print("(Спрацьовують лише на повну відповідність імені події)")
for event_name, callbacks in exact_subscriptions.items():
    callback_names = [cb.__name__ for cb in callbacks]
    print(f"'{event_name}': Спрацює: {callback_names}")

print("\nWildcard Підписки (Шаблони)")
print("(Спрацьовують на всі події, що починаються з цього префікса)")
for event_name, callbacks in wildcard_subscriptions.items():
    callback_names = [cb.__name__ for cb in callbacks]
    print(f"'{event_name}': Спрацює: {callback_names}")

# Додатковий вивід для підтвердження, якщо словник порожній
if not bus.subscribers:
    print("Словник підписок порожній")
