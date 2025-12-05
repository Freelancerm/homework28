from log_storage import FileProducer, FileConsumer
import os


# Очищення старих логів та офсетів для чистої демонстрації
def cleanup():
    for d in ["kafka_logs", "kafka_offsets"]:
        if os.path.exists(d):
            for f in os.listdir(d):
                os.remove(os.path.join(d, f))
            os.rmdir(d)
    os.makedirs("kafka_logs", exist_ok=True)
    os.makedirs("kafka_offsets", exist_ok=True)


cleanup()

producer = FileProducer()

# 1. Consumer А (Email Service): Читає 'orders' та 'payments'
consumer_a = FileConsumer(group_id="email_service")
consumer_a.subscribe("orders")
consumer_a.subscribe("payments")

# 2. Consumer Б (Analytics Service): Читає лише 'orders'
consumer_b = FileConsumer(group_id="analytics_service")
consumer_b.subscribe("orders")

print("\n--- ЕТАП 1: Producer відправляє 3 події ---")

producer.send("orders", {"type": "ORDER_CREATED", "id": 101, "amount": 50})
producer.send("payments", {"type": "PAYMENT_REQUEST", "order_id": 101})
producer.send("orders", {"type": "ORDER_UPDATED", "id": 102, "amount": 100})

print("\n--- ЕТАП 2: Consumer A (Email) опитує ---")
events_a = consumer_a.poll()
print(f"Оброблено Consumer A: {len(events_a)} подій")
# Очікується 3 (2 з orders, 1 з payments)


print("\n--- ЕТАП 3: Consumer B (Analytics) опитує ---")
events_b = consumer_b.poll()
print(f"Оброблено Consumer B: {len(events_b)} подій")
# Очікується 2 (лише з orders)


print("\n--- ЕТАП 4: Producer відправляє ще 1 подію ---")
producer.send("orders", {"type": "ORDER_SHIPPED", "id": 102})

print("\n--- ЕТАП 5: Consumer A опитує знову ---")
# Consumer A повинен прочитати ТІЛЬКИ нову подію (завдяки збереженому офсету)
events_a_2 = consumer_a.poll()
print(f"Оброблено Consumer A (2): {len(events_a_2)} подій")
# Очікується 1


print("\n--- ЕТАП 6: Перезапуск Consumer B ---")
# Імітуємо перезапуск: створюємо новий екземпляр, офсети будуть завантажені
consumer_b_restarted = FileConsumer(group_id="analytics_service")
consumer_b_restarted.subscribe("orders")

events_b_restarted = consumer_b_restarted.poll()
print(f"Оброблено Consumer B (Restart): {len(events_b_restarted)} подій")
# Очікується 1 (тільки нова подія ORDER_SHIPPED)

print("\n--- Завершення демонстрації Log-Based Storage ---")
