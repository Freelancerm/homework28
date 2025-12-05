from queue import Queue
from time import sleep

from core.event_bus import EventBus
from .notification_service import email_sender, sms_sender
from .analytics_service import analytics_counter, get_analytics_total, analytics_replay_listener, reset_analytics_total
from .order_service import create_order, pay_order
from .worker import start_worker, stop_worker
import os

if __name__ == "__main__":
    if os.path.exists("events.log"):
        os.remove("events.log")

    print("==============================================")
    print("   ФАЗА 1: ЗВИЧАЙНА РОБОТА ТА ЗАПИС ЛОГУ")
    print("==============================================")

    event_queue = Queue()
    bus = EventBus(event_queue)
    worker_thread = start_worker(event_queue)

    # Підписки
    bus.subscribe("order.created", email_sender)
    bus.subscribe("order.created", analytics_counter)
    bus.subscribe("order.paid", sms_sender)
    bus.subscribe("order.paid", analytics_counter)

    print("\n--- ГЕНЕРУВАННЯ ПОДІЙ ---")

    create_order(bus, user_id=100, order_id=1, amount=50.0)  # order.created, order.paid
    pay_order(bus, order_id=1)
    create_order(bus, user_id=501, order_id=2, amount=100.0)  # email_sender впаде

    print("\n... Очікування 3 секунди на обробку Фази 1 ...")
    sleep(3)

    # Зупинка Worker'а після Фази 1
    # Ми не зупиняємо увесь worker_thread, а лише сигналізуємо про необхідність
    # обробки черги, і потім зупиняємо.
    stop_worker(event_queue, worker_thread)
    print("--- Worker завершив роботу Фази 1. ---")

    # ----------------------------------------------------

    # Для демонстрації Replay ми ініціалізуємо Worker знову (як ніби відбувся перезапуск)
    # та скидаємо лічильники для чистоти демонстрації (хоча в реальному Event Sourcing вони б не скидались)

    print("\n==============================================")
    print("   ФАЗА 2: EVENT REPLAY")
    print("  (Лічильники ANALYTICS скинуті для демонстрації)")
    print("==============================================")

    # Скидаємо лічильники для чистої демонстрації, що Replay обробляє їх з нуля
    reset_analytics_total()
    bus.clear_subscriptions()

    # Запускаємо новий Worker для обробки Replay-завдань
    event_queue = Queue()
    bus.queue = event_queue  # Прив'язуємо EventBus до нової черги
    worker_thread = start_worker(event_queue)

    bus.subscribe("order.created", analytics_replay_listener)
    bus.subscribe("order.paid", analytics_replay_listener)

    # --- ВИКЛИК REPLAY ---
    bus.replay_from_file("events.log")

    print("\n... Очікування 3 секунди на обробку Replay ...")
    sleep(3)

    # --- ФІНАЛЬНИЙ СТАН ---
    analytics_totals = get_analytics_total()
    print("\n==============================================")
    print("  ФІНАЛЬНІ ДАНІ ПІСЛЯ REPLAY:")
    print(f"  Всього створено замовлень: {analytics_totals['orders']} (Очікується 2)")
    print(f"  Всього оплат: {analytics_totals['paid']} (Очікується 1)")
    print(f"  Всього перепрограно подій: {analytics_totals['replay_events']} (Очікується 3)")
    print("==============================================")

    stop_worker(event_queue, worker_thread)
