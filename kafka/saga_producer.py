from log_storage import FileProducer
from typing import Dict, Any, List

producer = FileProducer()
SAGA_ID_SUCCESS = "order_123_success"
SAGA_ID_FAILURE = "order_456_failure"


def log_saga_event(saga_id: str, event_type: str, details: Dict[str, Any]):
    """
    Генерує подію в топік 'saga_log'.

    Використовує FileProducer для імітації відправлення повідомлення
    до розподіленого брокера (наприклад, Kafka).
    Цей лог являє собою "журнал" Saga, який можна використовувати для
    відновлення або аудиту.

    :param saga_id: Унікальний ідентифікатор Saga (ідентифікатор замовлення).
    :type saga_id: str
    :param event_type: Тип події (наприклад, 'PAYMENT_RESERVED', 'INVENTORY_RESTORED').
    :type event_type: str
    :param details: Додаткові дані про подію.
    :type details: Dict[str, Any]
    """
    producer.send("saga_log", {"saga_id": saga_id, "type": event_type, "details": details})
    print(f"  [LOG]: Подія '{event_type}' для Saga {saga_id} записана.")


def process_order_saga(saga_id: str, order_data: Dict[str, Any], simulate_failure_step: str = None):
    """
    Виконує повний процес Saga для обробки замовлення.

    Кожен крок Saga є **локальною транзакцією**, яка емітує подію про свій успіх.
    У разі збою на будь-якому кроці, запускається послідовність
    компенсаційних транзакцій у зворотному порядку, використовуючи збережений
    список виконаних кроків.

    :param saga_id: ID замовлення/Saga.
    :type saga_id: str
    :param order_data: Дані, необхідні для всіх кроків Saga.
    :type order_data: Dict[str, Any]
    :param simulate_failure_step: Назва локальної транзакції, на якій потрібно
                                  симулювати збій (наприклад, 'payment_service.reserve').
    :type simulate_failure_step: str, optional
    """
    print(f"\n--- SAGA: Початок обробки замовлення {saga_id} ---")

    # Кроки Saga: (Назва Події, Локальна Транзакція, Компенсуюча Подія)
    steps = [
        ("ORDER_CREATED", "order_service.create", "ORDER_CANCELLED"),
        ("PAYMENT_RESERVED", "payment_service.reserve", "PAYMENT_CANCELLED"),
        ("INVENTORY_DEDUCTED", "inventory_service.deduct", "INVENTORY_RESTORED"),
        ("DELIVERY_REQUESTED", "delivery_service.request", "DELIVERY_CANCELLED"),
    ]

    executed_steps = []
    success = True

    for event_type, local_transaction, compensating_event in steps:

        if success:
            # 1. Виконуємо пряму транзакцію

            # --- СИМУЛЯЦІЯ ПОМИЛКИ ---
            if local_transaction == simulate_failure_step:
                print(f"!!! SAGA FAILED: Симульована помилка на кроці: {local_transaction}")
                success = False
                log_saga_event(saga_id, "SAGA_FAILED", {"step": local_transaction})
                # Переходимо до компенсації
                break

                # --- Успіх ---
            print(f"  [+] SAGA STEP: Успіх: {local_transaction}")
            log_saga_event(saga_id, event_type, order_data)
            executed_steps.append((local_transaction, compensating_event))  # Зберігаємо для компенсації

    if not success:
        print("\n--- SAGA COMPENSATION: Запуск компенсаційних транзакцій ---")

        # 2. Виконуємо компенсаційні транзакції у зворотному порядку
        executed_steps.reverse()
        for local_transaction, compensating_event in executed_steps:
            print(f"  [<] SAGA COMPENSATING: Відміна: {local_transaction} -> {compensating_event}")
            log_saga_event(saga_id, compensating_event, order_data)

    else:
        log_saga_event(saga_id, "SAGA_COMPLETED", {"order_id": saga_id})
        print(f"\n✅ SAGA: Процес {saga_id} успішно завершено.")


if __name__ == '__main__':
    # 1. Успішна Saga (повний цикл)
    order_details_1 = {"user_id": 42, "amount": 100.0}
    process_order_saga(SAGA_ID_SUCCESS, order_details_1)

    # 2. Невдала Saga (збій на резерві коштів)
    order_details_2 = {"user_id": 43, "amount": 250.0}
    # Симулюємо збій на кроці "payment_service.reserve"
    process_order_saga(SAGA_ID_FAILURE, order_details_2, simulate_failure_step="inventory_service.deduct")

    print("\n\n--- Перевірка логу Saga ---")
    print("Виконайте: 'cat kafka_logs/saga_log.log' для перегляду повної послідовності.")
