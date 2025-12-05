from typing import Any


def email_sender(event_name: str, data: Any):
    """
    Імітує відправку email-повідомлення при створенні замовлення.

    Ця функція призначена для підписки на 'order.created'.

    Реалізує імітацію збою: якщо 'user_id' дорівнює 501,
    функція навмисно кидає виняток `ValueError`.
    Це демонструє, як Worker має обробляти помилки.

    :param event_name: Назва події (очікується 'order.created').
    :type event_name: str
    :param data: Корисне навантаження події, що містить 'user_id' та 'order_id'.
    :type data: Any
    :raises ValueError: Симульована помилка відправки для user_id=501.
    """
    user_id = data.get("user_id", "N/A")
    order_id = data.get("order_id", "N/A")

    if user_id == 501:
        print(f"EMAIL ERROR: Не вдалося відправити лист користувачу {user_id}. Кидаємо виняток.")
        raise ValueError("Simulated Email Sending Failure")
    print(f"NOTIFICATION (Email): Замовлення #{order_id} створено. Відправлено лист користувачу {user_id}.")


def sms_sender(event_name: str, data: Any):
    """
    Імітує відправку SMS-повідомлення при успішній оплаті замовлення.

    Ця функція призначена для підписки на 'order.paid'.

    :param event_name: Назва події (очікується 'order.paid').
    :type event_name: str
    :param data: Корисне навантаження події, що містить 'order_id'.
    :type data: Any
    """
    order_id = data.get("order_id", "N/A")
    print(f"NOTIFICATION (SMS): Замовлення #{order_id} успішно ОПЛАЧЕНО.")
