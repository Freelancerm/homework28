from typing import Any


def email_sender(event_name: str, data: Any):
    """
    Обробник, який імітує відправку email-повідомлення.

    Активується лише при події **'user.registered'**.

    :param event_name: Назва події, що була емітована.
    :type event_name: str
    :param data: Корисне навантаження події (очікує 'user_id').
    :type data: Any
    """
    if event_name == "user.registered":
        print(f"EMAIL_SENDER: Відправлено вітальний лист користувачу {data.get('user_id')}.")


def logger(event_name: str, data: Any):
    """
    Обробник, який імітує запис подій у централізований лог-сервіс.

    Ця функція призначена для підписки на **вайлдкард** (наприклад, 'user.*' або 'order.*')
    та друкує повідомлення, яке відображає тип події, що спрацювала.

    :param event_name: Назва події, що була емітована.
    :type event_name: str
    :param data: Корисне навантаження події.
    :type data: Any
    """
    if 'user' in event_name:
        print(f"LOGGER (Wildcard): Зафіксована подія користувача '{event_name}' за даними: {data}")
    elif 'order' in event_name:
        print(f"LOGGER (Wildcard): Зафіксована подія замовлення '{event_name}' за даними: {data}")
    else:
        print(f"LOGGER (Wildcard): невідома подія '{event_name}'")


def analytics(event_name: str, data: Any):
    """
    Обробник, який імітує відправку метрики до системи аналітики.

    Активується лише при подіях **'user.registered'** або **'order.created'**.

    :param event_name: Назва події, що була емітована.
    :type event_name: str
    :param data: Корисне навантаження події.
    :type data: Any
    """
    if event_name in ["user.registered", "order.created"]:
        print(f"ANALYTICS: Відправлено метрику '{event_name}' до системи аналітики.")
