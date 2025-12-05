def create_order(bus, user_id, order_id, amount):
    """
    Імітує створення нового замовлення та емітує відповідну подію.

    Ця функція є виробником події 'order.created'.

    :param bus: Об'єкт шини подій (EventBus).
    :type bus: EventBus
    :param user_id: ID користувача, який створив замовлення.
    :type user_id: int
    :param order_id: Унікальний ID замовлення.
    :type order_id: int
    :param amount: Сума замовлення.
    :type amount: float
    """
    data = {"user_id": user_id, "order_id": order_id, "amount": amount}
    bus.emit("order.created", data)


def pay_order(bus, order_id):
    """
    Імітує оплату замовлення та емітує відповідну подію.

    Ця функція є виробником події 'order.paid'.

    :param bus: Об'єкт шини подій (EventBus).
    :type bus: EventBus
    :param order_id: Унікальний ID замовлення, яке було оплачено.
    :type order_id: int
    """
    data = {"order_id": order_id}
    bus.emit("order.paid", data)
