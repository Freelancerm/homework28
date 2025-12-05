import pika
import time
import json

CONNECTION_PARAMS = pika.ConnectionParameters('localhost', 5672, '/', pika.PlainCredentials('user', 'password'))
EXCHANGE_NAME = 'user_events_fanout'


def produce_event(event_name, data):
    """
    Встановлює з'єднання з RabbitMQ, декларує обмінник та відправляє одне повідомлення.

    Кожне повідомлення містить назву події, дані та часову мітку,
    серіалізовані у формат JSON.

    :param event_name: Назва події (наприклад, 'user.registered').
    :type event_name: str
    :param data: Корисне навантаження події (словник).
    :type data: dict
    """
    connection = pika.BlockingConnection(CONNECTION_PARAMS)
    channel = connection.channel()

    # Тип 'fanout' означає: надіслати повідомлення всім чергам, прив'язаним до цього Exchange.
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout')

    message = json.dumps({'event': event_name, 'data': data, 'timestamp': time.time()})
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key='',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE  # Робить повідомлення стійким
        )
    )
    """
    Публікує повідомлення в обмінник:
    - exchange=EXCHANGE_NAME: Відправляє до обмінника fanout.
    - routing_key='': Для fanout ключ маршрутизації ігнорується.
    - delivery_mode=PERSISTENT: Робить повідомлення стійким. RabbitMQ запише його на диск,
    якщо сервер впаде до того, як повідомлення буде доставлено.
    """

    print(f"[x] Producer: Надіслано {event_name}")
    connection.close()


if __name__ == '__main__':
    print("Producer запущено. Відправка подій...")
    produce_event(
        event_name="user.registered",
        data={"user_id": 101, "email": "testuser@example.com", "username": "JaneSmith"}
    )
    produce_event(
        event_name="user.registered",
        data={"user_id": 102, "email": "jane@example.com", "username": "JaneSmith"}
    )
    print("Готово")
