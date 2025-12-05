import pika
import json
import time
from producer import EXCHANGE_NAME, CONNECTION_PARAMS


def callback(ch, method, properties, body):
    """
    Функція зворотного виклику (callback), яка викликається Pika при отриманні нового повідомлення.

    Обробляє повідомлення 'user.registered', симулюючи роботу з аналітикою.
    Виконує **ручне підтвердження (basic_ack)** після успішної обробки.

    :param ch: Об'єкт каналу (Channel).
    :type ch: pika.channel.Channel
    :param method: Об'єкт, що містить інформацію про доставку (delivery_tag, exchange, routing_key).
    :type method: pika.spec.Basic.Deliver
    :param properties: Об'єкт властивостей повідомлення (наприклад, content_type).
    :type properties: pika.spec.BasicProperties
    :param body: Тіло повідомлення (байти JSON).
    :type body: bytes
    """
    data = json.loads(body)
    event = data["event"]

    if event == "user.registered":
        print(f" [ANALYTICS_WORKER] Отримано подію {event}.")
        time.sleep(0.5)
        print(f"[ANALYTICS_WORKER] Зареєстровано нового користувача {data['data']['username']}. Оновлення статистики.")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_worker():
    """
    Встановлює з'єднання з RabbitMQ та запускає Worker-споживача.

    Етапи:
    1. Створює **блокуюче з'єднання** та канал.
    2. Декларує обмінник `fanout` (що дозволяє широкомовну розсилку всім підписаним чергам).
    3. Декларує **анонімну, ексклюзивну чергу** (`queue=''`, `exclusive=True`). Ця черга:
       - Має випадкове ім'я.
       - Видаляється, коли з'єднання закривається.
    4. Прив'язує чергу до обмінника (`queue_bind`), щоб отримувати всі повідомлення.
    5. Встановлює **prefetch_count=1** (`basic_qos`) — це гарантує, що Worker отримуватиме лише одне нове повідомлення
       після того, як підтвердить попереднє (для рівномірного розподілу навантаження).
    6. Запускає цикл споживання (`start_consuming`).

    :raises KeyboardInterrupt: При натисканні Ctrl+C.
    :raises Exception: У разі помилки підключення або каналу.
    """
    connection = pika.BlockingConnection(CONNECTION_PARAMS)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)
    print(' [ANALYTICS_WORKER] Очікування подій. Натисніть CTRL+C для виходу.')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    channel.start_consuming()


if __name__ == '__main__':
    try:
        start_worker()
    except KeyboardInterrupt:
        print("ANALYTICS WORKER зупинено.")
    except Exception as error:
        print(f"Помилка в ANALYTICS WORKER: {error}")
