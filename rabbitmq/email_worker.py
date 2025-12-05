import pika
import json
import time
from producer import CONNECTION_PARAMS, EXCHANGE_NAME


def callback(ch, method, properties, body):
    """
    Функція зворотного виклику (callback), яка викликається Pika при отриманні нового повідомлення.

    Обробляє повідомлення 'user.registered', симулюючи відправку вітального листа.
    Виконує **ручне підтвердження (basic_ack)** після симуляції успішної обробки.

    :param ch: Об'єкт каналу (Channel).
    :type ch: pika.channel.Channel
    :param method: Об'єкт, що містить інформацію про доставку.
    :type method: pika.spec.Basic.Deliver
    :param properties: Об'єкт властивостей повідомлення.
    :type properties: pika.spec.BasicProperties
    :param body: Тіло повідомлення (байти JSON).
    :type body: bytes
    """
    data = json.loads(body)
    event = data["event"]

    if event == "user.registered":
        print(f" [EMAIL_WORKER] Отримано подію {event}.")
        time.sleep(2)
        print(f" [EMAIL_WORKER] Відправлено вітальний лист на {data['data']['email']}")

        ch.basic_ack(delivery_tag=method.delivery_tag)


def start_worker():
    """
    Встановлює з'єднання з RabbitMQ та запускає Worker-споживача.

    Етапи:
    1. Створює **блокуюче з'єднання** та канал.
    2. Декларує обмінник `fanout` (що дозволяє широкомовну розсилку).
    3. Декларує **анонімну, ексклюзивну чергу** (`queue=''`, `exclusive=True`). Ця черга:
       - Має випадкове ім'я.
       - Видаляється, коли з'єднання закривається.
    4. Прив'язує чергу до обмінника (`queue_bind`), щоб отримувати всі повідомлення, які потрапляють в обмінник.
    5. Встановлює **prefetch_count=1** (`basic_qos`) — гарантує, що Worker бере лише одне повідомлення за раз (для рівномірного розподілу навантаження між Workers).
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

    print(' [EMAIL_WORKER] Очікування подій. Натисніть CTRL+C для виходу.')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    channel.start_consuming()


if __name__ == '__main__':
    try:
        start_worker()
    except KeyboardInterrupt:
        print('EMAIL_WORKER зупинено.')
    except Exception as error:
        print(f"Помилка в EMAIL_WORKER: {error}")
