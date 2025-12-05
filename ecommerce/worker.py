import threading
import traceback
from queue import Queue, Empty
from time import sleep

STOP_SIGNAL = object()


class EventWorker(threading.Thread):
    """
    Фоновий потік-споживач, який безперервно бере завдання з черги (`Queue`)
    та викликає відповідні колбеки.

    Завданням є кортеж: (event_name: str, data: Any, callbacks: List[Callable]).

    Потік демон: Завершиться автоматично, якщо основна програма виходить.
    """

    def __init__(self, queue: Queue):
        """
        Ініціалізація Worker'а.

        :param queue: Черга, з якої Worker отримуватиме завдання.
        :type queue: Queue
        """
        super().__init__()
        self.queue = queue
        self.daemon = True
        print("WORKER: Ініціалізовано. Готовий обробляти завдання.")

    def run(self):
        """
        Основний цикл роботи Worker'а.
        """
        while True:
            try:
                task = self.queue.get(timeout=1)
            except Empty:
                continue
            except Exception as ex:
                print(f"WORKER FATAL ERROR (Queue Get): {ex.__class__.__name__}: {ex}")
                sleep(2)
                continue

            if task is STOP_SIGNAL:
                self.queue.task_done()
                break

            event_name, data, callbacks = task
            print(f"\nWORKER: Отримано завдання для '{event_name}'. Викликаємо {len(callbacks)} слухачів...")

            try:
                for callback in callbacks:
                    try:
                        callback(event_name, data)

                    except Exception as ex:
                        print(f"WORKER ERROR: Слухач '{callback.__name__}' для '{event_name}' впав. {ex}")
                        traceback.print_exc(limit=1)

                self.queue.task_done()

            except Exception as ex:
                print(f"WORKER ERROR (Task Processing) для '{event_name}': {ex.__class__.__name__}: {ex}")
                sleep(2)
        print("WORKER: Отримано STOP_SIGNAL. Завершення потоку.")


def start_worker(queue: Queue) -> EventWorker:
    """
    Створює та запускає потік EventWorker.

    :param queue: Черга, яку Worker повинен моніторити.
    :type queue: Queue
    :return: Об'єкт запущеного EventWorker.
    :rtype: EventWorker
    """
    worker = EventWorker(queue)
    worker.start()
    return worker


def stop_worker(queue: Queue, worker: EventWorker):
    """
    Надсилає сигнал зупинки Worker'у та очікує його коректного завершення.

    1. Надсилає `STOP_SIGNAL` у чергу.
    2. Викликає `worker.join()`, щоб дочекатися завершення потоку.
    3. Викликає `queue.join()`, щоб переконатися, що всі завдання в черзі (включаючи STOP_SIGNAL)
       були позначені як виконані (`task_done()`).

    :param queue: Черга, до якої потрібно додати сигнал зупинки.
    :type queue: Queue
    :param worker: Об'єкт Worker, який потрібно зупинити.
    :type worker: EventWorker
    """
    print("\n--- Зупинка Worker ---")
    queue.put(STOP_SIGNAL)
    worker.join()
    queue.join()
    print("Worker завершив роботу")
