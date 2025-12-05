import collections
import time
import json
from typing import Callable, Any, Dict, List, Tuple
from queue import Queue


class EventBus:
    """
    Асинхронна шина подій (Event Bus), яка використовує чергу Queue для
    розв'язання зв'язків між виробниками (producers) та споживачами (consumers).

    Підтримує:
    1. Підписку/Відписку колбеків на події.
    2. Шаблони підписки за допомогою вайлдкардів (наприклад, 'user.*').
    3. Асинхронну емісію подій через передачу завдань у чергу.
    4. Ведення історії подій у пам'яті та запис у файл ('events.log').
    5. Повторне програвання подій з лог-файлу.
    """

    def __init__(self, queue: Queue):
        """
        Ініціалізує шину подій.

        :param queue: Об'єкт черги (наприклад, `queue.Queue`),
                      який використовується для передачі завдань обробникам.
        :type queue: Queue
        """
        self.queue = queue
        """Основна черга для передачі завдань обробникам"""
        self.subscribers: Dict[str, List[Callable]] = collections.defaultdict(list)
        """Словник для зберігання підписок: {event_name: [callback1, callback2, ...]}."""
        self.history: List[Dict[str, Any]] = []
        """Історія всіх емітованих подій (зберігається у пам'яті)."""

    def subscribe(self, event_name: str, callback: Callable):
        """
        Підписує колбек-функцію на конкретну назву події.

        Колбек повинен приймати два аргументи: `event_name` (str) та `data` (Any).

        :param event_name: Назва події (наприклад, 'user.created', 'order.paid' або 'user.*').
        :type event_name: str
        :param callback: Функція, яка буде викликана при емісії події.
        :type callback: Callable
        """
        if callback not in self.subscribers[event_name]:
            self.subscribers[event_name].append(callback)
            print(f"Підписка: '{callback.__name__}' на подію '{event_name}'")
        else:
            print(f"Підписка: '{callback.__name__}' вже існує для '{event_name}'")

    def unsubscribe(self, event_name: str, callback: Callable):
        """
        Відписує колбек-функцію від конкретної події.

        :param event_name: Назва події, від якої потрібно відписати.
        :type event_name: str
        :param callback: Функція, яку потрібно відписати.
        :type callback: Callable
        """
        try:
            self.subscribers[event_name].remove(callback)
            if not self.subscribers[event_name]:
                del self.subscribers[event_name]
            print(f"Відписка: '{callback.__name__}' від події '{event_name}' успішна")
        except (ValueError, KeyError):
            print(f"Помилка відписки: '{callback.__name__}' не був підписаний на '{event_name}'")

    def _get_matching_callbacks(self, event_name: str) -> List[Callable]:
        """
        Внутрішній метод для пошуку всіх колбеків, що відповідають події.

        Підтримує пряму відповідність та вайлдкарди ('*') для першої частини
        (наприклад, 'user.created' відповідає 'user.*').

        :param event_name: Назва події, що емітується.
        :type event_name: str
        :return: Список унікальних колбек-функцій, які мають бути викликані.
        :rtype: List[Callable]
        """
        matching_callbacks = []

        if event_name in self.subscribers:
            matching_callbacks.extend(self.subscribers[event_name])

        parts = event_name.split('.')
        if len(parts) > 1:
            wildcard_event = f"{parts[0]}.*"
            if wildcard_event in self.subscribers:
                for cb in self.subscribers[wildcard_event]:
                    if cb not in matching_callbacks:
                        matching_callbacks.append(cb)
        return matching_callbacks

    def emit(self, event_name: str, data: Any = None):
        """
        Емітує подію. Записує її в історію та лог-файл, а потім додає завдання
        для обробки у внутрішню чергу.

        Кожне завдання у черзі має вигляд: (event_name, data, matching_callbacks).

        :param event_name: Назва події, що емітується.
        :type event_name: str
        :param data: Корисне навантаження події (будь-який об'єкт, який можна серіалізувати).
        :type data: Any
        """
        print(f"\nЕмісія події: '{event_name}' з даними: {data}")

        self._log_event(event_name, data)

        matching_callbacks = self._get_matching_callbacks(event_name)

        if not matching_callbacks:
            print(f"PRODUCER: Немає слухачів для події '{event_name}'. Завдання не додано.")
            return

        task = (event_name, data, matching_callbacks)
        self.queue.put(task)
        print(f"PRODUCER: Завдання для {len(matching_callbacks)} слухачів додано до черги.")

    def _log_event(self, event_name: str, data: Any):
        """
        Внутрішній метод для логування події.

        Зберігає запис в `self.history` (пам'ять) та записує його у файл `events.log`
        у форматі JSON (Event Sourcing).

        :param event_name: Назва події.
        :type event_name: str
        :param data: Дані події.
        :type data: Any
        """
        log_entry = {
            "timestamp": time.strftime("%d-%m-%Y %H:%M:%S"),
            "event": event_name,
            "data": data,
        }
        self.history.append(log_entry)
        print(f"Лог (пам'ять): Подія '{event_name}' зафіксована в історії.")

        try:
            with open("events.log", "a", encoding="utf-8") as file:
                file.write(json.dumps(log_entry) + "\n")
            print(f"Лог (файл): Подія '{event_name}' записана у events.log")
        except Exception as ex:
            print(f"⚠️ Помилка запису логу подій у файл: {ex}")

    def replay_from_file(self, filename: str):
        """
        Повторно програє події, записані у лог-файлі, додаючи їх у чергу для обробки.

        Це дозволяє відновити стан системи або провести діагностику.
        Під час повторного програвання події не логуються повторно.

        :param filename: Шлях до лог-файлу (наприклад, 'events.log').
        :type filename: str
        """
        print(f"\nREPLAY: Починаємо програвання подій з файлу '{filename}'...")
        replay_count = 0

        try:
            with open(filename, "r", encoding="utf-8") as file:
                for line in file:
                    try:
                        log_entry = json.loads(line.strip())

                        event_name = log_entry["event"]
                        data = log_entry["data"]
                        print(f"REPLAY: Програємо подію '{event_name}'...")

                        matching_callbacks = self._get_matching_callbacks(event_name)

                        if not matching_callbacks:
                            print(f"REPLAY: Проігноровано '{event_name}' — немає активних слухачів.")
                            continue

                        task: Tuple[str, Any, List[Callable]] = (event_name, data, matching_callbacks)
                        self.queue.put(task)
                        replay_count += 1

                    except json.JSONDecodeError:
                        print(f"REPLAY ERROR: Некоректний JSON рядок: {line.strip()}")

                print(f"REPLAY: Завершено. Додано {replay_count} подій у чергу для повторної обробки.")

        except FileNotFoundError:
            print(f"REPLAY ERROR: Файл '{filename}' не знайдено.")

        except Exception as ex:
            print(f"REPLAY ERROR: Невідома помилка при читанні файлу: {ex}")

    def clear_subscriptions(self):
        """Очищає всі підписки з шини подій."""
        self._subscribers = {}
        print("BUS: Усі підписки очищено.")
