import json
import os
import collections
from typing import Dict, Any, List

LOG_DIR = "kafka_logs"
OFFSET_DIR = "kafka_offsets"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OFFSET_DIR, exist_ok=True)


class FileProducer:
    """
    Імітує виробника Kafka. Відправляє повідомлення до топіка,
    записуючи їх у відповідний лог-файл у директорії 'kafka_logs'.
    """

    def send(self, topic: str, message: Dict[str, Any]):
        """
        Відправляє повідомлення до топіка, записуючи його як рядок JSON у файл {topic}.log.

        :param topic: Назва топіка, до якого потрібно відправити повідомлення.
        :type topic: str
        :param message: Повідомлення для відправки (має бути серіалізовано в JSON).
        :type message: Dict[str, Any]
        """
        log_file_path = os.path.join(LOG_DIR, f"{topic}.log")

        log_entry = json.dumps(message) + "\n"

        try:
            with open(log_file_path, "a", encoding='utf-8') as file:
                file.write(log_entry)
            print(f"PRODUCER: надіслано до '{topic}'. Повідомлення: {message}")
        except Exception as ex:
            print(f"PRODUCER ERROR: Не вдалося звписати у файл {log_file_path}: {ex}")


class FileConsumer:
    """
    Імітує споживача Kafka. Читає події з лог-файлів, використовуючи офсети,
    зберігаючи їх у директорії 'kafka_offsets' для забезпечення 'at least once'
    семантики.
    """

    def __init__(self, group_id: str):
        """
        Ініціалізує споживача. Завантажує останній збережений офсет для
        кожного топіка.

        :param group_id: Унікальний ідентифікатор групи споживачів (Consumer Group ID).
                         Використовується для ізоляції офсетів між різними групами.
        :type group_id: str
        """
        self.group_id = group_id
        """ Ідентифікатор групи споживачів. """

        self.offsets: Dict[str, int] = collections.defaultdict(int)
        """ Словник для зберігання поточних офсетів: {topic_name: byte_offset}. """

        self._load_offsets()
        """ Завантажує офсети з файлів при ініціалізації. """

    def _get_offset_file_path(self, topic: str) -> str:
        """
        Внутрішній метод для формування шляху до файлу офсетів для конкретного топіка.
        Формат: {group_id}_{topic}.offsets
        """
        return os.path.join(OFFSET_DIR, f"{self.group_id}_{topic}.offsets")

    def _load_offsets(self):
        """
        Внутрішній метод. Завантажує офсети для цієї групи споживачів з директорії 'kafka_offsets'.
        Якщо файл не знайдено або він пошкоджений, офсет встановлюється на 0.
        """
        for filename in os.listdir(OFFSET_DIR):
            if filename.startswith(f"{self.group_id}_") and filename.endswith(".offset"):
                topic = filename.replace(f"{self.group_id}_", "").replace(".offset", "")
                path = os.path.join(OFFSET_DIR, filename)
                try:
                    with open(path, 'r', encoding='utf-8') as file:
                        self.offsets[topic] = int(file.read().strip())
                except (FileNotFoundError, ValueError, IndexError):
                    self.offsets[topic] = 0

    def _save_offset(self, topic: str, new_offset: int):
        """
        Внутрішній метод. Зберігає новий офсет для топіка у файл.

        :param topic: Топік.
        :type topic: str
        :param new_offset: Новий офсет (позиція в байтах), з якого слід почати читання наступного разу.
        :type new_offset: int
        """
        path = self._get_offset_file_path(topic)
        with open(path, 'w', encoding='utf-8') as file:
            file.write(str(new_offset))
        self.offsets[topic] = new_offset

    def subscribe(self, topic: str):
        """
        Підписує споживача на топік, ініціалізуючи його офсет, якщо це необхідно.

        :param topic: Назва топіка для підписки.
        :type topic: str
        """
        if topic not in self.offsets:
            self.offsets[topic] = 0

        print(f"CONSUMER {self.group_id}: Підписано на '{topic}'. Початковий офсет: {self.offsets[topic]}")

    def poll(self, topics: List[str] = None) -> List[Dict[str, Any]]:
        """
        Запитує нові повідомлення з підписаних топіків.

        1. Використовує збережений офсет (`file.seek(current_offset)`) для початку читання.
        2. Читає всі нові рядки (повідомлення).
        3. Обчислює новий офсет (сума байтової довжини всіх прочитаних рядків).
        4. Зберігає новий офсет (`_save_offset`).

        Це імітує читання та збереження офсету в один цикл, забезпечуючи,
        що наступний poll почнеться з кінця поточного.

        :param topics: Список топіків для опитування. Якщо None, опитуються всі підписані топіки.
        :type topics: Optional[List[str]]
        :return: Список нових подій/повідомлень. До кожного додається ключ '__topic__'.
        :rtype: List[Dict[str, Any]]
        """
        if topics is None:
            topics = list(self.offsets.keys())

        new_events = []

        for topic in topics:
            log_file_path = os.path.join(LOG_DIR, f"{topic}.log")

            if not os.path.exists(log_file_path):
                continue

            current_offset = self.offsets.get(topic, 0)

            try:
                with open(log_file_path, 'r', encoding='utf-8') as file:
                    file.seek(current_offset)

                    new_lines = file.readlines()

                    if not new_lines:
                        continue

                    new_offset = current_offset + sum(len(line.encode('utf-8')) for line in new_lines)

                    for line in new_lines:
                        try:
                            event = json.loads(line.strip())
                            event['__topic__'] = topic
                            new_events.append(event)
                        except json.decoder.JSONDecodeError:
                            print(f"CONSUMEr WARNING: Пропущено некоректний JSON у {topic}.log")

                    self._save_offset(topic, new_offset)
                    print(
                        f"CONSUMER {self.group_id}: Прочитано {len(new_lines)} подій з '{topic}'. Новий офсет: {new_offset}")

            except Exception as ex:
                print(f"CONSUMER ERROR: Помилка читання топіка {topic}: {ex}")

        return new_events
