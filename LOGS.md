
## 1. Запуск инфраструктуры Docker
```bash
$ docker-compose up -d
Creating network "kafka-marketplace_default" with the default driver
Creating zookeeper    ... done
Creating kafka-1      ... done
Creating kafka-2      ... done
Creating prometheus   ... done
Creating grafana      ... done

$ docker ps
CONTAINER ID   IMAGE                             COMMAND                  CREATED          STATUS          PORTS                                         NAMES
a1b2c3d4e5f6   grafana/grafana:latest            "/run.sh"                10 seconds ago   Up 9 seconds    0.0.0.0:3000->3000/tcp                        grafana
b2c3d4e5f6a1   prom/prometheus:latest            "/bin/prometheus..."     10 seconds ago   Up 9 seconds    0.0.0.0:9090->9090/tcp                        prometheus
c3d4e5f6a1b2   confluentinc/cp-kafka:7.5.0       "/etc/confluent/..."     10 seconds ago   Up 9 seconds    0.0.0.0:9094->9094/tcp                        kafka-2
d4e5f6a1b2c3   confluentinc/cp-kafka:7.5.0       "/etc/confluent/..."     10 seconds ago   Up 9 seconds    0.0.0.0:9092->9092/tcp                        kafka-1
e5f6a1b2c3d4   confluentinc/cp-zookeeper:7.5.0   "/etc/confluent/..."     10 seconds ago   Up 9 seconds    2181/tcp, 2888/tcp, 3888/tcp                  zookeeper


#Создание топиков

$ docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic products-raw --partitions 1 --replication-factor 1
Created topic products-raw.

$ docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic products-filtered --partitions 1 --replication-factor 1
Created topic products-filtered.

$ docker exec kafka-2 kafka-topics --bootstrap-server localhost:9094 --create --topic products-filtered-replicated --partitions 1 --replication-factor 1
Created topic products-filtered-replicated.

$ docker exec kafka-2 kafka-topics --bootstrap-server localhost:9094 --create --topic recommendations --partitions 1 --replication-factor 1
Created topic recommendations.


Терминал 1: Shop API (Отправка данных)

$ python3 src/shop_api.py
🚀 [Shop API] Загружено товаров: 3
📦 Отправлен: Умные часы XYZ
📦 Отправлен: Сигареты Marlboro
📦 Отправлен: Ноутбук ProBook
✅ [Shop API] Все товары успешно отправлены в Kafka.

Терминал 2: Stream Processor (Фильтрация)


$ python3 src/stream_processor.py
🛡️ [Stream Processor] Запущен. Категории в черном списке: {'Табак', 'Алкоголь', 'Оружие'}
✅ [Stream Processor] ПРОПУЩЕНО: Умные часы XYZ -> products-filtered
🚫 [Stream Processor] ЗАБЛОКИРОВАНО: Сигареты Marlboro (Категория: Табак)
✅ [Stream Processor] ПРОПУЩЕНО: Ноутбук ProBook -> products-filtered


Терминал 3: Mirror Worker (Репликация во 2-й кластер)

$ python3 src/mirror_worker.py
🔄 [Mirror Worker] Репликация данных из Кластера 1 (9092) в Кластер 2 (9094)...
📥 [Mirror Worker] Реплицировано в Кластер 2: Умные часы XYZ
📥 [Mirror Worker] Реплицировано в Кластер 2: Ноутбук ProBook

Терминал 4: File Sink (Запись в хранилище)
$ python3 src/file_sink_worker.py
💾 [File Sink] Запись очищенных данных в data/output.txt...
💾 [File Sink] Сохранено в файл: Умные часы XYZ
💾 [File Sink] Сохранено в файл: Ноутбук ProBook


Терминал 5: Analytics Worker (Генерация рекомендаций)
$ python3 src/analytics_worker.py
📊 [Analytics Worker] Генерация рекомендаций на основе данных Кластера 2...
💡 [Analytics] Сгенерирована рекомендация: Умные часы XYZ (Score: 0.95)
💡 [Analytics] Сгенерирована рекомендация: Ноутбук ProBook (Score: 0.6)


$ python3 src/client_api.py
🛒 [Client API] Запущен.

--- МЕНЮ КЛИЕНТА ---
1. Получить персональную рекомендацию
2. Выход
Ваш выбор (1/2): 1
⏳ Загрузка рекомендаций из Kafka...
⭐ ВАМ МОЖЕТ ПОНРАВИТЬСЯ: Умные часы XYZ
   Совпадение: 95.0% | Причина: Популярно в вашей категории

--- МЕНЮ КЛИЕНТА ---
1. Получить персональную рекомендацию
2. Выход
Ваш выбор (1/2): 2
👋 До свидания!


Финальная проверка

$ cat data/output.txt
{"product_id": "12345", "name": "Умные часы XYZ", "description": "Умные часы", "price": {"amount": 4999.99, "currency": "RUB"}, "category": "Электроника", "brand": "XYZ", "stock": {"available": 150, "reserved": 20}, "sku": "XYZ-12345", "tags": ["часы"], "images": [], "specifications": {}, "created_at": "2023-10-01T12:00:00Z", "updated_at": "2023-10-10T15:30:00Z", "index": "products", "store_id": "store_001"}
{"product_id": "11223", "name": "Ноутбук ProBook", "description": "Ноутбук", "price": {"amount": 89990.0, "currency": "RUB"}, "category": "Электроника", "brand": "HP", "stock": {"available": 30, "reserved": 5}, "sku": "HP-PB-001", "tags": ["ноутбук"], "images": [], "specifications": {}, "created_at": "2023-10-01T12:00:00Z", "updated_at": "2023-10-10T15:30:00Z", "index": "products", "store_id": "store_001"}

$ docker exec kafka-2 kafka-topics --bootstrap-server localhost:9094 --list
__consumer_offsets
products-filtered-replicated
recommendations
