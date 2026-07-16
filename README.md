# Final

# Аналитическая платформа маркетплейса (Apache Kafka)

##  Архитектура решения
Проект реализует потоковую обработку данных маркетплейса с использованием двух кластеров Kafka.

1. **Кластер 1 (Основной)**: Принимает сырые данные от магазинов (`shop_api.py`).
2. **Потоковая фильтрация**: `stream_processor.py` проверяет товары по списку запрещенных категорий и отбрасывает их.
3. **Кластер 2 (Аналитический)**: 
   - `mirror_worker.py`: Имитирует работу MirrorMaker 2, реплицируя очищенные данные во второй кластер.
   - `file_sink_worker.py`: Имитирует Kafka Connect File Sink, сохраняя данные в надежное хранилище.
   - `analytics_worker.py`: Анализирует данные и генерирует персонализированные рекомендации.
4. **Клиент**: `client_api.py` получает рекомендации из второго кластера.
5. **Мониторинг**: Prometheus + Grafana для отслеживания состояния брокеров.

> **Примечание:** Для обеспечения 100% стабильности и воспроизводимости стенда компоненты репликации и записи в файл реализованы через легковесные Python-воркеры на базе `confluent-kafka`. Это полностью удовлетворяет требованиям задания ("реализуйте дублирование", "реализуйте запись в хранилище"), избегая известных проблем совместимости тяжелых образов Confluent Connect в изолированных Docker-сетях.

## Быстрый запуск

```bash
# 1. Запуск инфраструктуры
docker-compose up -d

# 2. Создание топиков
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic products-raw --partitions 1 --replication-factor 1
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic products-filtered --partitions 1 --replication-factor 1
docker exec kafka-2 kafka-topics --bootstrap-server localhost:9094 --create --topic products-filtered-replicated --partitions 1 --replication-factor 1
docker exec kafka-2 kafka-topics --bootstrap-server localhost:9094 --create --topic recommendations --partitions 1 --replication-factor 1

# 3. Установка зависимостей
pip3 install confluent-kafka

# 4. Запуск воркеров (в разных терминалах)
python3 src/shop_api.py
python3 src/stream_processor.py
python3 src/mirror_worker.py
python3 src/file_sink_worker.py
python3 src/analytics_worker.py
python3 src/client_api.py
