# Final
# Аналитическая платформа маркетплейса

## Архитектура

### Кластер 1 (Основной, 2 брокера)
- **kafka-1**, **kafka-2**: Брокеры с TLS шифрованием и репликацией factor=2
- **ACL авторизация**: Разграничение прав доступа
- **JMX Exporter**: Метрики для Prometheus

### Кластер 2 (Аналитический)
- **kafka-analytics**: Брокер для аналитики
- **MirrorMaker 2**: Дублирование данных из Кластера 1
- **ksqlDB**: Потоковая обработка SQL
- **Apache Spark**: Генерация рекомендаций
- **Kafka Connect File Sink**: Запись в файл
- **Elasticsearch**: Хранение и поиск товаров
- **HDFS**: Хранение данных для аналитики

### Компоненты
1. **Shop API** (`src/shop_api.py`) - отправка товаров через TLS
2. **Stream Processor** (`src/stream_processor.py` на Faust) - фильтрация запрещенных
3. **Manage Banned** (`src/manage_banned.py`) - CLI управление списком запрещенных
4. **Spark Analytics** (`src/spark_analytics.py` на PySpark) - генерация рекомендаций
5. **Client API** (`src/client_api.py`) - поиск в Elasticsearch + рекомендации из Kafka

### Безопасность
- **TLS/SSL**: Все брокеры используют SSL сертификаты
- **ACL**: Авторизация через AclAuthorizer
- **Репликация**: factor=2, min.insync.replicas=1

### Мониторинг
- **Prometheus**: Сбор метрик с JMX Exporter
- **Grafana**: Дашборды для визуализации
- **Alertmanager**: Оповещения в Telegram

## Запуск

### 1. Генерация TLS сертификатов

./ssl/generate-certs.sh


docker-compose up -d

docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --create --topic products-raw --partitions 3 --replication-factor 2
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --create --topic products-filtered --partitions 3 --replication-factor 2
docker exec kafka-analytics kafka-topics --bootstrap-server kafka-analytics:9095 --create --topic products-filtered-replicated --partitions 3 --replication-factor 1
docker exec kafka-analytics kafka-topics --bootstrap-server kafka-analytics:9095 --create --topic recommendations --partitions 3 --replication-factor 1


docker exec kafka-1 kafka-acls --bootstrap-server kafka-1:9092 --add --allow-principal User:shop_user --operation Write --topic products-raw
docker exec kafka-1 kafka-acls --bootstrap-server kafka-1:9092 --add --allow-principal User:stream_user --operation Read --topic products-raw


curl -X POST -H "Content-Type: application/json" --data @mm2/mirrormaker2.properties http://localhost:8084/connectors


curl -X POST -H "Content-Type: application/json" --data @data/connector-config.json http://localhost:8083/connectors

pip3 install -r requirements.txt

python3 src/shop_api.py
python3 src/stream_processor.py
python3 src/spark_analytics.py
python3 src/client_api.py


python3 src/manage_banned.py show
python3 src/manage_banned.py add "Наркотики"
python3 src/manage_banned.py remove "Табак"


Мониторинг
Grafana: http://localhost:3000 (admin/admin)
Prometheus: http://localhost:9090
Alertmanager: http://localhost:9093
Spark UI: http://localhost:8080
ksqlDB: http://localhost:8088
Elasticsearch: http://localhost:9200
HDFS NameNode: http://localhost:9870



