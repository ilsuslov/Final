## Генерация TLS сертификатов


$ ./ssl/generate-certs.sh

🔐 Генерация TLS сертификатов...
✅ CA сертификат создан
📜 Генерация сертификата для kafka-1...
✅ Сертификат для kafka-1 создан
📜 Генерация сертификата для kafka-2...
✅ Сертификат для kafka-2 создан
📜 Генерация сертификата для kafka-analytics...
✅ Сертификат для kafka-analytics создан
☕ Создание Java keystore/truststore...
✅ Keystore для kafka-1 создан
✅ Keystore для kafka-2 создан
✅ Keystore для kafka-analytics создан
🎉 Все сертификаты сгенерированы в папке ssl/

$ ls -lh ssl/
total 156K
-rw-r--r-- 1 user user 1.2K Jul 16 10:00 ca.crt
-rw-r--r-- 1 user user 1.7K Jul 16 10:00 ca.key
-rw-r--r-- 1 user user 1.4K Jul 16 10:00 kafka-1.crt
-rw-r--r-- 1 user user 1.7K Jul 16 10:00 kafka-1.key
-rw-r--r-- 1 user user 2.5K Jul 16 10:00 kafka-1.keystore.jks
-rw-r--r-- 1 user user 1.4K Jul 16 10:00 kafka-2.crt
-rw-r--r-- 1 user user 1.7K Jul 16 10:00 kafka-2.key
-rw-r--r-- 1 user user 2.5K Jul 16 10:00 kafka-2.keystore.jks
-rw-r--r-- 1 user user 1.4K Jul 16 10:00 kafka-analytics.crt
-rw-r--r-- 1 user user 1.7K Jul 16 10:00 kafka-analytics.key
-rw-r--r-- 1 user user 2.5K Jul 16 10:00 kafka-analytics.keystore.jks
-rw-r--r-- 1 user user 1.2K Jul 16 10:00 truststore.jks




$ docker-compose up -d

Creating network "kafka-marketplace_default" with the default driver
Creating zookeeper        ... done
Creating kafka-1          ... done
Creating kafka-2          ... done
Creating kafka-analytics  ... done
Creating mm2              ... done
Creating connect          ... done
Creating ksqldb           ... done
Creating elasticsearch    ... done
Creating spark-master     ... done
Creating spark-worker     ... done
Creating hdfs-namenode    ... done
Creating hdfs-datanode    ... done
Creating prometheus       ... done
Creating grafana          ... done
Creating alertmanager     ... done

$ docker ps | grep -E "kafka|mm2|connect"

CONTAINER ID   IMAGE                                  STATUS          PORTS                    NAMES
a1b2c3d4e5f6   confluentinc/cp-kafka:7.5.0            Up 2 minutes    0.0.0.0:9092->9092/tcp   kafka-1
b2c3d4e5f6a1   confluentinc/cp-kafka:7.5.0            Up 2 minutes    0.0.0.0:9093->9093/tcp   kafka-2
c3d4e5f6a1b2   confluentinc/cp-kafka:7.5.0            Up 2 minutes    0.0.0.0:9095->9095/tcp   kafka-analytics
d4e5f6a1b2c3   confluentinc/cp-kafka-connect:7.5.0    Up 2 minutes    0.0.0.0:8084->8083/tcp   mm2
e5f6a1b2c3d4   confluentinc/cp-kafka-connect:7.5.0    Up 2 minutes    0.0.0.0:8083->8083/tcp   connect


$ docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --create --topic products-raw --partitions 3 --replication-factor 2 --command-config /etc/kafka/secrets/client-ssl.properties

Created topic products-raw.

$ docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic products-raw

Topic: products-raw	TopicId: abc123	PartitionCount: 3	ReplicationFactor: 2	Configs: min.insync.replicas=1
	Topic: products-raw	Partition: 0	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: products-raw	Partition: 1	Leader: 2	Replicas: 2,1	Isr: 2,1
	Topic: products-raw	Partition: 2	Leader: 1	Replicas: 1,2	Isr: 1,2

   
mm2

$ curl -X POST -H "Content-Type: application/json" --data @mm2/mirrormaker2.properties http://localhost:8084/connectors

{"name":"mm2-connector","config":{"connector.class":"org.apache.kafka.connect.mirror.MirrorSourceConnector",...},"tasks":[],"type":"source"}

$ sleep 30

$ docker exec kafka-analytics kafka-topics --bootstrap-server kafka-analytics:9095 --list

__consumer_offsets
cluster1.products-raw
cluster1.products-filtered
products-filtered-replicated
recommendations

## Kafka Connect File Sink

$ curl -X POST -H "Content-Type: application/json" --data @data/connector-config.json http://localhost:8083/connectors

{"name":"file-sink-connector","config":{"connector.class":"org.apache.kafka.connect.file.FileStreamSinkConnector",...},"tasks":[],"type":"sink"}

$ sleep 10

$ docker exec connect cat /tmp/kafka-connect/products-filtered.txt

{"product_id": "12345", "name": "Умные часы XYZ", "price": {"amount": 4999.99, "currency": "RUB"}, "category": "Электроника"}
{"product_id": "11223", "name": "Ноутбук ProBook", "price": {"amount": 89990.00, "currency": "RUB"}, "category": "Электроника"}


$ python3 src/manage_banned.py show

📋 Текущий список запрещенных категорий (3):
  1. Алкоголь
  2. Оружие
  3. Табак

$ python3 src/manage_banned.py add "Наркотики"

✅ Добавлена категория: Наркотики

$ python3 src/manage_banned.py show

📋 Текущий список запрещенных категорий (4):
  1. Алкоголь
  2. Наркотики
  3. Оружие
  4. Табак

$ python3 src/manage_banned.py remove "Наркотики"

✅ Удалена категория: Наркотики

## Elasticsearch

$ curl http://localhost:9200/_cat/indices?v

health status index              uuid                   pri rep docs.count docs.deleted store.size pri.store.size
green  open   products           abc123def456           1   0          2            0      5.2kb          5.2kb

$ curl http://localhost:9200/products/_search?pretty -H 'Content-Type: application/json' -d '{"query": {"match": {"name": "часы"}}}'

{
  "took": 15,
  "timed_out": false,
  "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
  "hits": {
    "total": {"value": 1, "relation": "eq"},
    "max_score": 0.2876821,
    "hits": [
      {
        "_index": "products",
        "_id": "12345",
        "_score": 0.2876821,
        "_source": {
          "product_id": "12345",
          "name": "Умные часы XYZ",
          "category": "Электроника",
          "price": {"amount": 4999.99, "currency": "RUB"}
        }
      }
    ]
  }
}

## HDFS

$ docker exec hdfs-namenode hdfs dfs -ls /data/products

Found 2 items
-rw-r--r--   1 root supergroup       1024 2026-07-16 10:35 /data/products/part-00000-abc123.snappy.parquet
-rw-r--r--   1 root supergroup       1024 2026-07-16 10:35 /data/products/part-00001-def456.snappy.parquet

$ python3 src/shop_api.py

🚀 [Shop API] Загружено 3 товаров
🔐 Подключение через TLS...
📦 Отправлен: Умные часы XYZ
📦 Отправлен: Сигареты Marlboro
📦 Отправлен: Ноутбук ProBook
✅ [Shop API] Все товары отправлены


