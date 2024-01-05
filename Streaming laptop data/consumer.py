from kafka import KafkaConsumer

# Kafka yapılandırması
bootstrap_servers = '146.148.102.38:9092'
topic_name = 'topic_spark'

# Kafka consumer oluşturma
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: x.decode('utf-8'))

# Kafka'dan veri okuma ve ekrana yazdırma
for message in consumer:
    value = message.value
    print(value)

# Kafka consumer'ı kapatma
consumer.close()
