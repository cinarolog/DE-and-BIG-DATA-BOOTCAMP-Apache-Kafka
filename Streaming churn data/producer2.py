import json
from kafka import KafkaProducer
import pandas as pd
import time

# Kafka broker'larının adresi ve portu
bootstrap_servers = '146.148.102.38:9092'
# Kafka topic adı
topic_name = 'topic_nifi'

# Kafka producer oluşturma
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

# CSV dosyasını pandas kullanarak okuma
df = pd.read_csv("churn.csv")

# Her satırı JSON formatına çevirip Kafka topic'ine gönderme
for _, row in df.iterrows():
    # Satırı bir Python sözlüğüne çevirme
    to_json = row.to_dict()
    
    # Debug amaçlı, her satırın bilgisini ekrana yazdırma
    print(_)
    print("Aman yarabbim Sending Datas...")
    
    # Kafka topic'ine JSON verisini gönderme
    producer.send(topic_name, value=to_json)
    
    # Kafka producer'ı flush etme
    producer.flush()
    
    # 3 saniye bekleyerek bir sonraki satıra geçme
    time.sleep(3)

# Kafka producer'ı kapatma
producer.close()
