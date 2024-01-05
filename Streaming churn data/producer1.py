from kafka import KafkaProducer
import time

# 10 kez dönen bir döngü
for i in range(10):
    # Her seferinde yeni bir KafkaProducer oluşturuluyor. Bu, her iterasyonda aynı producer'ı kullanmak yerine
    # her seferinde yeni bir producer oluşturmayı sağlar.
    p = KafkaProducer(bootstrap_servers="146.148.102.38:9092")
    
    # Kafka topic'e bir mesaj gönderme
    p.send('ornek', b'Ya ALLAH Bismillah')
    
    # 3 saniye bekleme
    time.sleep(3)
    print("Sending datas...")
    
    # Kafka producer'ı flush etme
    p.flush()

# Bu döngü sona erdiğinde, en son oluşturulan Kafka producer'ı otomatik olarak kapanır.
