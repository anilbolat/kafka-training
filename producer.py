from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('mytopic', value=b'message 4')
producer.send('mytopic', value=b'message 5')