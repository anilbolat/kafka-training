#import logging
#logging.basicConfig(level=logging.DEBUG)

from kafka import KafkaConsumer

consumer = KafkaConsumer('mytopic', bootstrap_servers='localhost:9092')

for message in consumer:
	print(message)
