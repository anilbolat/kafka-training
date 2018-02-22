import threading
import logging
import time
import json
import requests
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

server = 'localhost:9092'
topicName = 'json-events'
url_events = 'https://api.flockler.com/v1/sites/846/events'

# producer
class Producer(threading.Thread):
	daemon = True
	def run(self):
		producer = KafkaProducer(bootstrap_servers=server,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
								 
	#while True:
		response = requests.get(url_events)
		json_data = json.loads(response.text)
		producer.send(topicName, json_data)

# consumer
class Consumer(threading.Thread):
    daemon = True
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=server,
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe([topicName])
        for message in consumer:
            print (message)			
			

def main():
    threads = [
        Producer(),
        Consumer()
    ]
    for t in threads:
        t.start()

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    #main()
    start = datetime.now()
    main()	
    end = datetime.now()
    elapsed = end-start
    elapsed_in_sec = elapsed.total_seconds()
    elapsed_in_min = elapsed_in_sec / 60.0
    print('elapsed_in_seconds:' + str(elapsed_in_sec))
    print('elapsed_in_minutes:' + str(elapsed_in_min))
    