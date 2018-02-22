import threading
import logging
import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

server = 'localhost:9092'
topicName = 'loop-test-count'
limit = 1000000
timeout=10

# producer
class Producer(threading.Thread):
    daemon = True
    def run(self):
        #start = datetime.now()
        producer = KafkaProducer(bootstrap_servers=server)
        for x in range(0, limit):
            data = "test-data"
            producer.send(topicName, data.encode('utf-8'))
            time.sleep(1)
        
        end = datetime.now()
        elapsed = end-start
        elapsed_in_sec = elapsed.total_seconds()
        elapsed_in_min = elapsed_in_sec / 60.0
        print('transactions: ' + str(limit))
        print('elapsed_in_seconds: ' + str(elapsed_in_sec))
        print('elapsed_in_minutes: ' + str(elapsed_in_min))

# consumer
class Consumer(threading.Thread):
    daemon = True
    def run(self):
        count=0
        start = datetime.now()
        consumer = KafkaConsumer(bootstrap_servers=server,
                                 auto_offset_reset='earliest')
        consumer.subscribe([topicName])
        for message in consumer:
            count = count+1

        end = datetime.now()
        elapsed = end-start
        elapsed_in_sec = elapsed.total_seconds()
        elapsed_in_min = elapsed_in_sec / 60.0
        print('count:' + str(count))
        print('transactions: ' + str(limit))
        print('elapsed_in_seconds: ' + str(elapsed_in_sec))
        print('elapsed_in_minutes: ' + str(elapsed_in_min))


def main():
    threads = [
        Producer(),
        Consumer()
    ]
    for t in threads:
        t.start()
    time.sleep(timeout)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    #start = datetime.now()
    main()	
    #end = datetime.now()
    #elapsed = end-start
    #elapsed_in_sec = elapsed.total_seconds()
    #elapsed_in_min = elapsed_in_sec / 60.0
    #print('transactions: ' + str(limit))
    #print('elapsed_in_seconds: ' + str(elapsed_in_sec))
    #print('elapsed_in_minutes: ' + str(elapsed_in_min))
    