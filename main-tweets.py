from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "637217529-h0tqg8PWXDOae2lYNkJCmmsdDnbZoqsfcLUAaGRU"
access_token_secret =  "Zlk0t31Z0zwvWK1cYnxpIJW95oLg6hY4HErChLl3ztLEM"
consumer_key =  "UDyNNpAErerjeteK4YRU8lFpl"
consumer_secret =  "MJqiNNuMxxadXEwO95BtLKN1zx5JQDSNN48bqMgwV0Ggw5e3ma"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("myTopic", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="nokia")