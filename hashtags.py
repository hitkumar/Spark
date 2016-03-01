import tweepy
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time
import sys

# Authentication details. To  obtain these visit dev.twitter.com
consumer_key = '82Bj4rVMEbHfXxTKNGoa2CnYq'
consumer_secret = 'Ol6Y8MGJqmNQc2wk4gI26y5vaUPtEDkeC4ikyyRSjSJCUZABtD'
access_token = '701633329896927232-71Gv82BTfxZtdUyaWdnp5Uz1luthr3l'
access_token_secret = 'NwBOWZpF8uSDxLO4AInSstXssylPn8iwcYRJ4R7rruslz'
sc = SparkContext(appName="PythonStreamingQueueStream")

# This is the listener, resposible for receiving data
class StdOutListener(tweepy.StreamListener):

    def __init__(self):
        super(StdOutListener, self).__init__
        self.rdds = []

    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        decoded = json.loads(data)
        text = decoded['text'].encode('ascii', 'ignore')
        tags = decoded['entities']['hashtags']
        tagText = [tag['text'].encode('utf-8') for tag in tags]
        self.rdds += [sc.parallelize([tag['text'].encode('utf-8') for tag in tags])]
        return True

    def on_error(self, status):
        print status

def get_output(rdd):
    rdd = rdd.map(lambda line: (line[1], line[0]))
    rdd = rdd.takeOrdered(25, key=lambda x: -x[0])
    mostCommon = rdd
    if mostCommon == []:
        print "Please exit the program by using Ctrl C as we don't have more data to process"
        return
    print "Most popular hastags with their counts in this window is"    
    for word in mostCommon:
        print word

if __name__ == '__main__':
    
    l = StdOutListener()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = tweepy.Stream(auth, l)
    stream.filter(track=['cricket'], async=True)
    timeInterval = int(sys.argv[1])
    windowInterval = int(sys.argv[2])

    if windowInterval > timeInterval:
        print "Error: Window interval should be greater than time interval"
        sys.exit(1)

    time.sleep(timeInterval)

    ssc = StreamingContext(sc, 1)
    stream = ssc.queueStream(l.rdds)
    ssc.checkpoint("/Users/highlight/sparkvagrant")
    stream = stream.map(lambda line: (line, 1))

    stream = stream.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, windowInterval, windowInterval, 20)
    
    stream.foreachRDD(get_output)

    ssc.start()
    ssc.awaitTermination()
