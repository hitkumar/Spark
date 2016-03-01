import tweepy
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time
import sys
import os

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
        self.retweet_rdds = []

    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        decoded = json.loads(data)

        retweet_count = 0
        if 'retweeted_status' in decoded:
            retweet_count = decoded['retweeted_status']['retweet_count']

        if 'text' not in decoded :
        	return True
        text = decoded['text'].encode('ascii', 'ignore')
        #print text
        tweet_id = decoded["id_str"]

        self.retweet_rdds += [sc.parallelize([(tweet_id, (text,int(retweet_count)))])]
        return True

    def on_error(self, status):
        print status

def get_output_retweet(rdd):
	if rdd.collect() == []:
		print "Please exit the program by using Ctrl C as we don't have more data to process"
		return
	rdd = rdd.takeOrdered(1, lambda x : -x[1][1])
	print "Most popular tweet with tweet count during this window is"
	print (rdd[0][1][0], rdd[0][1][1])       

if __name__ == '__main__':
    
    l = StdOutListener()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    timeInterval = int(sys.argv[1])
    windowInterval = int(sys.argv[2])

    if windowInterval > timeInterval:
    	print "Error: Window interval should be greater than time interval"
    	sys.exit(1)

    stream = tweepy.Stream(auth, l)
    stream.filter(track=['cricket'], async=True)
    time.sleep(timeInterval)

    ssc = StreamingContext(sc, 1)
    stream = ssc.queueStream(l.retweet_rdds)
    ssc.checkpoint("/Users/highlight/sparkvagrant")

    stream = stream.map(lambda line: (line[0], line[1]))
    stream = stream.reduceByKeyAndWindow(lambda x,y: max(x[1], y[1]), lambda x,y: min(x[1], y[1]), windowInterval, windowInterval, 20)

    stream.foreachRDD(get_output_retweet)

    ssc.start()
    ssc.awaitTermination()
