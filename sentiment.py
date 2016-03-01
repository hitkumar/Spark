import tweepy
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time
import re
import sys

# Authentication details. To  obtain these visit dev.twitter.com
consumer_key = '82Bj4rVMEbHfXxTKNGoa2CnYq'
consumer_secret = 'Ol6Y8MGJqmNQc2wk4gI26y5vaUPtEDkeC4ikyyRSjSJCUZABtD'
access_token = '701633329896927232-71Gv82BTfxZtdUyaWdnp5Uz1luthr3l'
access_token_secret = 'NwBOWZpF8uSDxLO4AInSstXssylPn8iwcYRJ4R7rruslz'
sc = SparkContext(appName="PythonStreamingQueueStream")

#Sample set of Negative words, positive words and stop words
neg_words_path = "resources/neg-words.txt"
neg_words = sc.textFile(neg_words_path).collect()

pos_words_path = "resources/pos-words.txt"
pos_words = sc.textFile(pos_words_path).collect()

stop_words_path = "resources/stop-words.txt"
stop_words = sc.textFile(stop_words_path).collect()

# This is the listener, resposible for receiving data
class StdOutListener(tweepy.StreamListener):

    def __init__(self):
        super(StdOutListener, self).__init__
        self.rdds = []

    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        decoded = json.loads(data)

        if 'text' not in decoded :
        	return True
        text = decoded['text'].encode('ascii', 'ignore')
        if text == "":
        	return True

        tweet_id = decoded["id_str"]
        self.rdds += [sc.parallelize([(tweet_id, text)])]
        return True

    def on_error(self, status):
        print status

def get_output(rdd):
	print rdd.collect()

def filter_stopwords(rdd):
    line = rdd[1]
    for word in stop_words:
        reMatch = "\\b" + word + "\\b"
        line = re.sub(reMatch, "", line)
    return rdd[0], line

def get_with_positive_score(rdd):
    line = rdd[1]
    words = line.split()
    numWords = len(words) + 0.0
    numPosWords = 0.0
    for word in words:
        if word in pos_words:
            print "Pos word is " + word
            numPosWords = numPosWords + 1.0

    return (rdd[0], line), numPosWords/numWords

def get_with_negative_score(rdd):
    line = rdd[1]
    words = line.split()
    numWords = len(words) + 0.0
    numNegWords = 0.0
    for word in words:
        if word in neg_words:
            print "neg word is " + word
            numNegWords = numNegWords + 1.0
    return (rdd[0], line), numNegWords/numWords


def score_tweets(rdd):
    score = "positive"
    if rdd[2] < rdd[3]:
        score = "negative"
    elif rdd[2] == rdd[3]:
        score = "Can't Say"
        
    return rdd[0], rdd[1], rdd[2], rdd[3], score
         
if __name__ == '__main__':
	
    l = StdOutListener()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    timeInterval = int(sys.argv[1])

    stream = tweepy.Stream(auth, l)
    stream.filter(track=['code'], async=True)
    time.sleep(timeInterval)

    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/Users/highlight/sparkvagrant") 
    stream = ssc.queueStream(l.rdds)

    #remove all special characters from tweets
    stream = stream.map(lambda line: (line[0], re.sub("[^a-zA-Z0-9\s]", " ", line[1]).strip().lower()))

    #remove stop words
    stream = stream.map(filter_stopwords)

    #Get positive scores for tweets
    pos_scores = stream.map(get_with_positive_score)

    #Get Negative scores for tweets
    neg_scores = stream.map(get_with_negative_score)

    #join postive and negative scores
    joined = pos_scores.join(neg_scores)

    #flatten the tweets rdd to include both pos and negative scores
    scored_tweets = joined.map(lambda line: (line[0][0], line[0][1], line[1][0], line[1][1]))
    
    #Assign scores to tweets, could be Positive, Negative or Can't say
    result = scored_tweets.map(score_tweets)

    #Prints the score of each tweet with other details
    print "TweetId, Stemmed Tweet, Positive Score, Negative score, Overall Score"
    result.foreachRDD(get_output)

    ssc.start()
    ssc.awaitTermination()       
