# Spark Streaming PySpark

I use Python's Tweepy library to get twitter stream. Spark has inbuilt functions for twitter stream in Java and Spark API, but it is not available for Python.
There are three tasks in this repo. Sample outputs are in output folder for these.

a) Finding most popular hashtags in a twitter stream.
How to run
pyspark hashtags.py <Stream_Length> <Window_Size>

where <Stream_Length> is the total length of twitter stream it looks at. <Window_size> is the window across we want to do our calculation. Both are in seconds. 
It prints the most common hastags in the stream.

b) Find the tweet that was most retweeted.

How to run
pyspark retweet.py <Stream_Length> <Window_Size>

where parameters are same as above.
It prints the most common tweet.

Both 1 and 2 print "Please exit the program by using Ctrl C as we don't have more data to process" when there is nothing to process. There should be better ways to stop a spark streaming program, but I didn't spend too much time on this.

c) Find whether a tweet is positive or negative

pyspark sentiment.py <Window>

where Window is the time you should run this for. It analyses all tweets in this time and prints whether they are positive or negative, Since tweets are so small, there are some cases where it says "Can't say". We use word sets for positive and neagtive words from the internet. Better datasets might lead to better results. 
