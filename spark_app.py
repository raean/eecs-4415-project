"""
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text. That stream is
    meant to be read and processed here, where top trending hashtags are
    identified. Both apps are designed to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

    and inside the docker:

        spark-submit spark_app.py

    For more instructions on how to run, refer to final tutorial 8 slides.

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

"""
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

# samsung = ["#galaxynote9", "#samsung","#samsunggalaxy","#s8","#galaxya7","#samsungtv","#samsungtab", "#s9", "#galaxytabs4","#samsung"]
apple = ["#iphone", "#iphonexs","#ipad", "#ipod", "#iwatch" , "#imac", "#macbookpro", "#iphonexr", "#apple", "#iphonexr"]
google = ["#pixel", "#firebase","#google","#chromebook","#googlehome","#chromecast","#googlehomemini", "#pixel3", "#googleassistant","#dialogflow"]
microsoft = ["#microsoftoffice", "#powerpoint","#microsoftword","#excel","#surfacepro","#azure","#office360", "#surface", "#xbox","#windows"]
ibm = ["#watson", "#ibmcloud","#db2","#redhat","#bpm","#ibmblockchain","#hyperledger", "#cognitivecomputing", "#ibm","#ibmwebsphere"]
amazon = ["#alexa", "#echo", "#echodot", "#kindle", "#aws", "#amazonprime", "#amazonvideo", "#amazon", "#amazonmusic", "#audible"]
# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter",9009)

# we change line to all lowercase so we can pick hashtags even if they appear in caps or mixed
# get only the apply tweets
def apple_filter(line):
    hashtags_count = 0
    for hashtag in apple:
        if hashtag in line.lower():
            hashtags_count += 1

    if hashtags_count > 0:
        return line

# get only the google tweets
def google_filter(line):
    hashtags_count = 0
    for hashtag in google:
        if hashtag in line.lower():
            hashtags_count += 1

    if hashtags_count > 0:
        return line

# get only the microsoft tweets
def microsoft_filter(line):
    hashtags_count = 0
    for hashtag in microsoft:
        if hashtag in line.lower():
            hashtags_count += 1

    if hashtags_count > 0:
        return line

# get only the ibm tweets
def ibm_filter(line):
    hashtags_count = 0
    for hashtag in ibm:
        if hashtag in line.lower():
            hashtags_count += 1

    if hashtags_count > 0:
        return line

# get only the amazon tweets
def amazon_filter(line):
    hashtags_count = 0
    for hashtag in amazon:
        if hashtag in line.lower():
            hashtags_count += 1

    if hashtags_count > 0:
        return line

# now we get the actual tweets using our filter function for each of the topics
apple_tweets = dataStream.filter(apple_filter)
amazon_tweets = dataStream.filter(amazon_filter)
google_tweets = dataStream.filter(google_filter)
ibm_tweets = dataStream.filter(ibm_filter)
microsoft_tweets = dataStream.filter(microsoft_filter)

# very repetitive since we apply the same filter, but we had to get the actual sentiment
apple_tweet_sentiment = apple_tweets.filter(lambda w: "|||||" in w)
amazon_tweet_sentiment = amazon_tweets.filter(lambda w: "|||||" in w)
google_tweet_sentiment = google_tweets.filter(lambda w: "|||||" in w)
ibm_tweet_sentiment = ibm_tweets.filter(lambda w: "|||||" in w)
microsoft_tweet_sentiment = microsoft_tweets.filter(lambda w: "|||||" in w)

# we use this to map the sentiment of the tweet to 1, and we make it different for each topic
# so we can append the topic to the sentiment for easier processing
def get_only_apple_sentiment(line):
    tweet =  line.split("|||||")
    sentiment = tweet[1]
   
    return(str(sentiment)+'|Apple', 1)

def get_only_amazon_sentiment(line):
    tweet =  line.split("|||||")
    sentiment = tweet[1]
   
    return(str(sentiment)+'|Amazon', 1)

def get_only_google_sentiment(line):
    tweet =  line.split("|||||")
    sentiment = tweet[1]
   
    return(str(sentiment)+'|Google', 1)

def get_Ibm_only_sentiment(line):
    tweet =  line.split("|||||")
    sentiment = tweet[1]
   
    return(str(sentiment)+'|Ibm', 1)

def get_only_microsoft_sentiment(line):
    tweet =  line.split("|||||")
    sentiment = tweet[1]
   
    return(str(sentiment)+'|Microsoft', 1)

apple_sentiment = apple_tweet_sentiment.map(get_only_apple_sentiment)
amazon_sentiment = amazon_tweet_sentiment.map(get_only_amazon_sentiment)
google_sentiment = google_tweet_sentiment.map(get_only_google_sentiment)
ibm_sentiment = ibm_tweet_sentiment.map(get_Ibm_only_sentiment)
microsoft_sentiment = microsoft_tweet_sentiment.map(get_only_microsoft_sentiment)

# get the total sentiments by putting it all together using union
all_sentiment = apple_sentiment.union(amazon_sentiment).union(google_sentiment).union(
        ibm_sentiment).union(microsoft_sentiment)

# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
all_sentiment_totals = all_sentiment.updateStateByKey(aggregate_tags_count)

# -------------- processing intervals -------------------- #

# process a single time interval for all the topics
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # sort counts (desc) in this time instance and take top 10
        sorted_rdd = rdd.sortBy(lambda x:x[1], False)
        top10 = sorted_rdd.take(10)
        send_info_to_dashboard(top10)
        # print it nicely
        for tag in top10:
            print('{:<40} {}'.format(tag[0], tag[1]))
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# -------------------- info to dashboards section -----------------------------

# we send the tags, and the count for the topics
def send_info_to_dashboard(top10):
        # extract the hashtags from dataframe and convert them into array
        top_tags = [str(tag[0]) for tag in top10]
       
        # extract the counts from dataframe and convert them into array
        tags_count = [tag[1] for tag in top10]
        # initialize and send the data through REST API
        url = 'http://dockerhost:5001/updateData'
        request_data = {'label': str(top_tags), 'data': str(tags_count)}
        requests.post(url, data=request_data)


# do this for every single interval
all_sentiment_totals.foreachRDD(process_interval)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
