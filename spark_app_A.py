from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import re
import requests

conf = SparkConf() # Creates a new Spark object.
conf.setAppName("TwitterStreamApp")

sc = SparkContext(conf=conf) # The real crux and MVP, this will aid in us creating RDDs and utilizing Spark features.
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 2) # Sets the batch processing to every 2 seconds.

ssc.checkpoint("checkpoint_TwitterApp") # Sets up a checkpoint for RDD recovery.

dataStream = ssc.socketTextStream("twitter", 9009) # Now this is where we connect to our Twitter app and get the tweets.

# The datastream is the object coming in from the twitter_app.
# we (permanently) flat map it based on splitting it by space character.
# And then we put it into words.
words = dataStream.flatMap(lambda line: line.split(" ")) # ??? How does flatMap work?

# Filter the words to get only hashtags you want.
tags = ["#apple", "#amazon", "#microsoft", "#facebook", "#google"]
# Other hashtags I've used:
# tags = ["asia","europe","middleeast","america","africa","thanksgiving","blackfriday"]

hashtags = words.filter(lambda w: w.lower() in tags) # We use lower becuase some people use different variations of the same word.

# Map each hashtag to be a pair of (hashtag,1)
hashtag_counts = hashtags.map(lambda x: (x.lower(), 1)) # We check the lowercase 

# Adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# ??? Do the aggregation, note that now this is a sequence of RDDs 
hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)

# Process a single time interval
def process_interval(time, rdd):
    # Print a separator
    print("----------- %s -----------" % str(time))
    try:
        # ??? Sort counts (desc) in this time instance and take top 10
        sorted_rdd = rdd.sortBy(lambda x:x[1], False)
        top10 = sorted_rdd.take(10)
        f = open("results.txt","w+") # We recreate the file from scratch to add the results (and so to not overlap with past values.)
        # print it nicely
        for tag in top10:
            print('{}: {}'.format(tag[0], tag[1]))
            f.write('{}: {}\n'.format(tag[0], tag[1])) # We also print the output of the Apache stream to a file.
        f.close()

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# Do this for every single interval
hashtag_totals.foreachRDD(process_interval)

# This starts the context streaming computation:
ssc.start()
# Wait for the streaming to finish:
ssc.awaitTermination()