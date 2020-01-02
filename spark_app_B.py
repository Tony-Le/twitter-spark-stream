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
    Modified by: Tilemachos Pechlivanoglou, Tony Le
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

"""

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from pprint import pprint
import csv

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
dataStream = ssc.socketTextStream("twitter", 9009)

topics = []
tags = []
# maps topics to tags
tracking_topics_dict = {}

#  Make sure our txt file has no leading or trailing spaces. strip() doesn't work to remove the leading white space
# for some reason.
with open('search-terms.txt', mode='r') as search_terms:
    reader = csv.reader(search_terms, skipinitialspace=True, delimiter=',')
    for row in reader:
        topic = row[0]
        topics.append(topic)
        hashtags = row[1].split(';')
        for tag in hashtags:
            tag = tag
            tracking_topics_dict[topic] = hashtags
            tags.append(tag)

# Filter the words to get only texts with the hashtags we are tracking
def filter(line):
    for w in line.split(' '):
        if w.lower() in tags:
            return True


contains_exact_hashtags = dataStream.filter(filter)


def mapper_to_topics(tweet):
    tweet_topic = ''
    # For each topic check if the a word in the tweet is in our dictionary of hashtags
    for a_topic in topics:
        for word in tweet.split():
            if word.lower() in tracking_topics_dict[a_topic]:
                # If its in the dictionary map the tweet to that topic. Ignore other possible topics.
                tweet_topic = a_topic
                break
    return tweet_topic, tweet


mapped_tweets_to_topics = contains_exact_hashtags.map(mapper_to_topics)

# x is tweet_topic, tweet
def mapper_to_sentiment(x):
    tweet_topic = x[0]
    tweet = x[1]
    sia = SIA()
    pol_score = sia.polarity_scores(tweet)
    return tweet_topic, [pol_score['compound'], 1]


mapped_topics_to_score = mapped_tweets_to_topics.map(mapper_to_sentiment)


# Aggregate on the topics so that for each topic there is a count of tweets in the topic
# and total compound sentiment score
# New values are coming in, aggregated_value is what has been aggregated so far
def aggregate_score_and_count(new_values, aggregated_value):
    score = 0
    count = 0
    if aggregated_value is not None:
        score = aggregated_value[0]
        count = aggregated_value[1]
    for new_value in new_values:
        score += new_value[0]
        count += new_value[1]
    return [score, count]


total_score_and_count = mapped_topics_to_score.updateStateByKey(aggregate_score_and_count)


# Calculate the average
# x is (topic, [score, count])
def map_average(x):
    tweet_topic = x[0]
    score = x[1][0]
    count = x[1][1]
    average = round((score / count), 4)
    return tweet_topic, average


average_score = total_score_and_count.map(map_average)


# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        results = rdd.collect()
        tweet_topics = []
        average_sentiment_compound = []
        for e in results:
            print('{:<40} {}'.format(e[0], e[1]))
            tweet_topics.append(e[0])
            average_sentiment_compound.append(e[1])

        request_data = {'label':str(tweet_topics), 'data':str(average_sentiment_compound)}
        # https://docs.docker.com/docker-for-windows/networking/#there-is-no-docker0-bridge-on-windows#i-want-to-connect-from-a-container-to-a-service-on-the-host
        # This is the url for docker to look at the localhost of the host in a development environment
        # app.py is a Flask framework application at port 5001 and will update the graph at localhost:5001/b when it receives this POST request
        url = 'http://host.docker.internal:5001/b/updateData'
        response = requests.post(url, data=request_data)
    except:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        print("Exception type : %s " % ex_type.__name__)
        print("Exception message : %s" % ex_value)


# do this for every single interval
average_score.foreachRDD(process_interval)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
