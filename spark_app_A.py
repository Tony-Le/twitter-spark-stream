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

# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))

tracking_hashtags = ['#pokemon', '#sonicthehedgehog', '#mario', '#smashbros', '#finalfantasy']

# filter the words to get only exact hashtags, but case-insensitive
hashtags = words.filter(
    lambda w: tracking_hashtags[0] == w.lower() or tracking_hashtags[1] == w.lower() or tracking_hashtags[2] == w.lower() or tracking_hashtags[3] == w.lower() or
              tracking_hashtags[4] == w.lower())

# map each hashtag to be a pair of (hashtag,1)
hashtag_counts = hashtags.map(lambda x: (x.lower(), 1))

# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)


# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        rdd_array = rdd.collect()
        hashtags = []
        counts = []
        # print it nicely
        for tag in rdd_array:
            print('{:<40} {}'.format(tag[0], tag[1]))
            hashtags.append(tag[0])
            counts.append(tag[1])

        request_data = {'label': str(hashtags), 'data': str(counts)}

        # https://docs.docker.com/docker-for-windows/networking/#there-is-no-docker0-bridge-on-windows#i-want-to-connect-from-a-container-to-a-service-on-the-host
        # This is the url for docker to look at the localhost of the host in a development environment
        # app.py is a Flask framework application at port 5001 and will update the graph at localhost:5001 when it receives this POST request
        url = 'http://host.docker.internal:5001/updateData'
        response = requests.post(url, data=request_data)
    except:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        print("Exception type : %s " % ex_type.__name__)
        print("Exception message : %s" % ex_value)


# do this for every single interval
hashtag_totals.foreachRDD(process_interval)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
