from pyspark.sql import SparkSession
import sys
import time
import json
import boto3
from boto3 import resource
import tweepy
import datetime
from tweepy import API
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pyspark.sql.types import *
import pyspark.sql.functions as F
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql.functions import col, udf
from pyspark.ml.clustering import LDA, LDAModel
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel, Tokenizer, RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.sql.functions import regexp_replace, trim, col, lower
from pyspark.ml.clustering import LDA, LDAModel
import pandas 
from graphframes import GraphFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import desc

sparkSession = SparkSession.builder.appName("tweet-collection").getOrCreate()

twitter_date_format="EEE MMM dd HH:mm:ss ZZZZZ yyyy"

user_schema = StructType([
    StructField('created_at',TimestampType(),True),
    StructField('followers_count',LongType(),True),
    StructField('id',LongType(),True),
    StructField('name',StringType(),True),
    StructField('screen_name',StringType(),True)
])

hashtag_schema = ArrayType(StructType([StructField('text',StringType(),True)]))

user_mentions_schema = ArrayType(StructType([StructField('id',LongType(),True), StructField('screen_name',StringType(),True)]))

entities_schema = StructType([
    StructField('hashtags',hashtag_schema,True),
    StructField('user_mentions',user_mentions_schema,True)
])

retweeted_status_schema =StructType([
    StructField("id", LongType(), True),
    StructField("in_reply_to_user_id", LongType(), True),
    StructField("in_reply_to_status_id", LongType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("user", user_schema)
])

tweet_schema =StructType([
    StructField("text", StringType(), True),
    StructField("id", LongType(), True),
    StructField("in_reply_to_user_id", LongType(), True),
    StructField("in_reply_to_status_id", LongType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("user", user_schema),
    StructField("entities", entities_schema),
    StructField("retweeted_status", retweeted_status_schema)
])

# Replace the "None"s by your own credentials
ACCESS_TOKEN = "781341112166481920-NQn7LMttKjd4hTCWiA9LREJlt2U9cYY"
ACCESS_TOKEN_SECRET = "F99LZjuxw8Gaz88ADWyHNjz7xThx6kcrdCiA7Eo6GE9FZ"
CONSUMER_KEY = "5sFUR20zAOT1tf3AS4VBKxxAT"
CONSUMER_SECRET = "HMxlzm4Xo18JGPIMjljzLQvT22c3B8sDICt8YDgvEYV9YaTI8e"

auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

todayTimestamp = datetime.datetime.now(datetime.timezone.utc)
todayDate = todayTimestamp.strftime("%Y-%m-%d")
tweetLimit = 10000

class Listener(StreamListener):
    def __init__(self):
        super(tweepy.StreamListener, self).__init__()
        self.counter = 0
    def on_data(self, tweet):
        t = json.loads(tweet)
        if self.counter < tweetLimit:
            if "created_at" in t.keys():
                difference = todayTimestamp - datetime.datetime.strptime(t["created_at"], "%a %b %d %H:%M:%S %z %Y")
                if difference.days < 1:
                    df = sparkSession.read.option("timestampFormat", twitter_date_format).json(sparkSession.sparkContext.parallelize([tweet]), tweet_schema).withColumn('user_id',F.col('user.id'))
                    df.write.json("hdfs:///temp_twitter_data/data.json", mode = 'append')
                    self.counter += 1
        else:
            stream.disconnect()
    def on_error(self, status_code):
        print(status_code)
        return False

listener = Listener()
stream = Stream(auth=api.auth, listener=listener)

try:
    print("Start streaming for", tweetLimit, "tweets")
    start = time.time()
    stream.filter(track=["#coronavirus", "#coronavirusoutbreak", "#coronavirusPandemic", "#covid19", "#covid_19", "#covid-19", \
    "#epitwitter", "#ihavecorona"], languages=["en"])
except KeyboardInterrupt:
    print("Streaming stopped.")
finally:
    timeElapsed = time.time() - start
    print("Streaming completed for", tweetLimit, "tweets in", timeElapsed, "seconds")
    stream.disconnect()


tweet_df = sparkSession.read.json('hdfs:///temp_twitter_data/data.json')

user_id_tweet_df = tweet_df.select(F.col("user_id"), F.col("id"), F.col("text"), F.col("created_at"))
# user_id_tweet_df = tweet_df.filter(tweet_df.retweeted_status.isNull()).select(F.col("user_id"), F.col("id"), F.col("text"), F.col("created_at"))

## Sentiment Analysis

def apply_vader_clf(sentence):
    polarity = SentimentIntensityAnalyzer().polarity_scores(sentence)
    if polarity['pos'] > polarity['neg']:
        return 1.0
    return -1.0

predictions = udf(apply_vader_clf)

classified_df = user_id_tweet_df.withColumn("predicted", predictions(user_id_tweet_df['text']))


pos_tweet_count = classified_df.filter(classified_df.predicted == 1.0).count()

neg_tweet_count = classified_df.filter(classified_df.predicted == -1.0).count()

print("Writing sentiment analysis results to DDB")

dynamodb = resource("dynamodb", region_name='us-west-1')
table = dynamodb.Table('CSCE-678-Spark')

sentiment_analysis_result = {
    'key': 'sentiment-analysis',
    'date': todayDate,
    'pos-tweet': pos_tweet_count,
    'neg-tweet': neg_tweet_count
}

response = table.put_item(Item=sentiment_analysis_result)

print("Starting topic modelling")

## Topic Modelling

def removePunctuation(column):
    return trim(lower(regexp_replace(column, '[^\s@a-zA-Z0-9]', ''))).alias("text")

clf_tweet_df_v1 = classified_df.select("id", removePunctuation(col("text")))
clf_tweet_df_final = clf_tweet_df_v1.withColumn("text", regexp_replace('text', '^rt ', ''))


tokenizer = Tokenizer(inputCol="text", outputCol="words")
wordsData = tokenizer.transform(clf_tweet_df_final)

remover = StopWordsRemover(inputCol="words", outputCol="filtered")
wordsDataFrame = remover.transform(wordsData)

#Create a new CountVectorizer model without the stopwords
cv = CountVectorizer(inputCol="filtered", outputCol="features") 
cvmodel = cv.fit(wordsDataFrame)
df_vect = cvmodel.transform(wordsDataFrame)


lda_10 = LDA(k=10, maxIter=10)
lda_model_10 = lda_10.fit(df_vect)
topics_10_df = lda_model_10.describeTopics(maxTermsPerTopic = 10)


#Print the topics in the model
vocabArray = cvmodel.vocabulary

def topic_render(row):
    topic = row.topic
    terms = row.termIndices
    values = row.termWeights
    words = []
    weights = []
    
    c = 0
    for i in terms:
        if vocabArray[i] != '':
            words.append(vocabArray[i])
            c = c + 1;
            if c == 10:
                break
    for j in range(0, 10):
        weights.append(values[j])
    return (topic,words,weights)

count_rdd_filtered = df_vect.rdd.flatMap(lambda a: [(w,1) for w in a.filtered]).reduceByKey(lambda a,b: a+b)
count_DF_filtered = count_rdd_filtered.toDF(['word','frequency'])

topics_final_10 = topics_10_df.rdd.map(topic_render)

topics_10_list = []

for i in topics_final_10.collect():
    topics_10_list.extend(i[1])
    
topics_10_set = set(topics_10_list)

topics_10_freq_df = count_DF_filtered.filter(count_DF_filtered.word.isin(topics_10_set))   
topics_10_freq_dict = topics_10_freq_df.toPandas().set_index('word').T.to_dict('list')

topics_10_dict = {}

for i in topics_final_10.collect():
    topic = i[0]
    words_list = i[1]
    weights_list = i[2]
    word_list_dict = {}
    for j,k in zip(words_list,weights_list):
        f = topics_10_freq_dict.get(j)
        word_list_dict[j] = [f,k]
    topics_10_dict[i[0]] = word_list_dict

print("Writing Topic Modelling results to DDB")

topic_modelling_result = {
    'key': 'topic-modelling',
    'date': todayDate,
    'topics-10': json.dumps(topics_10_dict)
}
response = table.put_item(Item=topic_modelling_result)

print("Starting page ranking")

## Top Users using Page Ranking

edges = tweet_df.select(F.col("user.id").alias("src"), F.explode("entities.user_mentions.id").alias("dst"))
edges = edges.where(col("src").isNotNull() & col("dst").isNotNull())

vertices = edges.select(edges["src"].alias("id")).union(edges.select("dst")).distinct()

graph = GraphFrame(vertices, edges)

vertices = graph.outDegrees
graph = GraphFrame(vertices, edges)

sparkSession.sparkContext.setCheckpointDir("s3://csce-678-twitter-corona-data/")

results = graph.pageRank(resetProbability=0.15, maxIter=5)

# Sort the ranks of users in descending order and extract the top 10.
sorted_ranks = results.vertices.select("id", "outDegree", "pagerank").sort(desc("pagerank"))

top_10_users = sorted_ranks.select(F.col("id").alias("id")).take(10)
user_descriptions = dict()
users = []
for row in top_10_users:
    users.append(row["id"])

user_lookup = api.lookup_users(user_ids=users)
for u in range(0, len(user_lookup)):
    user_descriptions[user_lookup[u].screen_name] = user_lookup[u].description

print("Writing page rank results to DDB")

page_rank_results = {
    'key': 'page-rank-results',
    'date': todayDate,
    'users': json.dumps(user_descriptions)
}
response = table.put_item(Item=page_rank_results)

print("Going ahead and deleting Spark context")

fs = (sparkSession.sparkContext._jvm.org
      .apache.hadoop
      .fs.FileSystem
      .get(sparkSession.sparkContext._jsc.hadoopConfiguration())
      )
path = "hdfs:///temp_twitter_data/"
# use the FileSystem manager to remove the path
fs.delete(sparkSession.sparkContext._jvm.org.apache.hadoop.fs.Path(path), True)




