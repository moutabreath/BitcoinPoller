import sched, time
import json
from urllib.request import urlopen
import tweepy # using version 4.1.0
import configparser
import sqlite3
from sqlite3 import Error
from datetime import timedelta, timezone , datetime
import os


s = sched.scheduler(time.time, time.sleep)

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def init_db():
    

    # Get the current working directory
    cwd = os.getcwd()

    database = cwd + r"\pythonsqlite.db"

    sql_create_scores_table = """ CREATE TABLE IF NOT EXISTS projects (
                                        id integer PRIMARY KEY,
                                        twwet_datetime text NOT NULL,
                                        bitcoin_price real,
                                        num_of_twwets int,
                                        batch_sentiment_score int
                                    ); """                                    

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create projects table
        create_table(conn, sql_create_scores_table)
        return conn;
        
    else:
        print("Error! cannot create the database connection.")

def save_score(score):
    conn = init_db();
    with conn:
        sql = ''' INSERT INTO projects(twwet_datetime,bitcoin_price,num_of_tweets, batch_sentiment_score)
                    VALUES(?,?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, score)
        conn.commit()
        return cur.lastrowid

def get_jsonparsed_data(url):

    response = urlopen(url)
    data = response.read().decode("utf-8")
    return json.loads(data)

def get_bitcoin_price():
    url = "https://financialmodelingprep.com/api/v3/quote/BTCUSD?apikey=9c33655ac70d040280297ef04cf3ceff"
    parsed_data = get_jsonparsed_data(url)[0]
    price = parsed_data["price"]
    return price;

def query_twitter(tweepy):
    twitter_api = tweepy.API(auth)

    query_string = "$BTC bitcoin" + " -filter:retweets"
    tweets = twitter_api.search_tweets(q=query_string, result_type="recent", count=100, tweet_mode='extended')
    total_score = 0;
    for tweet in tweets:
        now =  datetime.now(timezone.utc)
        if now - tweet.created_at <=  timedelta(minutes=1):
            sentiment_score = tweet.favorite_count + 2 * tweet.retweet_count
            total_score += sentiment_score;
    return tweets.count, total_score


def fetch_and_persist_data(tweepy):
    price = get_bitcoin_price()   
    
    count, total_score = query_twitter(tweepy)
   
    score = (datetime.utcnow(), price, count, total_score)
    save_score(score)


def task_loop(s, now, tweepy):    
    fetch_and_persist_data(tweepy)
    passed = (datetime.now() - now)
    delta = timedelta(hours=2)
    if passed < delta:
        s.enter(2, 1, task_loop, (s, now, tweepy))

if __name__ == "__main__":
    print("Welcome to crypto twitter sentiment tracker")
    config = configparser.ConfigParser()
    config.sections()
    config.read('config.ini')
    # Twitter OAuth Authentication
    consumer_key = config['DEFAULT']['consumer_key']
    consumer_secret = config['DEFAULT']['consumer_secret']

    access_token = config['DEFAULT']['access_token']
    access_token_secret = config['DEFAULT']['access_token_secret']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)


    now = datetime.now()
    s = sched.scheduler(time.time, time.sleep)
    s.enter(2, 1, task_loop, (s, now, tweepy))
    s.run()
   
    # What other info can I save so  I could use later?
    #1. Tweet author - if a tweet author is someone known or famous, it might alter these statistics
    #2. Twwet Geo - maybe bitcoin is more popular on certain countries more than others?