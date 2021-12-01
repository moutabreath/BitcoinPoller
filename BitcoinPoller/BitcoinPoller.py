

import json
from urllib.request import urlopen
import tweepy # using version 4.1.0
import configparser
import sqlite3
from sqlite3 import Error
from datetime import datetime
import os

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
        sql = ''' INSERT INTO projects(twwet_datetime,bitcoin_price,num_of_twwets, batch_sentiment_score)
                    VALUES(?,?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, score)
        conn.commit()
        return cur.lastrowid

def get_jsonparsed_data(url):

    response = urlopen(url)
    data = response.read().decode("utf-8")
    return json.loads(data)


if __name__ == "__main__":
    print("Welcome to crypto twitter sentiment tracker")
    url = "https://financialmodelingprep.com/api/v3/quote/BTCUSD?apikey=9c33655ac70d040280297ef04cf3ceff"
    parsed_data = get_jsonparsed_data(url)[0]
    price = parsed_data["price"]
    print("bitcoin price: " + str(price))


    config = configparser.ConfigParser()
    config.sections()
    config.read('config.ini')
    # Twitter OAuth Authentication
    consumer_key = config['DEFAULT']['consumer_key']
    consumer_secret = config['DEFAULT']['consumer_secret']

    access_token = config['DEFAULT']['access_token']
    access_token_secret = config['DEFAULT']['access_token_secret']

    # Configure tweepy
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twitter_api = tweepy.API(auth)

    query_string = "$BTC" + " -filter:retweets"
    tweets = twitter_api.search_tweets(q=query_string, result_type="recent", count=100, tweet_mode='extended')
    total_score = 0;
    for tweet in tweets:
        sentiment_score = tweet.favorite_count + 2 * tweet.retweet_count
        total_score += sentiment_score;
    score = (datetime.utcnow(), price, tweets.count, total_score);
    save_score(score);
    print(tweets[0])