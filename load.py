import sqlite3
import string
import tweepy
import logging
import config
from datetime import datetime
import pandas as pd
import random


logging.basicConfig(level=logging.INFO,filename='logs.log',format='%(asctime)s %(message)s') # include timestamp

def load():
    conn = sqlite3.connect('ULPT_db')
    c = conn.cursor()

    c.execute("CREATE TABLE IF NOT EXISTS Tweets_Sent(post_id PRIMARY KEY, post_title, date_sent)")
    conn.commit()


    auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_token_secret)
    api = tweepy.API(auth)


    c.execute('select * from Tweet_Ready')
    posts = c.fetchall()

    count = int(pd.read_sql_query("select count(*) from Tweets_Sent;", conn).values)

    x = pd.read_sql_query("select post_id from Tweets_Sent;", conn).values
    random.shuffle(posts)
    for t in posts:

        if t[0] not in x:
            tweet = t[1]
            date_sent = datetime.now()
            post_id = t[0]

            entities = (post_id, tweet,date_sent)
            try:
                api.update_status(tweet)
                c.execute('insert into Tweets_Sent (post_id,post_title, date_sent) VALUES(?,?,?)',entities)
                conn.commit()
            except Exception as e:
                print(e)
        
        count2 = int(pd.read_sql_query("select count(*) from Tweets_Sent;", conn).values)
        items = count2 - count
        logging.info(f'{items} unique items were added to Tweets_Sent.')
        conn.commit()
        
        break

if __name__ == "__main__":
    load()