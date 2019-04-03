import config
import praw
import string
from datetime import datetime
import sqlite3
from time import time
import threading
import logging


print("Logging in...")
reddit = praw.Reddit(user_agent = 'dontmindme', client_id = config.client_id, client_secret = config.client_secret, 
username = config.username, password = config.password)

top = reddit.subreddit("UnethicalLifeProTips").top(limit=10)
hot = reddit.subreddit("UnethicalLifeProTips").hot(limit=10)
#new = reddit.subreddit("UnethicalLifeProTips").new(limit=999)


print ("Logged in as:",config.username)

logging.basicConfig(level=logging.INFO,filename='logs.log',format='%(asctime)s %(message)s') # include timestamp



ts = time()

conn = sqlite3.connect('ULPT_db')
c = conn.cursor()
c.execute("select count(*) from ULPT_posts")
cb = c.fetchall()


def extract_posts(sorting):

    count = 0

    for s in sorting:

        if sorting == top:
            sort = 'top'
        if sorting == hot:
            sort = 'hot'
        #if sorting == new:
           # sort = 'new'
        
        post_id = str(s)
        post_title = s.title.strip()
        date = str(datetime.utcfromtimestamp(s.created_utc))
        author = str(s.author)
        score = float(s.score)
        upvote_ratio = (s.upvote_ratio)
        date_added = datetime.now()

        entities = (post_id, post_title, date, author, score, upvote_ratio, sort, date_added)
        

        conn = sqlite3.connect('ULPT_db')
        c = conn.cursor()

        c.execute("CREATE TABLE IF NOT EXISTS ULPT_Posts(post_id PRIMARY KEY, post_title, post_date, author, score, upvote_ratio, sort, date_added)")
        conn.commit()

        try:
            c.execute("INSERT INTO ULPT_Posts(post_id, post_title, post_date, author, score, upvote_ratio, sort, date_added) VALUES(?,?,?,?,?,?,?,?)", entities)
            conn.commit()
        except Exception as e:
            print(e)
            pass

        count += 1
        print(count)
        conn.close()

    
threads_list = []
t1 = threading.Thread(target = extract_posts, name ='top', args=(top,))
threads_list.append(t1)
t1.start()


t2 = threading.Thread(target = extract_posts, name ='hot', args=(hot,))
threads_list.append(t2)
t2.start()

#t3 = threading.Thread(target = extract_posts, name ='new', args=(new,))
#threads_list.append(t3)
#t3.start()

for t in threads_list:
    t.join()
tf = time() - ts

c.execute("select count(*) from ULPT_posts")
ca = c.fetchall()
conn.close()
cb = int(''.join(str(cb).replace(",","").replace("[","").replace("]","").replace("(","").replace(")","")))
ca = int(''.join(str(ca).replace(",","").replace("[","").replace("]","").replace("(","").replace(")","")))

items = ca-cb
logging.info(f'Time it took to extract posts:{tf} seconds.')
logging.info(f'{items} unique posts were stored in ULPT_db.')

