import pandas as pd
import sqlite3
import string
import logging



def transform():
	conn = sqlite3.connect("ULPt_db")
	c = conn.cursor()
	c.execute("CREATE TABLE IF NOT EXISTS Tweet_Ready(post_id PRIMARY KEY, post_title)")
	conn.commit()
	df = pd.read_sql_query("select * from ULPT_Posts;", conn)

	logging.basicConfig(level=logging.INFO,filename='logs.log',format='%(asctime)s %(message)s') # include timestamp

	RANGE = int(pd.read_sql_query("select count(*) from ULPT_Posts;", conn).values)
	count = int(pd.read_sql_query("select count(*) from Tweet_Ready;", conn).values)
	print("before:",count)


	TWEET_LENGTH = 280
	for i in range(RANGE):
		try:
			length = int(len(df['post_title'][i]) / 2)
			if 'request' not in df['post_title'][i].lower()[:length]:
				try:
					if  len(str(df['post_title'][i])) < TWEET_LENGTH:

						post_id = str(df['post_id'][i])
						post_title = str(df['post_title'][i])

					if ':' in post_title[:length]:

						ind = int(post_title.find(":"))
						sub = post_title[ind+1:].strip()
						post_title = 'ULPT: ' + sub

					elif '-' in post_title[:length]:

							ind = int(post_title.find("-"))
							sub = post_title[ind+1:].strip()
							post_title = 'ULPT: ' + sub
					
					elif 'ulpt' in post_title.lower():
							ind = int(post_title.find('ulpt'))
							sub = post_title[ind + 5:]
							sub = str(sub.capitalize())
							post_title =  'ULPT:' + sub

							entities = (post_id, post_title)
							c.execute("INSERT INTO Tweet_Ready(post_id, post_title) VALUES(?,?)", entities)
							conn.commit()
				except Exception as e:
						print(e)
						pass
		except Exception as e:
			print(e)
	count2 = int(pd.read_sql_query("select count(*) from Tweet_Ready;", conn).values)
	items = count2 - count
	logging.info(f'{items} unique items were stored in Tweet_Ready.')

if __name__ == "__main__":
	transform()