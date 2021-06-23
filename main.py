import tweepy as tw
import pandas as pd
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from tweepy import cursor
import re
from nltk.tokenize import word_tokenize
load_dotenv()


consumer_key = os.environ.get('consumer_key')
secret_key = os.environ.get('consumer_secret_key')
access_token = os.environ.get('access_token')
access_secret_token = os.environ.get('access_secret_token')
auth = tw.OAuthHandler(consumer_key,secret_key)


auth.set_access_token(access_token,access_secret_token)
api = tw.API(auth,wait_on_rate_limit=True)

search_words = '#education'
date_since = "2018-11-16"
tweets = tw.Cursor(api.search,q=search_words, lang='en',since=date_since).items(5)


new_search = search_words + ' -filter:retweets '

tweets = tw.Cursor(api.search,q=new_search, lang='en',since=date_since, tweet_mode='extended').items(5)

user_info = [[tweet.user.screen_name, tweet.full_text] for tweet in tweets]

host = os.environ.get('host')
database = os.environ.get('database')
user = os.environ.get('user')
password = os.environ.get('password')
try:
    connection = mysql.connector.connect(host=host,database=database,user=user,password=password)
    if connection.is_connected():
        print('connection Successfully')
except Error as e:
    print('something went wrong')

mycursor = connection.cursor()

#for user in user_info:
#    sql = 'insert into user (username,tweet) values (%s,%s)'
#    val = (user[0],user[1])
#    mycursor.execute(sql,val)
#    connection.commit()

#print('record inserted')


query = 'select tweet from user'
mycursor.execute(query)
result = mycursor.fetchall()
clean_tweets = []
for url in result:
    #print(url[0])
    res = re.sub(r"http\S+","",url[0])
    clean_tweets.append(res)
    #print(res)


for user,res in zip(user_info, clean_tweets):
    sql = 'insert into clean_data (username,clean_tweet) values (%s,%s)'
    val = (user[0],res)
    mycursor.execute(sql,val)
    connection.commit()

print('data inserted successfully')



