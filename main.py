import tweepy as tw
import pandas as pd
import os
import mysql.connector
from mysql.connector import Error


consumer_key = 'uHDSs2qfSH181ESHrFv9VaIJV'
consumer_secret_key = 'y0bFMfl0V6CVc8pyAPVaKCP2RBmkiqH59SIwV5TVQpCG6ES1ng'
access_token = '1407348469749235721-4KShsGkIWBZWGptF37H8rAdMYSFXu9'
access_secret_token = 'xsDeoZMJ1yxoZ1UUuG2KwRO6lg789RzywPSiwrl2T867j'

auth = tw.OAuthHandler(consumer_key,consumer_secret_key)
auth.set_access_token(access_token,access_secret_token)
api = tw.API(auth,wait_on_rate_limit=True)

search_words = '#education'
date_since = "2018-11-16"
tweets = tw.Cursor(api.search,q=search_words, lang='en',since=date_since).items(5)


new_search = search_words + ' -filter:retweets '

tweets = tw.Cursor(api.search,q=new_search, lang='en',since=date_since, tweet_mode='extended').items(5)

user_info = [[tweet.user.screen_name, tweet.full_text] for tweet in tweets]

print(user_info)

try:
    connection = mysql.connector.connect(host='database-1.c6k1fqjq7oqw.us-east-2.rds.amazonaws.com',database='user_tweet',user='admin',password='Maulish1306')
    if connection.is_connected():
        print('connection Successfully')
except Error as e:
    print('something went wrong')

mycursor = connection.cursor()


for user in user_info:
    sql = 'insert into user (username,tweet) values (%s,%s)'
    val = (user[0],user[1])
    mycursor.execute(sql,val)
    connection.commit()

print('record inserted')
