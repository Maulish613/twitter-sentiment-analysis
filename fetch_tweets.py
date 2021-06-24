import os
import tweepy as tw
import main
import connection
from dotenv import load_dotenv

load_dotenv()

def fetch_tweets(term,count):

    consumer_key = os.environ.get('consumer_key')
    secret_key = os.environ.get('consumer_secret_key')
    access_token = os.environ.get('access_token')
    access_secret_token = os.environ.get('access_secret_token')
    auth = tw.OAuthHandler(consumer_key,secret_key)


    auth.set_access_token(access_token,access_secret_token)
    api = tw.API(auth,wait_on_rate_limit=True)


    search_words = term
    new_search = search_words + ' -filter:retweets '

    tweets = tw.Cursor(api.search,q=new_search, lang='en',result_type='recent', tweet_mode='extended').items(count)
   
    user_info = [[tweet.user.screen_name, tweet.full_text] for tweet in tweets]
    for i in user_info:
        userName = i[0]
        text = i[1]
        query = 'insert into user values (%s, %s,%s)' 
        val = (term,userName,text)
        connection.insert_into(query,val)
        # print((val))
        


fetch_tweets('google',5)