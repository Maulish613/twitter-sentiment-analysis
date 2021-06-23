import tweepy as tw
import pandas as pd
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
load_dotenv()

consumer_key = os.environ.get('consumer_key')
secret_key = os.environ.get('consumer_secret_key')
access_token = os.environ.get('access_token')
access_secret_token = os.environ.get('access_secret_token')
auth = tw.OAuthHandler(consumer_key,secret_key)


auth.set_access_token(access_token,access_secret_token)
api = tw.API(auth,wait_on_rate_limit=True)