from typing import final
from mysql.connector.utils import validate_normalized_unicode_string
from nltk import data
from nltk import tokenize
from numpy import negative, record, result_type
import tweepy as tw
import pandas as pd
import os
import connection
from tweepy import cursor
import re
import nltk
from nltk.tokenize import word_tokenize
from itertools import chain
#nltk.download()

negative_file = open('negative-words.txt')
positive_file = open('positive-words.txt')

negative_words = []
positive_words = []

for neg,pos in zip(negative_file,positive_file):
    negative_words.append(neg.rstrip("\n"))
    positive_words.append(pos.rstrip("\n"))


def sentiment_analysis():
    username = []
    tweets = []
    fetch = connection.fetch_function('select username,clean_tweet from clean_data')
    
    #print(records)
    for record in fetch:
        username.append(record[0])
        tweets.append(record[1])
    

    for username,tweet in zip(username,tweets):
        tokenize = nltk.word_tokenize(tweet)
        #print(tokenize)
        pos_data = set(tokenize)&set(positive_words)
        #print(pos_data)
        pos_data_list = list(pos_data)
        print(pos_data_list)
        neg_data = set(tokenize)&set(negative_words)
        #print(neg_data)
        neg_data_list = list(neg_data)
        if len(pos_data_list)>len(neg_data_list):
            result = 'positive tweet'
        elif len(pos_data_list)<len(neg_data_list):
            result = "negative tweet"
        else:
            result = "nuteral tweet"
        print(result)
         
#sentiment_analysis()


     





