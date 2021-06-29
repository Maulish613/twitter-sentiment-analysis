import os
import connection
import clean_tweets
import fetchTweets
import nltk

negative_file = open('negative-words.txt')
positive_file = open('positive-words.txt')

negative_words = []
positive_words = []

for neg,pos in zip(negative_file,positive_file):
    negative_words.append(neg.rstrip("\n"))
    positive_words.append(pos.rstrip("\n"))



def sentiment_analysis(term):
    keyword = []
    username = [] 
    tweets = []
    query = "select * from user where keyword = \""+term+ "\""
    fetch = connection.fetch_function(query)
    
    #print(records)
    for record in fetch:
        keyword.append(record[0])
        username.append(record[1])
        tweets.append(record[2])
    

    for keyword,username,tweet in zip(keyword,username,tweets):
        tokenize = nltk.word_tokenize(tweet)
        #print(tokenize)
        pos_data = set(tokenize)&set(positive_words)
        #print(pos_data)
        pos_data_list = list(pos_data)
        #print(pos_data_list)
        neg_data = set(tokenize)&set(negative_words)
        #print(neg_data)
        neg_data_list = list(neg_data)
        if len(pos_data_list)>len(neg_data_list):
            result = 'positive tweet'
        elif len(pos_data_list)<len(neg_data_list):
            result = "negative tweet"
        else:
            result = "nuteral tweet"
        #print(result)
        sql = "insert into process_data values (%s,%s,%s,%s,%s,%s)"
        val = keyword,username,tweet,result,str(pos_data_list),str(neg_data_list)
        connection.insert_into(sql,val)
        