import connection
import re

def clean_data(term):
    query = "select * from user where keyword = \""+term+ "\""
    print(query)
    fetch_data = connection.fetch_function(query)
    for url in fetch_data:
        
        res = re.sub(r"http\S+","",url[2])  #remove https: and http
        res = re.sub(r"#\S+", " ", res)   #remove '#'
        res = re.sub(r"@\S+", " ", res) #remove mentions
        res = re.sub(r'\w*\d+\w*', '', res) #remove numbers
        res = re.sub(r"'[%s]' % re.escape(string.punctuation)",' ', res)  # Removes punctuations"
        res.strip()
        res = res.lower()
        connection.insert_into(sql = 'insert into clean_data (keyword,username,clean_tweet) values (%s,%s,%s)',val = (url[0],url[1],res))
    return url[0],url[1],res

clean_data('google')