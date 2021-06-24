from mysql.connector.utils import validate_normalized_unicode_string
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
load_dotenv() 
import os


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

def insert_into(sql,val):
    mycursor.execute(sql,val)
    connection.commit()
    print('successfully inserted')

def fetch_function(query):
    mycursor.execute(query)
    result = mycursor.fetchall()
    return result
    
mycursor = connection.cursor()
dbconnection = connection

