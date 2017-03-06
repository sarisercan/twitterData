# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 16:11:40 2017

@author: sercansari
"""

import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pymongo import MongoClient
import json

"""
to construct the stream listener 3 tweepy imports 
are used
"""

ckey = '7ftj9PzEF8VALBPALvmVJkvdl'
consumer_secret = 'vz9ezOcUi0IOq0Qy5ymljdxvqJtA51SqlQUlsBfUSlySs3OS0Q'
access_token_key = '834024146652823552-h1cSFoojO5UfwtN0BWKv6YUiwfDPE6n'
access_token_secret = '4X2tXgHLLssJ2dAGo5ixQLwbAdkp7Q8fdhxspRwU2y9GB'

start_time = time.time() #grabs system time
keyword_list = ['the'] #track list 

#Listener Class Override



class listener(StreamListener):

    def __init__(self, start_time, time_limit=60):
    
        self.time = start_time
        self.limit = time_limit
        self.tweet = []
    
    def on_data(self, data):
        
        while (time.time() - self.time) < self.limit:
        
            try:
            
            
                client = MongoClient('localhost', 27017)
                db = client['twitter_db']
                collection = db['twitter_collection']
                tweet = json.loads(data)
                
                collection.save(tweet)
                
                return True
            
            
            except BaseException, e:
                print 'failed ondata,', str(e)
                time.sleep(5)
            pass

        exit()

    def on_error(self, status):
        print('An Error has occured: ' + repr(status))
        return False


auth = OAuthHandler(ckey, consumer_secret) #OAuth object
auth.set_access_token(access_token_key, access_token_secret)


twitterStream = Stream(auth, listener(start_time, time_limit=20)) #initialize Stream object with a time out limit
twitterStream.filter(track=keyword_list, languages=['en'])  #call the filter method to run the Stream Object