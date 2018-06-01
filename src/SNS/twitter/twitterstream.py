__author__ = 'yoonsu'
# -*- coding: utf-8 -*-

#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from src.conf.config import Config
import time
import sys
import os

#Variables that contains the user credentials to access Twitter API
access_token = "48323366-Sy64FYgTvPyESe9NvvkNFFEi52ohYMsO0Bf6p2K40"
access_token_secret = "9VheG44zWMg2eSJWksdaOP2LPDXfZ1OJW2HRptvxK7ol8"
consumer_key = "AeixynBTaBA6OGe6y4kgA3byE"
consumer_secret = "wb7bS37hSsRmpvle3kI6xtBlwidZbl6JIjHsFOkOQZ8mvD776D"


#This is a basic listener that just prints received tweets to stdout.
class _StdOutListener(StreamListener):
    def __init__(self):
        self.start_time = time.time()
        self.tweet_data=[]

    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print status

class _StdOutToFile(StreamListener):
    def __init__(self, streamtime):
        self.start_time = time.time()
        self.tweet_data=[]
        conf = Config()
        self.__platform = conf.getPlatform()
        self.__home = conf.getHome()
	self.__streamtime = streamtime
        pass

    def on_data(self, data):
        #saveFile = io.open('raw_tweets.json', 'a', encoding='utf-8')
        saveFile = open('raw_tweets.json', 'a')
        if ( time.time() - self.start_time < self.__streamtime ):

            try:
                self.tweet_data.append(data)
                #print data
                return True


            except BaseException, e:
                print 'failed ondata,', str(e)
                time.sleep(5)
                pass
        else:
            #saveFile = io.open('raw_tweets.json', 'w', encoding='utf-8')
            #saveFile.write(u'[\n')
            for i in range(0, len(self.tweet_data)):
                saveFile.write(self.tweet_data[i]+'\n\n')
            #saveFile.write(u'\n]')
            saveFile.close()
            print '\033[92m'+"[ "+str( time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"Stop storing Twitter Stream"+'\033[0m'
            #os.system("mv /home/ubuntu/raw_tweets.json /home/ubuntu/integrated-cloud-platform/data/twitter/")
            mvcmd = "mv "+self.__home+"raw_tweets.json "+self.__platform+"data/twitter"
            os.system(mvcmd)
            sys.exit()


    def on_error(self, status):
        print status

class TwitterStream():
    def __init__(self,consumer_key, consumer_secret, access_token, access_token_secret, streamtime):
        self.__l = _StdOutListener()
        self.__l2 = _StdOutToFile(streamtime)
        self.__auth = OAuthHandler( consumer_key, consumer_secret)
        self.__auth.set_access_token( access_token, access_token_secret)

#    def authTwitter(self, consumer_key, consumer_secret, access_token, access_token_secret):
#        self.__auth = OAuthHandler( consumer_key, consumer_secret)
#        self.__auth.set_access_token( access_token, access_token_secret)

    def printfilterstream(self, terms):
        self.stream = Stream(self.__auth, self.__l)
        self.stream.filter(track=terms)

    def getfilterstream(self, terms):
        self.stream = Stream(self.__auth, self.__l2)
        self.stream.filter(track=terms)

if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = _StdOutListener()
    l2 = _StdOutToFile(5)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l2)
    

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['korea'])
