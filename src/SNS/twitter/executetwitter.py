__author__ = 'yoonsu'

import twitter

class Twitter:
    def __init__(self,consumer_key, consumer_secret, access_token, access_token_secret):
        #self.__api = twitter.Api(consumer_key='AeixynBTaBA6OGe6y4kgA3byE',
        #                       consumer_secret='wb7bS37hSsRmpvle3kI6xtBlwidZbl6JIjHsFOkOQZ8mvD776D',
        #                       access_token_key='48323366-Sy64FYgTvPyESe9NvvkNFFEi52ohYMsO0Bf6p2K40',
        #                       access_token_secret='9VheG44zWMg2eSJWksdaOP2LPDXfZ1OJW2HRptvxK7ol8'
        #                    )
        self.__api = twitter.Api(consumer_key=consumer_key,
                                 consumer_secret=consumer_secret,
                                 access_token_key=access_token,
                                 access_token_secret=access_token_secret
        )
        self.texttimeline = self.__api.GetHomeTimeline(count=200)
        self.textlist = [s.text for s in self.texttimeline]

#    def authTwitter(self, consumer_key, consumer_secret, access_token, access_token_secret):
#        self.__api = twitter.Api(consumer_key=consumer_key,
#                                 consumer_secret=consumer_secret,
#                                 access_token_key=access_token,
#                                 access_token_secret=access_token_secret
#        )

    def printhometimeline(self, filename):
        f1 = open(filename, 'w+')
        for i in range(0, len(self.textlist)):
            print >> f1, self.textlist[i].encode('utf8')
        f1.close()

    def getsearch(self, terms, filename):
        print terms, filename
        f1 = open(filename, 'a+')
        resultlist = self.__api.GetSearch(term=terms, count=200, lang='en')
        result = []
        for s in resultlist:
            result.append(s.text)
        for i in range(0, len(result)):
            f1.write(result[i].encode('utf8'))
            #print >> f1, result[i].encode('utf8')
        f1.close()
        #print result

