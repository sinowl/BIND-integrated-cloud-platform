__author__ = 'yoonsu'
# -*- coding: utf-8 -*-

#from src.OpenApi.GoogleOpenApi import *
#from src.OpenApi.naverOpenApi import *

from src.SNS.twitter.twitterstream import *
from src.SNS.twitter.executetwitter import *
from src.Works.crawling.executecrawling import *
from src.Works.hadoop.executeMapReduce import *
from src.Works.hadoop.HDFSIO import *
from src.Works.script.ExecuteScript import *
from src.RPC.master.master import *
from src.RPC.slave.slave import *
from src.conf.config import Config
from src.killJob import *

import os
import numpy as np

class Worker:
    def __init__(self):
        self._access_token = "48323366-Sy64FYgTvPyESe9NvvkNFFEi52ohYMsO0Bf6p2K40"
        self._access_token_secret = "9VheG44zWMg2eSJWksdaOP2LPDXfZ1OJW2HRptvxK7ol8"
        self._consumer_key = "AeixynBTaBA6OGe6y4kgA3byE"
        self._consumer_secret = "wb7bS37hSsRmpvle3kI6xtBlwidZbl6JIjHsFOkOQZ8mvD776D"
        self.startHDFSIO()
        self.startmapreduce()
        self.runCrawling()
        self.runScript()
        self._masterIP = ''
        self._slaveIP = []
        self.master = Master()
	#self.slave = Slave()
  	self.config = Config()
        self.__platform = self.config.getPlatform()

    def isMaster(self):
        isMS = self.master.ismaster()
        t_masterip = threading.Thread(target=self.master.returnMasterip, args=())
        t_masterip.start()
        if isMS == True:
            t_runmaster = threading.Thread(target=self.master.runMaster, args=())
            t_runmaster.start()
            self._slaveIP = self.master.returnSlaveip()
	    #for i in range(0, len(self._slaveIP)):
		#self.master.rpcRunSlaves(self._slaveIP[i])
            self.__slaveInfo = self.master.getSlaveInfo()
        else:
            pass
        t_masterip.join()
        t_runmaster.join()

    def returnMaster(self):
        return self.master

    def runSlave(self):
	self.slave.runSlave()

    def setSlave(self, slave):
	self.slave = slave

    ################################## Wait for run Work ####################################################
    ################################## RPC (function of start Master, Slave Node ) ##########################

    def startMasterWork(self, *args):
        t_work = threading.Thread(target=self.executework, args=())
        t_work.start()
        t_work.join()

    def findworkload(self, array, work):
        for i in range(0, len(array)):
            if array[i].split(":")[0].find(work) != -1:
                return i
        return -1

    def isFileinHDFS(self, source, filename):
    	for numS in range(0,len(source)):
		self.onesource = source[numS].strip()
		cmd = "hadoop fs -ls /user/yoonsu/"+self.onesource+" | grep "+filename
	        checkcmd = os.system( cmd )
		print cmd
		while True:
        	        if checkcmd == 0:
        	            break
                	else:
        	            pass

    def executeONEwork(self, args):

#	if self.config.getHostname() != "master":
#		self.runSlave()
#		self.slave.sendCmdtoMaster( "start Slave at " + self.config.getHostname() )
        #tmp = args[0][1].split('-')     # input1 // { }
	#print args[0][1]
	tmp = args.split('---')


#	parallelNum = tmp[1].split('&')[1].strip()
#	tmp[1] = tmp[1].split('&')[0].strip()
#        tmp_value = tmp[1].split('//')   # term // work
	parallelNum = 1
	if args.find('split:') != -1:
		parallelNum = tmp[1].split('split:')[1][0]
		tmp[1] = tmp[1].split('//split:')[0].strip()
		tmp_value = tmp[1].split('//')
	else:
		parallelNum = 1
		tmp_value = tmp[1].split('//')
	self.slave.sendCmdtoMaster( "parallelNum : "+str(parallelNum) )
        tmp_value_stage = []
        for j in range(0, len(tmp_value)):
            temp = tmp_value[j].translate(None, "{}\'")
            tmp_value_stage += [temp]
        self.current_stage = tmp[0].strip()
        self.current_work = tmp_value_stage[self.findworkload(tmp_value_stage, "work")].split(":")[1].strip()
	#self.slave.sendCmdtoMaster( "start work of " + self.current_work )

	########################################################################################################################################
        ### Source (input) #####################################################################################################################
	########################################################################################################################################
        if self.current_work == "crawling":
            print '\033[92m'+"[ "+str(time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"Start Crawling"+'\033[0m'
	    print '\033[92m'+"[ "+str(time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"End Crawling"+'\033[0m'

	    self.slave.sendCmdtoMaster("end-crawling")

        if self.current_work == "twitter":
	    self.slave.sendCmdtoMaster( '\033[92m'+"[ "+str(time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"Start Twitter"+'\033[0m' )
            #print '\033[92m'+"[ "+str(time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"Start Twitter"+'\033[0m'
	    self.term = tmp_value_stage[self.findworkload(tmp_value_stage,"term")].split(":")[1].strip()
            self.streamtime = int(tmp_value_stage[self.findworkload(tmp_value_stage,"time")].split(":")[1].strip())
	    
	    t_twitter = threading.Thread(target=self.runTwitter, args=(self._consumer_key, self._consumer_secret, self._access_token, self._access_token_secret, self.term, self.streamtime, ))
            t_twitter.start()
            t_twitter.join()
            while not self.checkData(self.__platform+"data/twitter"):
                pass

            self.writeHDFS(self.__platform+"data/twitter", "raw_tweets.json", tmp[0].strip(), "raw_tweets-"+tmp[0].strip()+".json")

	    #print '\033[92m'+"[ "+str(time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"End crawling Twitter"+'\033[0m' 
	    #print "end-twitter"

	    self.slave.sendCmdtoMaster( '\033[92m'+"[ "+str(time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"End crawling Twitter"+'\033[0m' )
	    self.slave.sendCmdtoMaster("end-twitter")
	
	########################################################################################################################################
        ### Processing (output) ################################################################################################################
       	########################################################################################################################################
	if self.current_work == "mapreduce":
	    #print '\033[92m'+"[ "+str( time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"Start MapReduce"+'\033[0m'
            self.slave.sendCmdtoMaster( '\033[92m'+"[ "+str( time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"Start MapReduce"+'\033[0m' )
            self.mapfile = tmp_value_stage[self.findworkload(tmp_value_stage, "map")].split(":")[1].strip()
            self.reducefile = tmp_value_stage[self.findworkload(tmp_value_stage, "reduce")].split(":")[1].strip()
            self.source = tmp_value_stage[self.findworkload(tmp_value_stage, "source")].split(":")[1].strip().split(",")

            self.isFileinHDFS(self.source, "raw_tweets")

            self.runsingleMapReduce("/user/yoonsu/"+self.onesource+"/*", "/user/yoonsu/"+self.current_stage, self.mapfile, self.reducefile)
	    
	    os.system("rm "+self.__platform+"src/"+self.mapfile+" "+self.__platform+"src/"+self.reducefile)

	    #print '\033[92m'+"[ "+str( time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"End MapReduce"+'\033[0m'
	    #print "end-mapreduce"
	    #removeHDFSDir(self.onesource)
            self.slave.sendCmdtoMaster( '\033[92m'+"[ "+str( time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"End MapReduce"+'\033[0m' )
	    self.slave.sendCmdtoMaster("end-mapreduce")

        if self.current_work == "script":
	    #print '\033[92m'+"[ "+str( time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"Start Script"+'\033[0m' 

            self.slave.sendCmdtoMaster( '\033[92m'+"[ "+str( time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"Start Script"+'\033[0m' )
            self.scriptfile = tmp_value_stage[self.findworkload(tmp_value_stage, "file")].split(":")[1].strip()
            self.source = tmp_value_stage[self.findworkload(tmp_value_stage, "source")].split(":")[1].strip().split(",")

            self.isFileinHDFS(self.source, "part")

            self.copyToLocal("/user/yoonsu/"+self.onesource, self.__platform+"data/script", "part-00000")
            #self.executePythonScript(self.scriptfile, self.__platform+"data/script/part-00000")
	    mod = __import__( "src."+self.scriptfile.split('.')[0] )
	    #getattr( mod, self.scriptfile.split('.')[0] )
	    r1 = getattr( mod, self.scriptfile.split('.')[0] ).results( self.__platform+"data/script/part-00000" ) 
	    r2 = getattr( mod, self.scriptfile.split('.')[0] ).results( self.__platform+"data/script/part-00000" ) 
 	    lengen = len(list(r1))
	    for m in range(0, lengen):
		self.slave.sendCmdtoMaster( ' script abcdefghijk321654 '+next(r2)+' abcdefghijk321654 '+str(m)+' abcdefghijk321654' )
	    self.slave.sendCmdtoMaster( ' script-end abcdefghijk321654 '+str(lengen-1)+' abcdefghijk321654' )
	    os.system("rm "+self.__platform+"data/script/*")
	    os.system("rm "+self.__platform+"src/"+self.scriptfile)
	    #removeHDFSDir(self.onesource)
	    #print '\033[92m'+"[ "+str(time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"End Script"+'\033[0m' 
	    #print 'end-script'

	    self.slave.sendCmdtoMaster( '\033[92m'+"[ "+str(time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"End Script"+'\033[0m' )
	    self.slave.sendCmdtoMaster("end-script")

	if self.current_work == "ML":
	    self.slave.sendCmdtoMaster( '\033[92m'+"[ "+str( time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"Start machine learning"+'\033[0m' )
	    #self.slave.sendCmdtoMaster( str( tmp_value_stage) )
            self.mlfile = tmp_value_stage[self.findworkload(tmp_value_stage, "file")].split(":")[1].strip()
	    self.mlreffile = tmp_value_stage[self.findworkload(tmp_value_stage, "reffile")].split(":")[1].strip()
            self.source = tmp_value_stage[self.findworkload(tmp_value_stage, "source")].split(":")[1].strip().split(",")
            self.cmd = tmp_value_stage[self.findworkload(tmp_value_stage, "cmd")].split(":")[1].strip()
	    self.isFileinHDFS(self.source, "mr")
            self.copyToLocal("/user/yoonsu/"+self.onesource, self.__platform+"src", "mr.p")
	    #self.slave.sendCmdtoMaster( self.cmd )
	    os.chdir( self.__platform+"src/" )
	    self.slave.sendCmdtoMaster( os.getcwd() )
	    tmpper = np.random.permutation(173) 
	    strper = tmpper.tolist().__str__().strip(']').strip('[')
	    self.slave.sendCmdtoMaster( self.cmd+' '+str(parallelNum)+' '+'\"'+strper+'\"' )
	    t_cnn = threading.Thread(target=self.runCNN, args=(self.cmd+' '+str(parallelNum)+' '+'\"'+strper+'\"', ))
            t_cnn.start()
            t_cnn.join()
	    #os.system( self.cmd+' '+str(parallelNum) )
 	    os.system("rm "+self.__platform+"src/mr.p")
	    os.system("rm "+self.__platform+"src/"+self.mlfile)
	    os.system("rm "+self.__platform+"src/"+self.mlreffile)	
	    self.slave.sendCmdtoMaster( '\033[92m'+"[ "+str(time.strftime("%Y-%m-%d %H-%M-%S", time.gmtime(time.time()) ))+" ] "+"End machine learning"+'\033[0m' )
	    self.slave.sendCmdtoMaster("end-script")


    def runMasterRPC(self):
        m = Master()
        #threading._start_new_thread( m.runMaster, () )
        t_runMaster = threading.Thread(target=m.runMaster, args=())
        t_runMaster.start()
        t_runMaster.join()

    def runSlaveRPC(self):
        s = Slave()
        s.runSlave()

    ################################## About SNS ###########################################################
    ################################## Twitter   ###########################################################

    def runTwitter(self, consumer_key, consumer_secret, access_token, access_secret, term, streamtime):
        self.startTwitter(consumer_key, consumer_secret, access_token, access_secret, streamtime)
        self.runTwitterStream(term)

    def startTwitter(self, consumer_key, consumer_secret, access_token, access_secret, streamtime):
        self.__tweet = Twitter(consumer_key,consumer_secret,access_token,access_secret)
        self.__tweetstream = TwitterStream( consumer_key, consumer_secret, access_token, access_secret, streamtime)

#    def authTwitter(self,consumer_key, consumer_secret, access_token, access_token_secret):
#        self.__tweetstream.authTwitter(consumer_key, consumer_secret, access_token, access_token_secret)
#        self.__tweet.authTwitter(consumer_key, consumer_secret, access_token, access_token_secret)

    def runTweet(self,terms,filename):
        self.__tweet.printhometimeline(filename)
        self.__tweet.getsearch(terms,filename)

    def runTwitterStream(self,terms):
        self.__tweetstream.getfilterstream(terms)

    ################################## About Crawling ######################################################

    def runCrawling(self):
        self.__crawl = ExecuteCrawling()

    def getCrawling(self,searchurl, makedir):
        self.__crawl.getcrawling(searchurl, makedir)

    ################################## About Script ######################################################

    def runScript(self):
        self.__script = ExecuteScript()

    def executePythonScript(self, filename, args):
        self.__script.executepython(filename, args)

    ################################## About ML (Machine Learning) #######################################

    def runCNN(self, cmd):
	os.system( cmd )

    ################################## About Hadoop (MapReduce, HDFS) #######################################

    def startmapreduce(self):
        self.__mapreduce = executemapreduce()

    def runMapReduce(self, inputfile, outputfile, mapfile, reducefile, numofreducer):
        self.__mapreduce.executemapreduce(inputfile, outputfile, mapfile, reducefile, numofreducer)

    def runsingleMapReduce(self, inputfile, outputfile, mapfile, reducefile):
        self.__mapreduce.executesinglemapreduce(inputfile, outputfile, mapfile, reducefile)

    def checkMapReduceJob(self):
	return self.__mapreduce.checkMapReduceJob()

    def startHDFSIO(self):
        self.__hdfsio = Hdfsio()

    def copyToLocal(self,frompath, topath, filename):
        self.__hdfsio.copyData(frompath,topath,filename)

    def writeHDFS(self,frompath,topath,filename):
        self.__hdfsio.writeData(frompath,topath,filename)

    def writeHDFS(self,frompath, filename1,topath,filename2):
        self.__hdfsio.writeData(frompath, filename1, topath, filename2)

    def deleteHdfsData(self, path):
	self.__hdfsio.deleteHdfsDir(path)

    def displayHDFS(self, path):
        self.__hdfsio.displaydata(path)

    def checkData(self,path):
        returnvalue = self.__hdfsio.isDataInlocal(path)
        return returnvalue

if __name__ == '__main__':
    worker = Worker()
    worker.executeONEwork(sys.argv)
