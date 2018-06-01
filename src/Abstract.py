__author__ = 'yoonsu'
# -*- coding: utf-8 -*-

"""
    multiple-stage :
        (stage1).(stage2).(stage3). ...

    each stage :
        한 작업을 여러 node에서 병렬적으로 실행가능
        여러 work 실행 가능 (OpenApi, SNS, Crawling, MapRedcue, Stript etc)

    metadata :
        작업 flow를 HDFS의 디렉토리로 표현
        ex) Crawling1 (input1) -> MapReduce1 (output1) -> Script1 (output2) -> OpenAPI1
            Twitter1 (input2) -> MapReduce2 (output3) -> Script1 (output2) -> OpenAPI1
        HDFS
            ./input1 :
            ./input1/output1 :
            ./input2 :
            ./input2/output3 :
            ./input1/output1/output2 (link to ./input2/output3/output2) :

    DOM:
        ./input1{'work':'file','path':'/home/user/file.txt'}/output1{}/output2{}/output6{}
        ./input2{}/output3{}/output2{}/output7{}
        ./input3/output2
        ./input4{'work':'twitter',}
            /output4{'work':'mapreduce','map':'file1','reduce':'file2'}
                /output5{'work':'script','file':'file3'}
                    /output6{'work':'linuxcmd','cmd':'grep hope'}
                        /output7{'work':'openapi','auth':'key'...}
        각 stage의 input, output은 stdin, stdout 형식을 따른다.

        method
        ./output1.show()
        ./input1.split(5)
"""

from src.executeWorker import *
from src.conf.config import Config
from src.killJob import *
import re
import math
#from src.Works.viz import *

class Node:
	def __init__(self, name):
		self.name = name
		self.__work = ''
		self.__source = ''
		self.__nodeWorkName = name + ' --- {'
		self.__splitnum = 0
		self.__parallelNum = 0

	def work(self, workName):
		self.__work = workName
		self.__nodeWorkName += 'work:'+workName+'//' 
		return self

	def term(self, term):
		self.__nodeWorkName += 'term:' + term +'//'
		if self.__work == 'twitter':
			return self		
		if self.__work == 'mapreduce':
			return self
		if self.__work == 'script':
			return self

	def cmd(self, cmd):
		self.__nodeWorkName += 'cmd:' + cmd + '//'
		return self

	def time(self, streamtime):
		self.__streamtime = streamtime
		self.__nodeWorkName += 'time:'+ str(self.__streamtime)+'//'
		return self

	def splitNum(self, num):
		self.__nodeWorkName += 'split:'+ str(num)+'//'
		self.__splitnum = num
		self.__streamtime = self.__streamtime / num
		#c = re.compile(r"time:\w*,")
		c = re.compile(r"time:\w*//")
		print "nodeWorkName : "+ str(self.__nodeWorkName)
		self.__nodeWorkName = self.__nodeWorkName.replace( c.findall(self.__nodeWorkName)[0], "time:"+str(self.__streamtime)+"//")
		#print self.__nodeWorkName
		return self
	
	def parallelNum(self, num):
		self.__parallelNum = num
		self.__nodeWorkName += 'parallel:' + str(num) + '//'
		return self

	def getsplitNum(self):
		return self.__splitnum

	def getparallelNum(self):
		return self.__parallelNum

	def map(self, mapfile):
		if self.__work == 'mapreduce':
			self.__nodeWorkName += 'map:'+mapfile+'//'
			return self
		else:
			print "Error for using unavailavle function (map)"

	# end function of mapreduce 
	def reduce(self, reducefile):
		if self.__work == 'mapreduce':
			self.__nodeWorkName += 'reduce:'+reducefile+'//'
			return self
		else:
			print "Error for using unavailavle function (reduce)"

	# end function of script
	def file(self, filename):
		#if self.__work == 'script':
		self.__nodeWorkName += 'file:'+filename+'//'
		return self
		#else:
		#	print "Error for using unavailavle function (file)"

	def reffile(self, reffile):
		self.__nodeWorkName += 'reffile:'+reffile+'//'
		return self

	def source(self, sourceName):
		self.__source = sourceName
		self.__nodeWorkName += 'source:'+sourceName + '//'
		return self
		
	def getnodeContents(self):
		#print "complete node id is %s" % id(self)
		return "%s" % self.__nodeWorkName + '}'

class Abstract:

    def __init__(self):
        self.w = Worker()
        self.conf = Config()
        self.__total_args = []
        self.__total_args2 = []
        self.__total_phase = []
        self.__execute_node = []
        self.__slaveInfo = {}
	self.__slaveInfoInmaster = {}
        self.__platform = ''
        self.__slaveip = []
	self.__node = []

    def pyquery2(self, nodeName):
	node = Node( nodeName )	
	self.__node += [node]
	return node

    def getNodes(self):
	return self.__node

    def showNode(self):
	for node in self.__node:
		print "%s" % node.getnodeContents()

    def pyquery(self, **kwargs):
        args_string=[]
        #self.w.startMasterWork(kwargs)
        #print kwargs
        #self.w.executework(kwargs)
        #for name, value in kwargs.items():
        for key, value in sorted(kwargs.items()):
            print "%s : %s" % (key, value)
            args_string += [key]
            self.__total_args += [key + " --- " + str(value)]
            self.__total_args2 += [key + " --- " + str(value)]
        self.__total_args2 += ["oneDAG"]
        #self.w.executework((lambda x: x)(args_string))

    def displaytopology(self):
        buf_args = []
        buf_string = ''
	for node in self.__node:
		if node.getsplitNum() > 0:
			self.__splitNum = node.getsplitNum()
		if node.getparallelNum > 0:
			self.__parallelNum = node.getparallelNum()
		self.__total_args += [node.getnodeContents()]
		#print "total_args : " + str(self.__total_args)
		self.__total_args2 += [node.getnodeContents()]
	self.__total_args2 += ["oneDAG"]

        for key in self.__total_args2:
            if key.find('input') != -1 or key == "oneDAG":
                if key.find('input') != -1:
                    buf_string = '%s \n %s' % (buf_string, key.split("---")[0])
                    buf_args += [key.split("---")[0].strip()]
                if key == "oneDAG":
                    buf_string = '%s \n\t' % buf_string
            else:
                checksum = 0
                for outputkey in buf_args:
                    if outputkey != key.split("---")[0].strip():
                        checksum = checksum + 1
                    else:
                        buf_string = '%s \t\t' % buf_string
                        break
                if checksum == len(buf_args):
                    buf_string = '%s \t %s' % (buf_string, key.split("---")[0])
                    buf_args += [key.split("---")[0].strip()]
	print "Topology"
        print buf_args
        self.__execute_node = buf_args

    def start(self):
        self.Master = self.w.returnMaster()
        self.phase_start = 0
        self.__slaveInfo = self.conf.getSlaveInfo()
	self.__slaveInfo.pop( self.__slaveInfo.index('') )
	self.__slaveInfoInmaster = self.Master.getSlaveInfo()

        for i in range(1, len(self.__total_args)):
            #print "total_args : " + self.__total_args[i]
            self.phase_string =''
            if self.__total_args[i].find('input') != -1:
                for j in range(self.phase_start, i):
                    if j == self.phase_start:
                        self.phase_string = self.__total_args[j]
                    else:
                        self.phase_string = self.phase_string + '/' + self.__total_args[j]
                print self.phase_string
                self.__total_phase += [self.phase_string]
                self.phase_start = i
        for k in range(self.phase_start, len(self.__total_args)):
            if k == self.phase_start == k:
                self.phase_string = self.__total_args[k]
            else:
                self.phase_string = self.phase_string + '/' + self.__total_args[k]
        print self.phase_string
        self.__total_phase += [self.phase_string]
        l = locals()
        for q in range(0, len(self.__total_phase)):
            dag_num = q+1
            l['dag%d' % dag_num] = self.__total_phase[q].split('/')
	
	#self.once=0
	#print 'conf slave Info : '+str(self.__slaveInfo)
	#print 'master slave Info : '+str(self.__slaveInfoInmaster)
	while len(self.__slaveInfo) != len(self.__slaveInfoInmaster):
		self.__slaveInfoInmaster = self.Master.getSlaveInfo()
		pass

	#print "splitNum : " + str(self.__splitNum)
	splitNum = 1
	subExe=1
	slaveNum=0
	d = re.compile(r"source:\w*//")
	numOfphase = len(self.__total_args)
	print "NumPhases : " + str( numOfphase ) 
	totalslaveNum = len( self.__slaveInfoInmaster.keys() )

	if self.__parallelNum >0:
		restWorks = self.__parallelNum
		print self.__parallelNum
		loopNum = math.ceil( float(self.__parallelNum) / len(self.__slaveInfo) )
		for oneloop in range(0, int(loopNum) ):
			executeWorks = 0
			for onework in range(0, len(self.__slaveInfo)):
				if restWorks > 0:
					restWorks -= 1
					executeWorks += 1
					threading._start_new_thread(self.executePhase, ( self.__slaveInfoInmaster.keys()[ onework % totalslaveNum][0], self.__total_args[0]+'&'+str(restWorks), ) )	
			for waits in range(0, executeWorks ):
				self.w.returnMaster().setPhaseEnd()
				while not self.w.returnMaster().IsPhaseEnd():
					pass
		self.__splitNum = 1 - len(self.__total_args)

	for numPhase in range(0, len(self.__total_args)+self.__splitNum-1):
		self.w.returnMaster().setPhaseEnd()
		if splitNum <= numOfphase:
			maxExe=1
			for i in range(splitNum, 0, -1):
				if maxExe <= self.__splitNum:
					print self.__total_args[i-1] + 	str(slaveNum % totalslaveNum) + ' : ' + str(i-1)
					threading._start_new_thread(self.executePhase, (self.__slaveInfoInmaster.keys()[slaveNum % totalslaveNum][0], self.__total_args[i-1], ))
					if self.__total_args[i-1].find('source') > 0:
						print "before i-1 total_args : "+str(self.__total_args[i-1])
						self.__total_args[i-1] = self.__total_args[i-1].replace( d.findall(self.__total_args[i-1])[0], "source:"+self.__total_args[i-2].split('-')[0].strip()+"//")
						print "after i-1 total_args : "+str(self.__total_args[i-1])	
					self.__total_args[i-1] = self.__total_args[i-1].split('---')[0].strip()+'_'+str(splitNum)+' --- '+self.__total_args[i-1].split('---')[1]
					maxExe+=1
					slaveNum+=1
		else:
			for j in range(0, self.__splitNum-subExe):
				if numOfphase-j-1>=0:
					print self.__total_args[numOfphase-j-1] + str(slaveNum % totalslaveNum) + ' : ' + str(numOfphase-j-1)
					threading._start_new_thread(self.executePhase, (self.__slaveInfoInmaster.keys()[slaveNum % totalslaveNum][0], self.__total_args[numOfphase-j-1], ))
					if self.__total_args[i-1].find('source') > 0:
						self.__total_args[numOfphase-j-1] = self.__total_args[numOfphase-j-1].replace( d.findall(self.__total_args[numOfphase-j-1])[0], "source:"+self.__total_args[numOfphase-j-2].split('-')[0].strip()+"//")
					self.__total_args[numOfphase-j-1] = self.__total_args[numOfphase-j-1].split('-')[0].strip()+'_'+str(splitNum)+' --- '+self.__total_args[numOfphase-j-1].split('-')[1]
					slaveNum+=1
			subExe+=1
		exeNum = splitNum
		if splitNum > self.__splitNum:
			exeNum = self.__splitNum
		print 'waits times : ' + str( exeNum-(subExe-1) )
		for waits in range(0, exeNum-(subExe-1) ):
			self.w.returnMaster().setPhaseEnd()
			while not self.w.returnMaster().IsPhaseEnd():
				pass
		splitNum+=1

	killSlaveJobs("slave01")	
	killSlaveJobs("slave02")	
	killSlaveJobs("slave03")	
	killSlaveJobs("slave04")	
	#removeHDFS()
	killpythonJobs()


    # this method send necessary file to slave
    def executePhase(self, host, arg1):
        self.__platform = self.conf.getPlatform()
        #COMMAND = "ssh " + host + " -f \". ~/.profile; python /home/ubuntu/integrated-cloud-platform/src/executeWorker.py \'" + arg1 + "\'\""
	print "arg1 : " + str(arg1)
	files = [item.split(":")[1].replace(",","")  for item in arg1.split("---")[1].split("//") if item.find("map:") != -1 or item.find("reduce:") != -1 or item.find("file:") != -1 or item.find("reffile:") != -1]
	if len(files) > 0:
		filelist=''
		for item in files:
			filelist += item + ' '
	        SCP_CMD = "scp "+filelist+"ubuntu@"+host+":"+self.__platform+"src/"
		os.system(SCP_CMD)
	#print host + ' --- ' + arg1
        self.Master.phaseExecute( host, arg1 )
	#COMMAND = "ssh "+host+" -f \". ~/.profile; python "+self.__platform+"src/executeWorker.py \'"+arg1+"\'\""
        #os.system(COMMAND)

    def runWhat(self, arg):
        if arg == "runMaster":
            self.w.isMaster()
        if arg == "runDisplaytopology":
            self.displaytopology()
        if arg == "runStart":
            self.start()
	#if arg == "runSlave":
	#   self.w.runSlave()

    def run(self):

        sub_thread_array = []
        main_thread_array = []
        sub_thread_array.append(threading.Thread(target=self.runWhat, args=("runDisplaytopology", )))
	#print "Hostname : "+self.conf.getHostname()
	if self.conf.getHostname() == "master":
	        sub_thread_array.append(threading.Thread(target=self.runWhat, args=("runMaster", )))
	#else:
	#	sub_thread_array.append(threading.Thread(target=self.runWhat, args=("runSlave", )))
        main_thread_array.append(threading.Thread(target=self.runWhat, args=("runStart", )))

        for i in range(0, len(sub_thread_array)):
            sub_thread_array[i].start()

        for i in range(0, len(main_thread_array)):
            main_thread_array[i].start()

        for i in range(0, len(main_thread_array)):
            main_thread_array[i].join()

        sys.exit()
        
"""
d = pyquery( [
            key(string) : value(json)
            ])
ex)
d1 = pyquery([  './input1' : {  'work':'twitter'    }                                       ,
                '/output1' : {  'work':'mapreduce', 'map':'file1', 'reduce':'file2' }       ,
                '/output2' : {  'work':'script', 'file':'file3' }                           ,
                '/output3' : {  'work':'openapi', 'auth':'key'  }
            ])
d2 = pyquery(...)
d3 = pyquery(...)
"""

