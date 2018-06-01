__author__ = 'yoonsu'

import socket
import sys
import threading
import time
import os
from operator import itemgetter
from collections import OrderedDict
from src.conf.config import Config
from src.Works.viz.executeHTTP import *

class Master:

    def __init__(self):
        self._slaveip = []
        self._masterip = ''
        self.PORT = 30000
        self._conn = ''
        self._addr = ''
        self._conn2 = ''
        self._addr2 = ''
        self._conn3 = ''
        self._addr3 = ''
        self._conn4 = ''
        self._addr4 = ''
        self.__slaveInfo = {}
        self.__endPhase = False
	self.registerConn={}
	self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	config = Config()

        self.__platform = config.getPlatform()
	self.getSlaveIP()

    def returnSlaveip(self):
        return self._slaveip

    def returnMasterip(self):
        return self._masterip

    def ismaster(self):
        self.getMasterIP()
        nodeip = socket.gethostbyname(socket.gethostname())
        if self._masterip == nodeip.strip():
            return True
        else:
            return False

    def getSlaveIP(self):
        #f = open('/home/ubuntu/integrated-cloud-platform/src/conf/slaves')
        f = open(self.__platform+'src/conf/slaves')
        hosts_file = f.read()
        hosts_array = hosts_file.split('\n')
        ip = []

        for i in range(0, len(hosts_array)-1):
            try:
                ip.append(hosts_array[i])
            except:
                print "Exception Error when getting IP/Name of connected Node"
        self._slaveip = ip

    def getMasterIP(self):
        #f = open('/home/ubuntu/integrated-cloud-platform/src/conf/masters')
        f = open(self.__platform+'src/conf/masters')
        network = f.read()
        self._masterip = network.strip('\n')

    def rpcRunSlaves(self, host):
        #time.sleep(3)
        #COMMAND = "\". ~/.profile; python /home/ubuntu/integrated-cloud-platform/src/RPC/slave/slave.py\""
        COMMAND = "\". ~/.profile; python "+self.__platform+"src/RPC/slave/slave.py\""
        print "%s" % host, COMMAND
        os.system("ssh %s -f %s" % (host, COMMAND))

    def sendingMsg(self, data):
        self._conn.sendall(data)

    def phaseExecute(self, slaveip, args):
	self.registerConn[slaveip].sendall( slaveip+ ' --- '+ args )

    def gettingMsg(self, connection, address):
	self.registerConn[address[0]] = connection
	self.viz = Viz()			
	self.fulldata =''
	self.datavalue=[]
        while True:
            data = connection.recv(1024)
            if data:
		print data
		if data.find("Start") >= 0:
			print data
		if data.find("End") >0:
			print data
		#if data.split("-")[0].strip() == "end":
		#if data.find("end") >0:
			self.__endPhase = True
			print "end of phase : " + str( self.__endPhase )
		if data.find('cpu') >= 0:
			if data.find('word') == -1:
	                	self.saveSlaveInfo(address, data)
			#print data
		if data.find('script') >= 0:
			self.fulldata += data
		if data.find('script-end') >= 0:
			#print self.fulldata
			datasplits = self.fulldata.split('abcdefghijk321654')
			for j in range(0, len(datasplits) ):
				if datasplits[j].strip() == 'script':
					self.datavalue+=[ datasplits[j+1] ]	
					#print datasplits[j+1]	
			self.fulldata = ''
			for item in self.datavalue:
				print item	
		if data.find('http') >= 0:
			self.fulldata += data
		if data.find('http-end') >= 0:
			#print self.fulldata
			datasplits = self.fulldata.split('abcdefghijk321654')
			for j in range(0, len(datasplits) ):
				if datasplits[j].strip() == 'http':
					self.viz.insertMSG( datasplits[j+1] )
					print datasplits[j+1]
			self.viz.initHTTP()
			#self.viz.startHTTP()
			#self.viz.startHTTPforever()	
    def IsPhaseEnd(self):
	return self.__endPhase

    def setPhaseEnd(self):
	self.__endPhase = False

    def saveSlaveInfo(self, address, data):
        check = 0
        first_index = data.find(':') + 13
        second_index = data.find(',')
        third_index = data.rfind('-')+1
        cpu_usage = data[first_index:second_index].strip()
        mem_usage = data[third_index:third_index+10].strip()
        for key in self.__slaveInfo.keys():
            if key == address:
                check = 1
        if check == 0:
            self.__slaveInfo[address] = {'cpu': float(cpu_usage), 'mem': float(mem_usage)}
        else:
            self.__slaveInfo[address] = {'cpu': float(cpu_usage), 'mem': float(mem_usage)}
        self.__slaveInfo = OrderedDict( sorted(self.__slaveInfo.iteritems(), key=itemgetter(1), reverse=False) )
	#self.displaySlaveInfo()

    def getSlaveInfo(self):
        return self.__slaveInfo

    def displaySlaveInfo(self):
        print self.getSlaveInfo().keys()

    def runMaster(self):
        #self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.getMasterIP()
        #self.getSlaveIP()
        HOST = self._masterip
        master_address = (HOST, self.PORT)
        print >> sys.stderr, 'starting up on %s port %s' % master_address
        self.sock.bind(master_address)
        self.sock.listen(len(self._slaveip))

        threads_array = []
        for i in range(0, len(self._slaveip)):
            host = "ubuntu@" + self._slaveip[i]
            t1 = threading.Thread(target=self.rpcRunSlaves, args=(host,))
            threads_array.append(t1)
            threads_array[i].start()
            threads_array[i].join()
            #threading._start_new_thread(self.rpcRunSlaves, (host,) )

        slave_threads_array = []
        for i in range(0, len(self._slaveip)):
            self._conn, self._addr = self.sock.accept()
            print >> sys.stderr, "Connected by ", self._addr
            t2 = threading.Thread(group=None, target=self.gettingMsg, name=None, args=(self._conn, self._addr,))
            slave_threads_array.append(t2)
            slave_threads_array[i].start()
            #slave_threads_array[i].join()

        for i in range(0, len(self._slaveip)):
            threads_array[i].join()

        for i in range(0, len(self._slaveip)):
            slave_threads_array[i].join()

if __name__ == '__main__':
    master = Master()
    master.runMaster()
