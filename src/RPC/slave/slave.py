__author__ = 'yoonsu'

import socket
import sys
import threading
import time
import random

from src.Monitoring.monitoring import *
from src.conf.config import Config
from src.executeWorker import *

class Slave:
    slaveIns = None
    def __init__(self):
        self._master_ip = ''
        self._slave_ip = ''
        self._hostname = ''
        self.PORT = 30000
	self.completeWorks =False;
	self.w = Worker()
	self.w.setSlave(self)
	#Slave.slaveIns = self
        conf = Config()
        self.__platform = conf.getPlatform()

    def setSlave(self):
	Slave.slaveIns = self
	print self	

    def getSlave(self):
	return Slave.slaveIns

    def getSlaveIP(self):
        f = open(self.__platform+'src/conf/slaves')
        hosts_file = f.read()
        hosts_array = hosts_file.split('\n')
        ip = []

        for i in range(0, len(hosts_array)-1):
            try:
                ip.append(hosts_array[i])
            except:
                print "Exception Error when getting IP/Name of connected Node"
        self._slave_ip = ip

    def getMasterIP(self):
        f = open(self.__platform+'src/conf/masters')
        network = f.read()
        self._master_ip = network.strip('\n')

    def getHostname(self):
	#self._hostname = socket.getfqdn()
        f = open('/etc/hostname')
        self._hostname = f.read().strip('\n')
	return self._hostname

    def _getheartbeatmsg(self):
        sysMon = SystemMonitor()
        return sysMon.get_CPU_Mem()

    def sendingMsg(self):
        color = random.randrange(0, 5)
        if color==0:
            color_value = '\033[95m'
        if color==1:
            color_value = '\033[94m'
        if color==2:
            color_value = '\033[93m'
        if color==3:
            color_value = '\033[92m'
        if color==4:
            color_value = '\033[91m'
        self.getHostname()
        color_end = '\033[0m'

        while not self.completeWorks:
            cpu, mem = self._getheartbeatmsg()
            data = color_value+"[ "+str( time.strftime( "%Y-%m-%d %H-%M-%S", time.gmtime( time.time() ) ) )+ " ] "+self._hostname+" : cpu usage - "+cpu+" , mem usage - "+mem+color_end
            #print(data)
            self.sock.sendall(data)
            time.sleep(10)

    def gettingMsg(self):
        while not self.completeWorks:
            data = self.sock.recv(1024)
            if data:
		print "slaveData : "+data
		self.datasplits = data.split('---')
		self.sendCmdtoMaster("slaveReceiveData : "+data)
		#print self.datasplits
		#print self.datasplits[1]+'---'+self.datasplits[2]
		self.w.executeONEwork( self.datasplits[1] + ' --- ' + self.datasplits[2] )
		#self.w.executeONEwork( self.datasplits[1] )

    def runSlave(self):

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.getMasterIP()
        master_address = (self._master_ip, self.PORT)
        print >> sys.stderr, 'waiting for packets on %s port %s' % master_address
        self.sock.connect(master_address)

        t_send = threading.Thread(target=self.sendingMsg, args=())
        t_get = threading.Thread(target=self.gettingMsg, args=())

        t_send.start()
        t_get.start()

#        t_send.join()
#        t_get.join()

    def returnSlaveClass(self):
	return self

    def sendCmdtoMaster(self, cmd):
	self.sock.send(cmd)

    def _sendheartbeatmsg(self):
        heartbeatsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_address = (self.master_ip, self.PORT)
        heartbeatsock.bind(master_address)
        print >> sys.stderr, 'starting up to send heartbeatmsg to master ( %s ) on port %s' % master_address
        heartbeatsock.connect(master_address)

        try:
            self._useageCPU, self._useageMem = self._getheartbeatmsg()
            print >> sys.stderr, 'sending %s' % self._useageCPU
            heartbeatsock.sendall(self._useageCPU)
            print >> sys.stderr, 'sending %s' % self._useageMem
            heartbeatsock.sendall(self._useageMem)
        finally:
            print >> sys.stderr, 'closing socket'
            heartbeatsock.close()

    def _getMasterIP(self):
        mastersock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        slave_address = (self._getlocalhost(), self.PORT)
        mastersock.bind(slave_address)
        mastersock.listen(1)
        receive = 0
        while( receive == 1 ):
            print >> sys.stderr, 'waiting for a connection'
            connection, master_address = mastersock.accept()
            try:
                print >> sys.stderr, 'connection from ', master_address
                data = connection.recv(16)

                if data:
                    print >> sys.stderr, 'received %s' % data
                    self.master_ipname, self.master_ip = data.split('\t')
                    receive = 1
                else:
                    print >> sys.stderr, 'no data from ', master_address
            finally:
                connection.close()
            connection.close()
        #return self.master_ip

if __name__ == '__main__':
	slave = Slave()
	slave.setSlave()
	slave.runSlave()


