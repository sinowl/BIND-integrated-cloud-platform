__author__ = 'yoonsu'

import os

class Config:

    def __init__(self):
        self.abspath = os.path.dirname(os.path.abspath(__file__))
        self.__f = open(self.abspath+'/config')
        self.__home = ''
        self.__hadoop = ''
        self.__platform = ''
	self.__hostname =''
        self.__getConfig()

    def getSlaveInfo(self):
	f = open(self.abspath+'/slaves')
	self.__slaveInfo = f.read().split('\n')
	return self.__slaveInfo

    def getHostname(self):
        f = open('/etc/hostname')
        self.__hostname = f.read().strip('\n')
	return self.__hostname

    def __getConfig(self):
        #print self.__f.read()
        self.__configArray = self.__f.read().split('\n')
        for i in range(0, len(self.__configArray)):
            self.__one = self.__configArray[i].split(':')
            if self.__one[0].strip() == 'home':
                self.__home = self.__one[1].strip()
            if self.__one[0].strip() == 'hadoop':
                self.__hadoop = self.__one[1].strip()
            if self.__one[0].strip() == 'platform':
                self.__platform = self.__one[1].strip()
        #self.__home = self.__configArray[0].split(':')[1].strip()
        #self.__hadoop = self.__configArray[1].split(':')[1].strip()

    def getHome(self):
        if self.__home.rfind('/') != len(self.__home)-1:
            self.__home = self.__home + '/'
        if self.__home.find('/') != 0:
            self.__home = '/'+self.__home
        return self.__home

    def getHadoop(self):
        if self.__hadoop.rfind('/') != len(self.__hadoop)-1:
            self.__hadoop = self.__hadoop + '/'
        if self.__hadoop.find('/') != 0:
            self.__hadoop = '/'+self.__hadoop
        return self.__hadoop

    def getPlatform(self):
        if self.__platform.rfind('/') != len(self.__platform)-1:
            self.__platform = self.__platform + '/'
        if self.__platform.find('/') != 0:
            self.__platform = '/'+self.__platform
        return self.__platform

if __name__ == '__main__':
    con = Config()
    print con.getHome()
    print con.getHadoop()
    print con.getPlatform()
    print con.getHostname()
    print con.getSlaveInfo()
