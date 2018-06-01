 #__author__ = 'yoonsu'

import os
import time

class Hdfsio:

    def __init__(self):
        pass

    def copyData(self, frompath, topath, filename):
        if len(filename) > 0:
            cmd = "hadoop fs -copyToLocal "+frompath+"/"+filename+" "+topath+"/"+filename
        try:
            print cmd
            os.system(cmd)
            return True
        except:
            print "[ "+ str(time.time()) + " ] Fail to copy data From HDFS"
            return False

    def writeData1(self, frompath, topath, filename):
        #if len(filename) >0:
        os.system("hadoop fs -mkdir /user/yoonsu/"+topath)
        cmd = "hadoop fs -copyFromLocal "+frompath+"/"+filename+" /user/yoonsu/"+topath+"/"+filename
        print cmd
        try:
            os.system(cmd)
            os.system("rm "+frompath+"/"+filename)
            return True
        except:
            print "[ "+ str(time.time()) + " ] Fail to write data to HDFS"
            return False

    def writeData(self, frompath, filename1, topath, filename2):
        #if len(filename2) >0:
        os.system("hadoop fs -mkdir /user/yoonsu/"+topath)
        cmd = "hadoop fs -copyFromLocal "+frompath+"/"+filename1+" /user/yoonsu/"+topath+"/"+filename2
        print cmd
        try:
            os.system(cmd)
            os.system("rm "+frompath+"/"+filename1)
            return True
        except:
            print "[ "+ str(time.time()) + " ] Fail to write data to HDFS"
            return False

    def deleteHdfsDir(self, frompath):
	for numS in range(0,len(frompath)):
		self.onesource = frompath[numS].strip()
		cmd = "hadoop fs -rmr /user/yoonsu/"+self.onesource 
 		print cmd
		try:
			os.system(cmd)
			return True
		except:
            		print "[ "+ str(time.time()) + " ] Fail to delete Dir in HDFS"
            		return False

    def displaydata(self, path):
        cmd = "hadoop fs -ls "+path+"/"
        try:
            os.system(cmd)
            return True
        except:
            print "[ "+ str(time.time()) + " ] Fail to read data From HDFS"
            return False

    def isDataInlocal(self, path):
        self.isData = os.listdir(path)
        if( len(self.isData) > 0 ):
            return True
        else:
            return False
