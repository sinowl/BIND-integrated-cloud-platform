__author__ = 'yoonsu'

import os

class executemapreduce:
    def __init__(self):
        pass

    def executemapreduce(self, inputfile, outputfile, mapfilename, reducefilename, numofreducer):
        cmdmapreduce = "hadoop jar ./integrated-cloud-platform/src/hadoop-streaming-1.0.4.jar" + \
                       " -input " + inputfile + \
                       " -output " + outputfile + \
                       " -numReduceTasks " + numofreducer + \
                       " -mapper " +"/home/ubuntu/integrated-cloud-platform/src/"+mapfilename + \
                       " -reducer " +"/home/ubuntu/integrated-cloud-platform/src/"+reducefilename + \
                       " -file " +"/home/ubuntu/integrated-cloud-platform/src/"+mapfilename + \
                       " -file " +"/home/ubuntu/integrated-cloud-platform/src/"+reducefilename
        print cmdmapreduce
        os.system(cmdmapreduce)

    def executesinglemapreduce(self, inputfile, outputfile, mapfilename, reducefilename):
        cmdsinglemapreduce = "hadoop jar ./integrated-cloud-platform/src/hadoop-streaming-1.0.4.jar" + \
                       " -input " + inputfile + \
                       " -output " + outputfile + \
                       " -mapper " +"./integrated-cloud-platform/src/"+mapfilename + \
                       " -reducer " +"./integrated-cloud-platform/src/"+reducefilename + \
                       " -file " +"./integrated-cloud-platform/src/"+mapfilename + \
                       " -file " +"./integrated-cloud-platform/src/"+reducefilename
        print cmdsinglemapreduce
        os.system(cmdsinglemapreduce)

    def checkMapReduceJob(self):
	cmd = "hadoop job -list | grep job_"
	checkcmd = os.system(cmd)
	if( checkcmd == 0 ):
		return True
	else:
		return False	
