__author__ = 'yoonsu'

import os
import re

class SystemMonitor:

    def __init__(self):
        self._cpu = ''
        self._mem = ''
        self._CPU_array = []
        self._Mem_array = []
        self._useageCPU = ''
        self._useageMem = ''

    def runTop(self):
        os.system("top -b -n 1 | grep Cpu >> tmp_cpu")
        os.system("top -b -n 1 | grep Mem >> tmp_mem")
        f_cpu = open("tmp_cpu")
        f_mem = open("tmp_mem")
        ansi_cpu = f_cpu.read()
        ansi_mem = f_mem.read()

        ansi_escape = re.compile(r'\x1b[^m]*m')

        self._cpu = ansi_escape.sub('', ansi_cpu)
        self._mem = ansi_escape.sub('', ansi_mem)

        os.system("rm tmp_cpu")
        os.system("rm tmp_mem")

    def displayTop(self):
        print self._cpu
        print self._mem

    def usage_CPU_Mem(self):
        self._CPU_array = self._cpu.split()
        self._Mem_array = self._mem.split()
        self._useageCPU = self._CPU_array[1]

        self._usedMem = self._Mem_array[self._Mem_array.index('used,')-1].translate(None,"k")
        self._totalMem = self._Mem_array[self._Mem_array.index('total,')-1].translate(None,"k")
        self._useageMem = str( float(self._usedMem) / float(self._totalMem) )


    def display_CPU_Mem(self):
        self.runTop()
        self.usage_CPU_Mem()
        print "CPU used : ", self._useageCPU +" %"
        print "Mem used : ", self._useageMem +" %"

    def get_CPU_Mem(self):
        self.runTop()
        self.usage_CPU_Mem()
        return self._useageCPU, self._useageMem

if __name__ == '__main__':
    system = SystemMonitor()
    while(1):
        system.runTop()
        system.displayTop()
        system.usage_CPU_Mem()
        system.display_CPU_Mem()