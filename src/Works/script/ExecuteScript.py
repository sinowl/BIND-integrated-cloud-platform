__author__ = 'yoonsu'

import os

class ExecuteScript:

    def __init__(self):
        pass

    def executepython(self, filename, args):
        os.system("python "+"./integrated-cloud-platform/src/"+filename+" "+args)
