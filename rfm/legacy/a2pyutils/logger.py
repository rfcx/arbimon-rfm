import os
from config import EnvironmentConfig
import time

configuration = EnvironmentConfig()


class Logger:
    def __init__(self, jobId, script, logFor='worker', logON=True):
        if type(jobId) is not int:
            raise ValueError("jobId must be a number")
        if type(script) is not str:
            raise ValueError("script must be a string")
        if type(logFor) is not str:
            raise ValueError("logFor must be a string")
        if type(logON) is not bool:
            raise ValueError("logON must be a boolean")

        self.logON = logON
        self.also_print = False
        if self.logON:
            tempFolders = str(configuration.pathsConfig['temp_dir'])
            self.workingFolder = tempFolders+"/logs/job_"+str(jobId)
            if not os.path.exists(self.workingFolder):
                os.makedirs(self.workingFolder)

            lognametry = 0
            self.filePath = self.workingFolder+"/"+script+"_"+logFor+"_"+str(lognametry)+".log"

            while os.path.isfile(self.filePath):
                lognametry += 1
                self.filePath = self.workingFolder+"/"+script+"_"+logFor+"_"+str(lognametry)+".log"

            self.log_file_handle = open(self.filePath, 'w')
            self.write(script+' log file')
            if self.log_file_handle:
                self.log_file_handle.close()
            self.log_file_handle = None
            self.jobId = jobId
            self.logFor = logFor

    def write(self, message):
        if self.logON:
            currTime = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            if self.log_file_handle:
                self.log_file_handle.write(currTime+':\t'+message+'\n')
            else:
                self.log_file_handle = open(self.filePath, 'a')
                self.log_file_handle.write(currTime+':\t'+message+'\n')
            if self.log_file_handle:
                self.log_file_handle.close()
                self.log_file_handle = None
            if self.also_print:
                print "#LOG:" + currTime + ':\t'+message

    def time_delta(self, message, start):
        self.write("{} --- seconds --- {}".format(
            message, time.time() - start
        ))

    def write_clean(self, message):
        if self.logON:
            if self.log_file_handle:
                self.log_file_handle.write(message)
            else:
                self.log_file_handle = open(self.filePath, 'a')
                self.log_file_handle.write(message)
            if self.log_file_handle:
                self.log_file_handle.close()
                self.log_file_handle = None

    def close(self):
        if self.logON:
            if self.log_file_handle:
                self.write('end of log')
                self.log_file_handle.close()
                self.log_file_handle = None

    def __exit__(self, type, value, traceback):
        if self.logON:
            if self.log_file_handle:
                self.write('end of log')
                self.log_file_handle.close()
                self.log_file_handle = None
