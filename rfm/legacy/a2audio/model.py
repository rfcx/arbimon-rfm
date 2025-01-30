
from sklearn.ensemble import RandomForestClassifier
import numpy
import pickle
from itertools import count
import random
import csv

class Model:

    def __init__(self,classid,speciesSpec,jobid):
        if type(classid) is not str and type(classid) is not int:
            raise ValueError("classid must be a string or int. Input was a "+str(type(classid)))
        if not isinstance(speciesSpec, (numpy.ndarray, numpy.generic, numpy.memmap)):
            raise ValueError("speciesSpec must be a numpy.ndarray. Input was a "+str(type(speciesSpec)))
        if type(jobid) is not int:
            raise ValueError("jobid must be a int. Input was a "+str(type(jobid)))
        
        self.classid = classid
        self.speciesSpec = speciesSpec
        self.data  = numpy.zeros(shape=(0,41))
        self.classes = []
        self.uris = []
        self.minv = 9999999
        self.maxv = -9999999
        self.jobId = jobid
        
    def addSample(self,present,row,uri):
        if numpy.any(numpy.isnan(row)):
            row = [numpy.float32(0) if numpy.isnan(x) else x for x in row]
        self.classes.append(str(present))
        self.uris.append(uri)
        if self.minv > row[3]:
            self.minv = row[3]
        if self.maxv < row[2]:
            self.maxv = row[2]
        self.data = numpy.vstack((self.data,row))
    
    def getDataIndices(self):
        return {"train":self.trainDataIndices ,"validation": self.validationDataIndices}
    
    def splitData(self,useTrainingPresent,useTrainingNotPresent,useValidationPresent,useValidationNotPresent):
        self.splitParams = [useTrainingPresent,useTrainingNotPresent,useValidationPresent,useValidationNotPresent]

        presentIndices = [i for i, j in zip(count(), self.classes) if j == '1' or j == 1]
        notPresentIndices = [i for i, j in zip(count(), self.classes) if j == '0' or j == 0]
        
        if(len(presentIndices) < 1):
            return False
        if(len(notPresentIndices) < 1):
            return False
          
        random.shuffle(presentIndices)
        random.shuffle(notPresentIndices)
        
        self.trainDataIndices = presentIndices[:useTrainingPresent] + notPresentIndices[:useTrainingNotPresent]
        self.validationDataIndices = presentIndices[useTrainingPresent:(useTrainingPresent+useValidationPresent)] + notPresentIndices[useTrainingNotPresent:(useTrainingNotPresent+useValidationNotPresent)]
    
    def getModel(self):
        return self.clf
    
    def getOobScore(self):
        return self.obbScore
    
    def train(self):
        self.clf = RandomForestClassifier(n_estimators=1000,n_jobs=-1,oob_score=True)
        classSubset = [self.classes[i] for i in self.trainDataIndices]
        self.clf.fit(self.data[self.trainDataIndices], classSubset)
        self.obbScore = self.clf.oob_score_
        
    def retrain(self):
        self.clf = RandomForestClassifier(n_estimators=1000,n_jobs=-1,oob_score=True)
        self.clf.fit(self.data, self.classes)
        self.obbScore = self.clf.oob_score_
        
    def validate(self):
        classSubset = [self.classes[i] for i in self.validationDataIndices]
        classSubsetTraining = [self.classes[i] for i in self.trainDataIndices]
        self.outClasses = classSubset
        self.outClassesTraining = classSubsetTraining
        self.outuris = [self.uris[i] for i in self.validationDataIndices]
        self.outurisTraining = [self.uris[i] for i in self.trainDataIndices]
        predictions = self.clf.predict(self.data[self.validationDataIndices])
        self.validationpredictions = predictions
        presentIndeces = [i for i, j in zip(count(), classSubset) if j == '1' or j == 1] 
        notPresentIndices = [i for i, j in zip(count(), classSubset) if j == '0' or j == 0]
        minamxdata = self.data[self.validationDataIndices]
        minv = 99999999
        maxv = -99999999
        for row in minamxdata:
            if max(row) > maxv:
                maxv = max(row)
            if min(row) < minv:
               minv = min(row)
        self.minv = minv
        self.maxv = maxv
        self.tp = 0.0
        self.fp = 0.0
        self.tn = 0.0
        self.fn = 0.0
        self.accuracy_score = 0.0
        self.precision_score = 0.0
        self.sensitivity_score = 0.0
        self.specificity_score  = 0.0
        
        truePositives =  [classSubset[i] for i in presentIndeces]
        truePosPredicted =  [predictions[i] for i in presentIndeces]
        for i in range(len(truePositives)):
            if truePositives[i] == truePosPredicted[i]:
                self.tp = self.tp + 1.0
            else:
                self.fn = self.fn + 1.0
               
        trueNegatives = [classSubset[i] for i in notPresentIndices]
        trueNegPrediceted = [predictions[i] for i in notPresentIndices]
        for i in range(len(trueNegatives )):
            if trueNegatives[i] == trueNegPrediceted[i]:
                self.tn = self.tn + 1.0
            else:
                self.fp = self.fp + 1.0
        
        if (self.tp+self.fp+self.tn+self.fn) >0:
            self.accuracy_score = (self.tp +  self.tn)/(self.tp+self.fp+self.tn+self.fn)
        if (self.tp+self.fp) > 0:
            self.precision_score = self.tp/(self.tp+self.fp)
        if (self.tp+self.fn) > 0:
            self.sensitivity_score = self.tp/(self.tp+self.fn)
        if (self.tn+self.fp) > 0:
            self.specificity_score  = self.tn/(self.tn+self.fp)
        
    def modelStats(self):
        return [self.accuracy_score,self.precision_score,self.sensitivity_score,self.obbScore,self.speciesSpec,self.specificity_score ,self.tp,self.fp,self.tn,self.fn,self.minv,self.maxv]
    
    def save(self,filename,l,h,c):
        with open(filename, 'wb') as output:
            pickle.dump([self.clf,self.speciesSpec,l,h,c], output, -1)
            
    def getSpec(self):
        return self.speciesSpec
   
    def getClasses(self):
        return self.classes
    
    def getData(self):
        return self.data
    
    def saveValidations(self,filename):
        with open(filename, 'w') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',')
            for i in range(0,len(self.outClasses)):
                spamwriter.writerow([self.outuris[i],self.outClasses[i],self.validationpredictions[i],'validation'])
            for i in range(0,len(self.outClassesTraining)):
                spamwriter.writerow([self.outurisTraining[i],self.outClassesTraining[i],'NA','training'])
                