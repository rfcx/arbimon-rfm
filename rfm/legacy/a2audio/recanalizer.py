import json
import math
import numpy
import os
import random
import time
import warnings
import cv2
# from cv import *
from contextlib import closing
from pylab import *
from skimage.metrics import structural_similarity as ssim
from scipy.stats import pearsonr as prs
from scipy.stats import kendalltau as ktau
from scipy.spatial.distance import cityblock as ct
from scipy.spatial.distance import cosine as csn
from scipy.stats import *
from scipy.signal import *
from ..a2pyutils.logger import Logger
from .rec import Rec
from .thresholder import Thresholder
from .filters.resample_poly_filter import resample_poly_filter
from .constants import FREQUENCIES_44100

class Recanalizer:
    def __init__(self, uri, speciesSurface, low, high, tempFolder, bucketName, logs=None,test=False,ssim=True,searchMatch=False,db=None,rec_id=None,job_id=None,modelSampleRate=44100,legacy=True):
        if type(uri) is not str and type(uri) is not unicode:
            raise ValueError("uri must be a string")
        if not isinstance(speciesSurface, (numpy.ndarray, numpy.generic, numpy.memmap)):
            raise ValueError("speciesSurface must be a numpy.ndarray. Input was a "+str(type(speciesSurface)))
        if type(low) is not int and type(low) is not float:
            raise ValueError("low must be a number")
        if type(high) is not int and type(high) is not float:
            raise ValueError("high must be a number")
        if low >= high :
            raise ValueError("low must be less than high")
        if type(tempFolder) is not str:
            raise ValueError("invalid tempFolder")
        if not os.path.exists(tempFolder):
            raise ValueError("invalid tempFolder")
        elif not os.access(tempFolder, os.W_OK):
            raise ValueError("invalid tempFolder")
        if type(bucketName) is not str:
            raise ValueError("bucketName must be a string")
        if logs is not None and not isinstance(logs, Logger):
            raise ValueError("logs must be a a2pyutils.Logger object")
        self.ssim = ssim
        self.step = 32
        start_time = time.time()
        self.low = float(low)
        self.high = float(high)
        self.columns = speciesSurface.shape[1]
        self.speciesSurface = speciesSurface
        self.modelSampleRate = modelSampleRate
        self.logs = logs
        self.uri = uri
        self.bucketName = bucketName
        self.tempFolder = tempFolder
        self.rec = None
        self.status = 'NoData'
        self.searchMatch = searchMatch
        self.db =db
        self.rec_id = rec_id
        self.job_id = job_id
        self.legacy = legacy
        # if self.logs:
        #    self.logs.write("processing: "+self.uri)
        # if self.logs :
        #     self.logs.write("configuration time --- seconds ---" + str(time.time() - start_time))

        if not test:
            self.process()

    def process(self):
        start_time = time.time()
        self.instanceRec()
        if self.logs:
            self.logs.write("retrieving recording from bucket --- seconds ---" + str(time.time() - start_time))
        if self.rec.status == 'HasAudioData':
            # If the recording's sample rate is not modelSampleRate, resample the audio data
            if self.rec.sample_rate != self.modelSampleRate and self.modelSampleRate >= 44100:
                self.rec_resample(self.modelSampleRate)
            maxFreqInRec = float(self.rec.sample_rate)/2.0
            if self.high >= maxFreqInRec:
                self.status = 'CannotProcess'
            else:
                start_time = time.time()
                # start_time_all = time.time()
                self.spectrogram()
                if self.spec.shape[1] < 2*self.speciesSurface.shape[1]:
                    self.status = 'AudioIsShort'
                    # if self.logs:
                    #     self.logs.write("spectrogrmam --- seconds ---" + str(time.time() - start_time))
                else:
                    # if self.logs:
                    #     self.logs.write("spectrogrmam --- seconds ---" + str(time.time() - start_time))
                    # start_time = time.time()
                    self.featureVector_search()

                    # if self.db:
                    #     elapsed = time.time() - start_time_all
                    #     with closing(self.db.cursor()) as cursor:
                    #         cursor.execute('insert into  `recanalizer_stats` (job_id,rec_id,exec_time) VALUES('+str(self.job_id)+','+str(self.rec_id)+','+str(elapsed)+')')
                    #         self.db.commit()
                    # if self.logs:
                    #     self.logs.write("feature vector --- seconds ---" + str(time.time() - start_time))
                    self.status = 'Processed'
        else:
            self.status = self.rec.status

    def getRec(self):
        return self.rec

    def instanceRec(self):
        self.rec = Rec(str(self.uri),self.tempFolder,self.bucketName,None,legacy=self.legacy)

    def getVector(self ):
        return self.distances

    def features(self):
        if len(self.distances)<1:
            self.featureVector()
        N = len(self.distances)
        fvi = np.fft.fft(self.distances, n=2*N)
        acf = np.real( np.fft.ifft( fvi * np.conjugate(fvi) )[:N] )
        acf = acf/(N - numpy.arange(N))

        xf = abs(numpy.fft.fft(self.distances))

        fs = [ numpy.mean(xf), (max(xf)-min(xf)),
                max(xf), min(xf)
                , numpy.std(xf) , numpy.median(xf),skew(xf),
                kurtosis(xf),acf[0] ,acf[1] ,acf[2]]
        hist = histogram(self.distances,6)[0]
        cfs =  cumfreq(self.distances,6)[0]
        ffs = [numpy.mean(self.distances), (max(self.distances)-min(self.distances)),
                    max(self.distances), min(self.distances)
                    , numpy.std(self.distances) , numpy.median(self.distances),skew(self.distances),
                    kurtosis(self.distances),moment(self.distances,1),moment(self.distances,2)
                    ,moment(self.distances,3),moment(self.distances,4),moment(self.distances,5)
                    ,moment(self.distances,6),moment(self.distances,7),moment(self.distances,8)
                    ,moment(self.distances,9),moment(self.distances,10)
                    ,cfs[0],cfs[1],cfs[2],cfs[3],cfs[4],cfs[5]
                    ,hist[0],hist[1],hist[2],hist[3],hist[4],hist[5]
                    ,fs[0],fs[1],fs[2],fs[3],fs[4],fs[5],fs[6],fs[7],fs[8],fs[9],fs[10]]
        return ffs

    def featureVector_search(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # if self.logs:
            #    self.logs.write("featureVector start")
            # if self.logs:
            #    self.logs.write(self.uri)

            self.matrixSurfacComp = numpy.copy(self.speciesSurface[self.spechigh:self.speclow,:])
            removeUnwanted = self.matrixSurfacComp == -10000
            if len(removeUnwanted) > 0  :
                self.matrixSurfacComp[self.matrixSurfacComp[:,:]==-10000] = numpy.min(self.matrixSurfacComp[self.matrixSurfacComp != -10000])
            spec = self.spec
            currColumns = self.spec.shape[1]
            spec = ((spec-numpy.min(numpy.min(spec)))/(numpy.max(numpy.max(spec))-numpy.min(numpy.min(spec))))*255
            spec = spec.astype('uint8')
            pat = self.matrixSurfacComp
            pat = ((pat-numpy.min(numpy.min(pat)))/(numpy.max(numpy.max(pat))-numpy.min(numpy.min(pat))))*255
            pat = pat.astype('uint8')
            th, tw = pat.shape[:2]

            result = cv2.matchTemplate(spec, pat, cv2.TM_CCOEFF_NORMED)

            self.distances = numpy.mean(result,axis=0)

    def featureVector(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # if self.logs:
            #    self.logs.write("featureVector start")
            # if self.logs:
            #    self.logs.write(self.uri)
            pieces = self.uri.split('/')
            self.distances = []
            currColumns = self.spec.shape[1]
            step = self.step
            if self.logs:
               self.logs.write("featureVector start")
            self.matrixSurfacComp = numpy.copy(self.speciesSurface[self.spechigh:self.speclow,:])
            self.matrixSurfacComp[self.matrixSurfacComp[:,:]==-10000] = numpy.min(self.matrixSurfacComp[self.matrixSurfacComp != -10000])
            winSize = min(self.matrixSurfacComp.shape)
            winSize = min(winSize,7)
            if winSize %2 == 0:
                winSize = winSize - 1
            spec = self.spec;
            if self.searchMatch:
                if self.logs:
                    self.logs.write("using search match")
                self.computeGFTT(numpy.copy(self.matrixSurfacComp),numpy.copy(spec),currColumns)
            else:
                if self.ssim:
                    if self.logs:
                        self.logs.write("using ssim")
                    for j in range(0,currColumns - self.columns,step):
                        val = ssim( numpy.copy(spec[: , j:(j+self.columns)]) , self.matrixSurfacComp , win_size=winSize)
                        if val < 0:
                           val = 0
                        self.distances.append(  val   )
                else:
                    if self.logs:
                        self.logs.write("not using ssim")
                    threshold = Thresholder()
                    matrixSurfacCompCopy = threshold.apply(numpy.copy(self.matrixSurfacComp))
                    specCopy = threshold.apply(numpy.copy(spec))
                    maxnormforsize = numpy.linalg.norm( numpy.ones(shape=matrixSurfacCompCopy.shape) )
                    for j in range(0,currColumns - self.columns,step):
                        val = numpy.linalg.norm( numpy.multiply ( (specCopy[: , j:(j+self.columns)]), matrixSurfacCompCopy ) )/maxnormforsize
                        self.distances.append(  val )
            if self.logs:
               self.logs.write("featureVector end")

    def computeGFTT(self,pat,spec,currColumns):
        currColumns = self.spec.shape[1]
        spec = ((spec-numpy.min(numpy.min(spec)))/(numpy.max(numpy.max(spec))-numpy.min(numpy.min(spec))))*255
        spec = spec.astype('uint8')
        self.distances = numpy.zeros(spec.shape[1])
        pat = ((pat-numpy.min(numpy.min(pat)))/(numpy.max(numpy.max(pat))-numpy.min(numpy.min(pat))))*255
        pat = pat.astype('uint8')
        #self.logs.write("shape:"+str(spec.shape)+' '+str(self.distances.shape))
        th, tw = pat.shape[:2]
        result = cv2.matchTemplate(spec, pat, cv2.TM_CCOEFF)
        #self.logs.write(str(result))
        #self.logs.write(str(len(result)))
        #self.logs.write(str(len(result[0])))
        #self.logs.write(str(numpy.max(result)))
        #self.logs.write(str(numpy.mean(result)))
        #self.logs.write(str(numpy.min(result)))
        (_, _, minLoc, maxLoc) = cv2.minMaxLoc(result)
        #self.logs.write(str(minLoc)+' '+str(maxLoc))
        threshold = numpy.percentile(result,98.5)
        loc = numpy.where(result >= threshold)
        winSize = min(pat.shape)
        winSize = min(winSize,7)
        step = 16
        if winSize %2 == 0:
            winSize = winSize - 1
        ssimCalls = 0
        #if self.logs:
               #self.logs.write("searching locations : "+str(len(loc[0])))
               #self.logs.write(str(loc))
        xs = []
        s = -999
        for pt in zip(*loc[::-1]):
            if abs(pt[0] - s)>step/2 :
                xs.append(pt[0])
            s = pt[0]
        self.logs.write(str(xs))
        xs_smpl = [ xs[i] for i in sorted(random.sample(xrange(len(xs)), min(4,len(xs)))) ]
        self.logs.write(str(xs_smpl))
        for pts in xs_smpl:
            if pts+math.floor((pat.shape[1]*1.33))<=currColumns:
                for pt in range(max(0,pts-int(math.floor(pat.shape[1]/3))),min(currColumns - self.columns,pts+int(math.floor((pat.shape[1]*1.33)))),step):
                    ssimCalls = ssimCalls + 1
                    val = ssim( numpy.copy(spec[:,pt:(pt+tw)]) , pat, win_size=winSize)
                    if val < 0:
                        val = 0
                    self.distances[pt+tw/2] = val
        # for pt in range(max(0,maxLoc[0]-int(math.floor(pat.shape[1]/3))),min(currColumns - self.columns,maxLoc[0]+int(math.floor((pat.shape[1]*1.33)))),step):
        #     ssimCalls = ssimCalls + 1
        #     val = ssim( numpy.copy(spec[:,pt:(pt+tw)]) , pat, win_size=winSize)
        #     if val < 0:
        #         val = 0
        #     self.distances[pt+tw/2] = val
        if maxLoc[0]+tw<=currColumns:
            ssimCalls = ssimCalls + 1
            val = ssim( numpy.copy(spec[:,maxLoc[0]:(maxLoc[0]+tw)]) , pat, win_size=winSize)
            if val < 0:
                val=0
            self.distances[maxLoc[0]+tw/2] = val
        self.logs.write('------------------------- ssimCalls: '+str(ssimCalls)+'-------------------------')

    def getSpec(self):
        return self.spec

    def spectrogram(self):
        maxHertzInRec = float(self.rec.sample_rate)/2.0
        i = 0
        nfft = 512
        if self.rec.sample_rate <= 44100:
            while i<len(FREQUENCIES_44100) and FREQUENCIES_44100[i] <= maxHertzInRec:
                i = i + 1
            nfft = i
        start_time = time.time()

        Pxx, freqs, bins = mlab.specgram(self.rec.original, NFFT=nfft *2, Fs=self.rec.sample_rate , noverlap=nfft )
        if self.rec.sample_rate < 44100:
            self.rec.sample_rate = 44100
        dims =  Pxx.shape
        # if self.logs:
        #     self.logs.write("mlab.specgram --- seconds ---" + str(time.time() - start_time))

        i = 0
        j = 0
        start_time = time.time()
        while freqs[i] < self.low:
            j = j + 1
            i = i + 1

        #calculate decibeles in the passband
        Pxx =  10. * np.log10( Pxx.clip(min=0.0000000001))
        while (i < len(freqs)) and (freqs[i] < self.high):
            i = i + 1

        if i >= dims[0]:
            i = dims[0] - 1

        Z= Pxx[numpy.max([0, (j-2)]):numpy.min([(i+2), Pxx.shape[0]]),:]

        self.highIndex = dims[0]-j
        self.lowIndex = dims[0]-i

        if self.lowIndex < 0:
            self.lowIndex = 0

        if self.highIndex >= dims[0]:
            self.highIndex = dims[0] - 1
        i = 0
        j = 0
        if self.rec.sample_rate <= 44100:
            i = len(FREQUENCIES_44100) - 1
            j = i
            while FREQUENCIES_44100[i] > self.high and i>=0:
                j = j -1
                i = i -1

            while FREQUENCIES_44100[j] > self.low and j>=0:
                j = j -1
            self.speclow = len(FREQUENCIES_44100) - j - 2
            self.spechigh = len(FREQUENCIES_44100) - i - 2
            if self.speclow >= len(FREQUENCIES_44100):
                self.speclow = len(FREQUENCIES_44100)-1
            if self.spechigh < 0:
                self.spechigh = 0
        else:
            i = len(freqs) - 1
            j = i
            while freqs[i] > self.high and i>=0:
                j = j -1
                i = i -1

            while freqs[j] > self.low and j>=0:
                j = j -1
            self.speclow = len(freqs) - j - 2
            self.spechigh = len(freqs) - i - 2
            if self.speclow >= len(freqs):
                self.speclow = len(freqs)-1
            if self.spechigh < 0:
                self.spechigh = 0
        Z = np.flipud(Z)
        # if self.logs:
        #     self.logs.write("spec hi : %s spec low: %s" % (self.spechigh, self.speclow))
        #     self.logs.write('logs and flip ---' + str(time.time() - start_time))
        self.spec = Z

    def showVectAndSpec(self):
        ax1 = subplot(211)
        plot(self.distances)
        subplot(212, sharex=ax1)
        ax = gca()
        im = ax.imshow(self.spec, None)
        ax.axis('auto')
        show()
        close()

    def showSurface(self):
        imshow(self.matrixSurfacComp)
        show()
        close()

    # Function for resampling audio
    def rec_resample(self, newSampleRate):
        self.rec.original = resample_poly_filter(
            self.rec.original,
            self.rec.sample_rate,
            newSampleRate
        )
        self.rec.samples = len(self.rec.original)
        self.rec.sample_rate = newSampleRate
