import time
import math
import json
import random
import numbers

import warnings
import cv2
import numpy

# from pylab import *
# from scipy.signal import *
import matplotlib

import scipy.stats
import skimage.measure

import a2.util.memoize
from a2audio.thresholder import Thresholder

class Recanalizer:
    def __init__(self, rec, speciesSurface, low, high, ssim=True, searchMatch=False):
        if not isinstance(speciesSurface, (numpy.ndarray, numpy.generic, numpy.memmap)):
            raise ValueError("speciesSurface must be a numpy.ndarray. Input was a "+str(type(speciesSurface)))
        if not isinstance(low, numbers.Number):
            raise ValueError("low must be a number")
        if not isinstance(high, numbers.Number):
            raise ValueError("high must be a number")
        if low >= high :
            raise ValueError("low must be less than high")

        self.ssim = ssim
        self.step = 32

        self.low = float(low)
        self.high = float(high)
        self.columns = speciesSurface.shape[1]
        self.speciesSurface = speciesSurface
        
        self.highIndex = 0
        self.lowIndex = 0
        self.speclow = 0
        self.spechigh = 0
        self.spec = None

        self.rec = rec

        self.searchMatch = searchMatch

        self.process()
    
    def process(self):
        self.spec = self.get_spectrogram()
        
        maxFreqInRec = float(self.rec.sample_rate)/2.0
        if self.high >= maxFreqInRec:
            raise ValueError("Surface high frequency bound ({}) is higher than recording's maximum frequency ({})".format(
                self.high,
                maxFreqInRec
            ))
        else:

            if self.spec.shape[1] < 2*self.speciesSurface.shape[1]:
                raise ValueError("Recording length {} is too short. (minimum length is twice the surface's length {})".format(
                    self.spec.shape[1],
                    2*self.speciesSurface.shape[1]
                ))

            self.featureVector_search()

    def getRec(self):
        return self.rec
    
    def get_vector(self ):
        return self.distances
    
    def get_features(self):
        if len(self.distances)<1:
            self.featureVector()
        N = len(self.distances)
        fvi = numpy.fft.fft(self.distances, n=2*N)
        acf = numpy.real( numpy.fft.ifft( fvi * numpy.conjugate(fvi) )[:N] )
        acf = acf/(N - numpy.arange(N))
        
        xf = abs(numpy.fft.fft(self.distances))

        fs = [ numpy.mean(xf), (max(xf)-min(xf)),
                max(xf), min(xf), numpy.std(xf), numpy.median(xf), scipy.stats.skew(xf),
                scipy.stats.kurtosis(xf),acf[0] ,acf[1] ,acf[2]]
        hist = scipy.stats.histogram(self.distances,6)[0]
        cfs = scipy.stats.cumfreq(self.distances,6)[0]
        ffs = [numpy.mean(self.distances), (max(self.distances)-min(self.distances)),
            max(self.distances), min(self.distances),
            numpy.std(self.distances), numpy.median(self.distances),scipy.stats.skew(self.distances),
            scipy.stats.kurtosis(self.distances),scipy.stats.moment(self.distances,1),scipy.stats.moment(self.distances,2),
            scipy.stats.moment(self.distances,3),scipy.stats.moment(self.distances,4),scipy.stats.moment(self.distances,5),
            scipy.stats.moment(self.distances,6),scipy.stats.moment(self.distances,7),scipy.stats.moment(self.distances,8),
            scipy.stats.moment(self.distances,9),scipy.stats.moment(self.distances,10),
            cfs[0],cfs[1],cfs[2],cfs[3],cfs[4],cfs[5],
            hist[0],hist[1],hist[2],hist[3],hist[4],hist[5],
            fs[0],fs[1],fs[2],fs[3],fs[4],fs[5],fs[6],fs[7],fs[8],fs[9],fs[10]
        ]
        return ffs
				
    def featureVector_search(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            self.matrixSurfacComp = numpy.copy(self.speciesSurface[self.spechigh:self.speclow,:])
            pat = self.matrixSurfacComp
            
            removeUnwanted = numpy.isnan(pat)
            if len(removeUnwanted) > 0  :
                pat[numpy.isnan(pat)] = numpy.nanmin(pat)

            spec = self.spec
            currColumns = self.spec.shape[1]

            spec = ((spec-numpy.min(numpy.min(spec)))/(numpy.max(numpy.max(spec))-numpy.min(numpy.min(spec))))*255
            spec = spec.astype('uint8')
            pat = ((pat-numpy.min(numpy.min(pat)))/(numpy.max(numpy.max(pat))-numpy.min(numpy.min(pat))))*255
            pat = pat.astype('uint8')

            th, tw = pat.shape[:2]
            
            result = cv2.matchTemplate(spec, pat, cv2.TM_CCOEFF_NORMED)

            self.distances = numpy.mean(result,axis=0) 
			
    def featureVector(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            self.distances = []
            currColumns = self.spec.shape[1]
            step = self.step

            self.matrixSurfacComp = numpy.copy(self.speciesSurface[self.spechigh:self.speclow,:])
            self.matrixSurfacComp[self.matrixSurfacComp[:,:]==-10000] = numpy.min(self.matrixSurfacComp[self.matrixSurfacComp != -10000])
            winSize = min(self.matrixSurfacComp.shape)
            winSize = min(winSize,7)
            if winSize %2 == 0:
                winSize = winSize - 1
            spec = self.spec
            if self.searchMatch:
                self.computeGFTT(numpy.copy(self.matrixSurfacComp),numpy.copy(spec),currColumns)
            else:
                if self.ssim:
                    for j in range(0,currColumns - self.columns,step):
                        val = skimage.measure.structural_similarity(numpy.copy(spec[: , j:(j+self.columns)]) , self.matrixSurfacComp , win_size=winSize)
                        if val < 0:
                            val = 0
                        self.distances.append(val)
                else:
                    threshold = Thresholder()
                    matrixSurfacCompCopy = threshold.apply(numpy.copy(self.matrixSurfacComp))
                    specCopy = threshold.apply(numpy.copy(spec))
                    maxnormforsize = numpy.linalg.norm( numpy.ones(shape=matrixSurfacCompCopy.shape) )
                    for j in range(0,currColumns - self.columns,step):
                        val = numpy.linalg.norm( numpy.multiply ( (specCopy[: , j:(j+self.columns)]), matrixSurfacCompCopy ) )/maxnormforsize
                        self.distances.append(  val )
 
    def computeGFTT(self,pat,spec,currColumns):
        currColumns = self.spec.shape[1]
        spec = ((spec-numpy.min(numpy.min(spec)))/(numpy.max(numpy.max(spec))-numpy.min(numpy.min(spec))))*255
        spec = spec.astype('uint8')
        self.distances = numpy.zeros(spec.shape[1])
        pat = ((pat-numpy.min(numpy.min(pat)))/(numpy.max(numpy.max(pat))-numpy.min(numpy.min(pat))))*255
        pat = pat.astype('uint8')

        th, tw = pat.shape[:2]
        result = cv2.matchTemplate(spec, pat, cv2.TM_CCOEFF)

        (_, _, minLoc, maxLoc) = cv2.minMaxLoc(result)

        threshold = numpy.percentile(result,98.5)
        loc = numpy.where(result >= threshold)
        winSize = min(pat.shape)
        winSize = min(winSize,7)
        step = 16
        if winSize %2 == 0:
            winSize = winSize - 1
        ssimCalls = 0

        xs = []
        s = -999
        for pt in zip(*loc[::-1]):
            if abs(pt[0] - s)>step/2 :
                xs.append(pt[0])
            s = pt[0]

        xs_smpl = [ xs[i] for i in sorted(random.sample(xrange(len(xs)), min(4,len(xs)))) ]

        for pts in xs_smpl:
            if pts+math.floor((pat.shape[1]*1.33))<=currColumns:
                for pt in range(max(0,pts-int(math.floor(pat.shape[1]/3))),min(currColumns - self.columns,pts+int(math.floor((pat.shape[1]*1.33)))),step):
                    ssimCalls = ssimCalls + 1
                    val = skimage.measure.structural_similarity( numpy.copy(spec[:,pt:(pt+tw)]) , pat, win_size=winSize)
                    if val < 0:
                        val = 0
                    self.distances[pt+tw/2] = val

        if maxLoc[0]+tw<=currColumns:
            ssimCalls = ssimCalls + 1
            val = skimage.measure.structural_similarity( numpy.copy(spec[:,maxLoc[0]:(maxLoc[0]+tw)]) , pat, win_size=winSize)
            if val < 0:
                val=0
            self.distances[maxLoc[0]+tw/2] = val

        
    def getSpec(self):
        return self.spec
    
    @a2.util.memoize.self_noargs
    def get_spectrogram(self):
        nfft = 512
        audio = self.rec.get_audio()
        if self.rec.sample_rate <= 44100:
            max_i = float(self.rec.sample_rate) / 44100.0 * 256.0
            nfft = min(256, max_i)

        Pxx, freqs, _ = matplotlib.mlab.specgram(audio, NFFT=nfft*2, Fs=self.rec.sample_rate , noverlap=nfft)
        if self.rec.sample_rate < 44100:
            self.rec.sample_rate = 44100

        dims =  Pxx.shape

        i = 0
        j = 0

        while freqs[i] < self.low:
            j = j + 1
            i = i + 1
        while (i < len(freqs)) and (freqs[i] < self.high):
            i = i + 1
 
        if i >= dims[0]:
            i = dims[0] - 1
        
        #calculate decibeles in the passband
        Pxx =  10. * numpy.log10( Pxx.clip(min=0.0000000001))

        Z = Pxx[(j-2):(i+2),:]
        
        self.highIndex = min(dims[0]-j, dims[0]-j)
        self.lowIndex = max(dims[0]-i, 0)
        
        i = 0
        j = 0
        if self.rec.sample_rate <= 44100:
            freqs = [(x+1) / 256.0 * 44100.0 / 2 for x in range(256)]

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

        Z = numpy.flipud(Z)

        self.spec = Z
        return Z
