from pylab import *
from matplotlib import *
import numpy
import math
from scipy.signal import spectrogram

from .constants import FREQUENCIES_44100
from .rec import Rec


class Roizer:

    def __init__(self, uri, tempFolder, bucketName, iniSecs=5, endiSecs=15, lowFreq = 1000, highFreq = 2000, legacy=True):
        
        if type(uri) is not str and type(uri) is not unicode:
            raise ValueError("uri must be a string")
        if type(tempFolder) is not str:
            raise ValueError("invalid tempFolder")
        if not os.path.exists(tempFolder):
            raise ValueError("invalid tempFolder")
        elif not os.access(tempFolder, os.W_OK):
            raise ValueError("invalid tempFolder")
        if type(bucketName) is not str:
            raise ValueError("bucketName must be a string")
        if type(iniSecs) is not int and  type(iniSecs) is not float:
            raise ValueError("iniSecs must be a number")
        if type(endiSecs) is not int and  type(endiSecs) is not float:
            raise ValueError("endiSecs must be a number")
        if type(lowFreq) is not int and  type(lowFreq) is not float:
            raise ValueError("lowFreq must be a number")
        if type(highFreq) is not int and  type(highFreq) is not float:
            raise ValueError("highFreq must be a number")
        if iniSecs>=endiSecs:
            raise ValueError("iniSecs must be less than endiSecs")
        if lowFreq>=highFreq :
            raise ValueError("lowFreq must be less than highFreq")
        self.spec = None
        recording = Rec(uri,tempFolder,bucketName,None,legacy=legacy)

        if  'HasAudioData' in recording.status:
            self.original = recording.original
            self.sample_rate = recording.sample_rate
            self.recording_sample_rate = recording.sample_rate
            self.channs = recording.channs
            self.samples = recording.samples
            self.status = 'HasAudioData'
            self.iniT = iniSecs
            self.endT = endiSecs
            self.lowF = lowFreq
            self.highF = highFreq 
            self.uri = uri
        else:
            self.status = recording.status
            return None
        dur = float(self.samples)/float(self.sample_rate)
        if dur < endiSecs:
            raise ValueError("endiSecs greater than recording duration")
        
        if  'HasAudioData' in self.status:
            self.spectrogram()

    def getAudioSamples(self):
        return self.original
    
    def getSpectrogram(self):
        if self.spec is not None:
             self.spectrogram()
        return self.spec
    
    def spectrogram(self):
        
        initSample = int(math.floor(float((self.iniT)) * float((self.sample_rate))))
        endSample = int(math.floor(float((self.endT)) * float((self.sample_rate))))
        if endSample >= len(self.original):
           endSample = len(self.original) - 1

        maxHertzInRec = float(self.sample_rate)/2.0
        nfft = 512
        targetrows = 512
        if self.sample_rate <= 44100:
            i = 0
            while i<len(FREQUENCIES_44100) and FREQUENCIES_44100[i] <= maxHertzInRec :
                i = i + 1
            nfft = i
            targetrows = len(FREQUENCIES_44100)
        data = self.original[initSample:endSample]
        f, t, Sxx = spectrogram(
                data,
                fs=self.sample_rate,
                nperseg=nfft*2,
                noverlap=nfft,
                window='hann',
                scaling='spectrum',
                mode='magnitude',
            )

        # Ensure compatibility with 44100 Hz sample rate
        if self.sample_rate < 44100:
            self.sample_rate = 44100

        # Filter frequencies below lowF and above highF
        i = 0
        while f[i] < self.lowF:
            Sxx[i, :] = 0
            i += 1

        # Convert power to decibels in the passband
        while f[i] < self.highF:
            Sxx[i, :] = 10.0 * numpy.log10(Sxx[i, :].clip(min=1e-10)) + 38.0
            i += 1

        # Filter out frequencies above the desired range
        while i < Sxx.shape[0]:
            Sxx[i, :] = 0
            i += 1

        # Flip and pad spectrogram to match desired target rows
        Z = numpy.flipud(Sxx[1:(Sxx.shape[0] - 1), :])
        z = numpy.zeros(shape=(targetrows, Sxx.shape[1]))
        z[(targetrows - Sxx.shape[0] + 1):(targetrows - 1), :] = Z

        self.spec = z
        
