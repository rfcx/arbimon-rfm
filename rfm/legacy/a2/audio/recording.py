import math
import os
import time
import sys
import tempfile
import warnings
from urllib import quote
import traceback
import urllib2
import httplib
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from scikits.audiolab import Sndfile, Format
import contextlib



import numpy
import matplotlib.mlab

import a2.runtime as runtime
import a2.util.memoize

class Recording(object):
    audio = None
    bps = 0
    channs = 0
    samples = 0
    sample_rate = 0
    
    def __init__(self, recid, uri=None):
        self.id = recid
        if uri:
            self.get_uri.val = uri
        

    @a2.util.memoize.self_noargs
    def get_uri(self):
        "returns the recoring's uri"
        return runtime.db.queryOne("""
            SELECT uri FROM recordings WHERE recording_id = %s
        """, [self.id])['uri']

    def get_name(self):
        "returns the recoring's name"
        return self.get_uri().split('/')[-1]

    @a2.util.memoize.self_noargs
    def get_audio(self):
        f = runtime.bucket.open_url(self.get_uri())
        with runtime.tmp.tmpfile() as tmpfile:
            fdata = f.read()
            tmpfile.file.write(fdata)
            tmpfile.close_file()
            
            with contextlib.closing(Sndfile(tmpfile.filename)) as f:
                self.bps = 16 #self.parseEncoding(f.encoding)
                self.channs = f.channels
                self.samples = f.nframes
                self.sample_rate = f.samplerate
                print("recording {}, ({} bps, {} channels, {} samples, {} sample rate)".format(
                    self.get_uri(),
                    self.bps,
                    self.channs,
                    self.samples,
                    self.sample_rate
                ))
                return f.read_frames(f.nframes,dtype=numpy.dtype('int'+str(self.bps)))
        
    def get_spectrogram(self, clip=None):
        audio  = self.get_audio()
        
        if clip:
            t0 = int(math.floor(float(clip[0]) * float(self.sample_rate)))
            t1 = int(math.floor(float(clip[1]) * float(self.sample_rate)))

            if t1 >= len(audio):
                t1 = len(audio) - 1

            audio = audio[t0:t1]

        nfft = 512
        targetrows = 512
        if self.sample_rate <= 44100:
            max_i = float(self.sample_rate) / 44100.0 * 256.0
            nfft = min(256, max_i)
            targetrows = 256

        Pxx, freqs, _ = matplotlib.mlab.specgram(audio, NFFT=nfft*2, Fs=self.sample_rate, noverlap=nfft)

        if self.sample_rate < 44100:
            self.sample_rate = 44100
            
        if clip:
            dims =  Pxx.shape
            for i in range(dims[0]):
                if clip[2] < freqs[i] < clip[3]:
                    Pxx[i,:] =  10. * numpy.log10(Pxx[i,:].clip(min=0.0000000001))
                else:
                    Pxx[i,:] = 0

            Z = numpy.flipud(Pxx[1:(Pxx.shape[0]-1),:])
            z = numpy.zeros(shape=(targetrows, Pxx.shape[1]))
            z[(targetrows-Pxx.shape[0]+1):(targetrows-1),:] = Z
            
            Pxx = z
        
        return Pxx
                    
