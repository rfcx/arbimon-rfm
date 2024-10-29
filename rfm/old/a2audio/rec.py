import os
import time
import sys
import traceback
import urllib.request, urllib.error, urllib.parse
import http.client
import subprocess
import boto3
import numpy as np
import warnings
warnings.filterwarnings( "ignore", module = "matplotlib\..*" )
import soundfile as sf

config = {
    's3_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    's3_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
    's3_endpoint': os.getenv('S3_ENDPOINT')
}

encodings = {
    "pcms8": 8,
    "pcm16": 16,
    "pcm24": 32,
    "pcm32": 32,
    "pcmu8": 8,
    "float32": 32,
    "float64": 64,
    "ulaw": 16,
    "alaw": 16,
    "ima_adpcm": 16,
    "gsm610": 16,
    "dww12": 16,
    "dww16": 16,
    "dww24": 32,
    "g721_32": 32,
    "g723_24": 32,
    "vorbis": 16,
    "vox_adpcm": 16,
    "ms_adpcm": 16,
    "dpcm16": 16,
    "dpcm8": 8
}


class Rec:

    filename = ''
    samples = 0
    sample_rate = 0
    channs = 0
    status = 'NotProcessed'

    def __init__(self,
                 uri,
                 tempFolder,
                 bucketName,
                 logs=True,
                 removeFile=True,
                 test=False,
                 legacy=True):

        if type(uri) is not str and type(uri) is not str:
            raise ValueError("uri must be a string")
        if type(tempFolder) is not str:
            raise ValueError("invalid tempFolder")
        if not os.path.exists(tempFolder):
            raise ValueError("invalid tempFolder")
        elif not os.access(tempFolder, os.W_OK):
            raise ValueError("invalid tempFolder")
        if type(bucketName) is not str:
            raise ValueError("bucketName must be a string")
        if type(removeFile) is not bool:
            raise ValueError("removeFile must be a boolean")
        if type(test) is not bool:
            raise ValueError("test must be a boolean")
        start_time = time.time()
        self.legacy = legacy
        self.logs = logs
        self.localFiles = tempFolder
        self.bucket = bucketName
        self.uri = uri
        self.removeFile = removeFile
        self.original = []
        tempfilename = uri.split('/')
        self.filename = tempfilename[len(tempfilename) - 1]
        self.seed = "%d" % (sys.maxsize * np.random.rand(1))
        self.localfilename = self.localFiles + self.seed + '_' + self.filename.replace(' ', '_')
        if self.logs:
            print("init completed:" + str(time.time() - start_time))
            print('bucket: '+self.bucket+'\turi: '+self.uri)

        if not test:
            start_time = time.time()
            self.process()
            if self.logs:
                print("process completed:" +
                                str(time.time() - start_time))

    def process(self):
        start_time = time.time()
        if self.legacy and not self.getAudioFromLegacyUri():
            self.status = 'KeyNotFound'
            return None
        if not self.legacy and not self.getAudioFromUri():
            self.status = 'KeyNotFound'
            return None

        if self.logs:
            print("getAudioFromUri:" + str(time.time() - start_time))

        start_time = time.time()
        if not self.readAudioFromFile():
            self.status = 'CorruptedFile'
            return None
        if self.logs:
            print("readAudioFromFile:" +
                            str(time.time() - start_time))

        if not self.removeFiles():
            if self.logs:
                print("removeFiles: warning some files could not be removed")

        if self.channs > 1:
            self.original = np.mean(self.original, axis=-1)

        if self.samples == 0:
            self.status = 'NoData'
            return None

        if self.samples != len(self.original):
            self.status = 'CorruptedFileLength'
            return None

        self.status = 'HasAudioData'

    def getAudioFromUri(self):
        s3 = boto3.resource('s3', 
                            aws_access_key_id=config['s3_access_key_id'], 
                            aws_secret_access_key=config['s3_secret_access_key'],
                            endpoint_url=config['s3_endpoint'])
        b = s3.Bucket(self.bucket)
        try:
            b.download_file(self.uri, self.localfilename)
        except:
            print(("missing file. {} {}".format(self.bucket, self.uri)))
            return False
        return True

    def getAudioFromLegacyUri(self, retries=6):
        start_time = time.time()
        f = None
        url = 'https://s3.amazonaws.com/' + self.bucket + '/' + self.uri
        if self.logs:
            print(url + ' to ' + self.localfilename)
        retryCount = 0
        while not f and retryCount < retries:
            try:
                f = urllib.request.urlopen(url)
            except http.client.HTTPException as e:
                print((traceback.format_exc()))
                time.sleep(1.5**retryCount)  # exponential waiting
            except urllib.error.HTTPError as e:
                print((traceback.format_exc()))
                time.sleep(1.5**retryCount)  # exponential waiting
            except urllib.error.URLError as e:
                print((traceback.format_exc()))
                time.sleep(1.5**retryCount)  # exponential waiting
            retryCount += 1

        if f:
            if self.logs:
                print('urlopen success')
            try:
                with open(self.localfilename, "wb") as local_file:
                    local_file.write(f.read())
            except:
                if self.logs:
                    print('error f.read')
                return False
        else:
            if self.logs:
                print("url error. {}".format(traceback.format_exc()))
            return False

        if self.logs:
            print('f.read success')
            print("retrieve recording:" +
                            str(time.time() - start_time))
        return True

    def parseEncoding(self, enc_key):
        enc = 16
        if enc_key in encodings:
            enc = encodings[enc_key]
        return enc

    def readAudioFromFile(self):
        file_extension = self.filename.split('.')[-1]
        if file_extension == 'opus':
            process = subprocess.Popen(['opusdec', self.localfilename, self.localfilename+'.wav'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            os.remove(self.localfilename)
            self.localfilename = self.localfilename+'.wav'
            if self.logs:
                print('converted opus file: '+str(self.filename))
        elif file_extension == 'flac':
            command = ['/usr/bin/sox', self.localfilename, self.localfilename+'.wav']
            proc = subprocess.run(command, capture_output=True, text=True)
            # print('sox stdout:', proc.stdout)
            # print('sox stderr:', proc.stderr)
            os.remove(self.localfilename)
            self.localfilename = self.localfilename+'.wav'
            if self.logs:
                print('converted flac file: '+str(self.filename))

        try:
            s, fs = sf.read(self.localfilename)
            if self.logs:
                print(
                    "sampling rate = {} Hz, length = {} samples"
                    .format(fs, len(s)))
            self.bps = 16
            self.channs = 1
            self.samples = len(s)
            self.sample_rate = fs
            self.original = s
            self.status = 'AudioInBuffer'
            return True
        except Exception as e:
            if self.logs:
                print("error opening: " + self.filename)
                print("error:", e)
            return False

    def removeFiles(self):
        start_time = time.time()
        if self.removeFile:
            if os.path.isfile(self.localfilename):
                os.remove(self.localfilename)
            if self.logs:
                print("remove temporary file:", str(time.time() - start_time))
        return True

    def appendToOriginal(self, i):
        self.original.append(i)

    def getAudioFrames(self):
        return self.original

    def setLocalFileLocation(self, loc):
        self.localfilename = loc

    def getLocalFileLocation(self, ignore_not_exist=False):
        if ignore_not_exist:
            return self.localfilename
        else:
            if os.path.isfile(self.localfilename):
                return self.localfilename
            else:
                return None
