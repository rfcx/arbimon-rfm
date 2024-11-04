import os
import boto3
import csv
from contextlib import closing

from .roizer import Roizer
from ..a2audio.recanalizer import Recanalizer
from ..db import connect

config = {
    's3_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    's3_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
    's3_bucket_name': os.getenv('S3_BUCKET_NAME'),
    's3_legacy_bucket_name': os.getenv('S3_LEGACY_BUCKET_NAME'),
    's3_endpoint': os.getenv('S3_ENDPOINT')
}

def upload_file(local_path, key):
    s3 = boto3.resource('s3', aws_access_key_id=config['s3_access_key_id'], 
                        aws_secret_access_key=config['s3_secret_access_key'], endpoint_url=config['s3_endpoint'])
    bucket = s3.Bucket(config['s3_legacy_bucket_name'])
    bucket.upload_file(local_path, key, ExtraArgs={'ACL': 'public-read'})

def roigen(line,tempFolder,currDir,jobId,log=None):
    if log is not None:
        log.write('roizing recording: '+line[7])
    db = connect()
    if len(line) < 8:
        if log is not None:
            log.write('cannot roize : '+line[7])
        db.close()
        return 'err'
    recId = int(line[0])
    roispeciesId = int(line[1])
    roisongtypeId= int(line[2])
    initTime = float(line[3])
    endingTime = float(line[4])
    lowFreq = float(line[5])
    highFreq = float(line[6])
    recuri = line[7]
    legacy = line[8]
    bucketName = config['s3_legacy_bucket_name'] if legacy else config['s3_bucket_name']
    roi = Roizer(recuri,tempFolder,bucketName,initTime,endingTime,lowFreq,highFreq,legacy)

    with closing(db.cursor()) as cursor:
        cursor.execute('update `jobs` set `state`="processing", `progress` = `progress` + 1 ,last_update = now() where `job_id` = '+str(jobId))
        db.commit()

    if 'HasAudioData' not in roi.status:
        with closing(db.cursor()) as cursor:
            cursor.execute('INSERT INTO `recordings_errors` (`recording_id`, `job_id`) VALUES ('+str(recId)+','+str(jobId)+') ')
            db.commit()
        db.close()
        if log is not None:
            log.write('cannot roize : '+line[7])
            log.write(roi.status)
        return 'err'
    else:            
        db.close()
        if log is not None:
            log.write('done roizing: '+line[7])
        return [roi,str(roispeciesId)+"_"+str(roisongtypeId)]

def recnilize(line,workingFolder,currDir,jobId,pattern,log=None,ssim=True,searchMatch=False):
    if log is not None:
        log.write('recnilizing recording: '+line[0])
    recId = int(line[5])
    db = connect()
    pid = None
    with closing(db.cursor()) as cursor:
        cursor.execute('update `jobs` set `state`="processing", `progress` = `progress` + 1 ,last_update = now() where `job_id` = '+str(jobId))
        db.commit()
    with closing(db.cursor()) as cursor:
        cursor.execute('SELECT `project_id` FROM `jobs` WHERE `job_id` =  '+str(jobId))
        rowpid = cursor.fetchone()
        pid = rowpid[0]
    if pid is None:
        if log is not None:
            log.write('cannot recnilize '+line[0])
        return 'err project not found'
    bucketBase = 'project_' + str(pid) + '/training_vectors/job_' + str(jobId) + '/'
    legacy = line[6]
    recBucketName = config['s3_legacy_bucket_name'] if legacy else config['s3_bucket_name']
    recAnalized = Recanalizer(line[0], pattern[0], pattern[2], pattern[3], workingFolder,
                              recBucketName, log, False, ssim, searchMatch, modelSampleRate=pattern[1], db=db,
                              rec_id=recId, job_id=jobId,
                              legacy=legacy)
    if recAnalized.status == 'Processed':
        recName = line[0].split('/')
        recName = recName[len(recName)-1]
        vectorUri = bucketBase+recName
        fets = recAnalized.features()
        vector = recAnalized.getVector()
        vectorFile = workingFolder+recName
        myfileWrite = open(vectorFile, 'w')
        wr = csv.writer(myfileWrite)
        wr.writerow(vector)
        myfileWrite.close()
        upload_file(vectorFile, vectorUri)
        info = []
        info.append(line[4])
        info.append(line[3])
        info.append(pattern[4])
        info.append(pattern[2])
        info.append(pattern[3])
        info.append(pattern[1])
        info.append(line[0])
        db.close()
        if log is not None:
            log.write('done recnilizing: '+line[0])
        return {'fets':fets,'info':info}
    else:
        if log is not None:
            log.write('cannot recnilize '+line[0])
            log.write(recAnalized.status)
        db.close()
        return 'err ' + recAnalized.status
