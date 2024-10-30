import MySQLdb
from a2audio.roizer import Roizer
from contextlib import closing
from boto.s3.connection import S3Connection
from a2audio.recanalizer import Recanalizer
import csv
import sys
from a2pyutils.config import EnvironmentConfig

config = EnvironmentConfig().data()


def roigen(line,tempFolder,currDir ,jobId,log=None):
    if log is not None:
        log.write('roizing recording: '+line[7])
    db = MySQLdb.connect(host=config[0], user=config[1], passwd=config[2],db=config[3])
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
    bucketName = config[4] if legacy else config[7]
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
            log.write('done roizing : '+line[7])
        return [roi,str(roispeciesId)+"_"+str(roisongtypeId)]
    
def recnilize(line,workingFolder,currDir,jobId,pattern,log=None,ssim=True,searchMatch=False):
    if log is not None:
        log.write('analizing recording: '+line[0])
    bucketName = config[4]
    awsKeyId = config[5]
    awsKeySecret = config[6]
    recId = int(line[5])
    db = MySQLdb.connect(host=config[0], user=config[1], passwd=config[2], db=config[3])
    conn = S3Connection(awsKeyId, awsKeySecret)
    bucket = conn.get_bucket(bucketName)
    pid = None
    with closing(db.cursor()) as cursor:
        cursor.execute('update `jobs` set `state`="processing", `progress` = `progress` + 1 ,last_update = now() where `job_id` = '+str(jobId))
        db.commit()
    with closing(db.cursor()) as cursor:
        cursor.execute('SELECT `project_id` FROM `jobs` WHERE `job_id` =  '+str(jobId))
        db.commit()
        rowpid = cursor.fetchone()
        pid = rowpid[0]
    if pid is None:
        if log is not None:
            log.write('cannot analize '+line[0])
        return 'err project not found'
    bucketBase = 'project_' + str(pid) + '/training_vectors/job_' + str(jobId) + '/'
    legacy = line[6]
    recBucketName = bucketName if legacy else config[7]
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
        myfileWrite = open(vectorFile, 'wb')
        wr = csv.writer(myfileWrite)
        wr.writerow(vector)
        myfileWrite.close()
        k = bucket.new_key(vectorUri)
        k.set_contents_from_filename(vectorFile)
        k.set_acl('public-read')
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
            log.write('done analizing '+line[0])
        return {'fets':fets,'info':info}
    else:
        if log is not None:
            log.write('cannot analize '+line[0])
            log.write(recAnalized.status)
        db.close()
        return 'err ' + recAnalized.status
