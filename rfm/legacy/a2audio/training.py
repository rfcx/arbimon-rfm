import os
from contextlib import closing
from .roizer import Roizer
from ..db import connect

config = {
    's3_bucket_name': os.getenv('S3_BUCKET_NAME'),
    's3_legacy_bucket_name': os.getenv('S3_LEGACY_BUCKET_NAME')
}

def roigen(line,tempFolder,currDir ,jobId,log=None):
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
            log.write('done roizing : '+line[7])
        return [roi,str(roispeciesId)+"_"+str(roisongtypeId)]
   