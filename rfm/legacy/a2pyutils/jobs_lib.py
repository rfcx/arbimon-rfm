import os
from contextlib import closing
import shutil
from soundscape.set_visual_scale_lib import exit_error
from boto.s3.connection import S3Connection

def  get_model_type():
    pass

def cancelStatus(db,jobId,rmFolder=None,quitj=True):
    status = None
    with closing(db.cursor()) as cursor:
        cursor.execute('select `cancel_requested` from`jobs`  where `job_id` = '+str(jobId))
        db.commit()
        status = cursor.fetchone()
        if status:
            if 'cancel_requested' in status:
                status = status['cancel_requested']
            else:
                status  = status[0]
        else:
            return False
        if status and int(status) > 0:
            cursor.execute('update `jobs` set `state`="canceled" where `job_id` = '+str(jobId))
            db.commit()
            print 'job canceled'
            if rmFolder:
                if os.path.exists(rmFolder):
                    shutil.rmtree(rmFolder)
            if quitj:
                quit()
            else:
                return True
        else:
            return False

def upload_files_2bucket(config,files,log,jobId,db,workingFolder):
    bucket = None
    log.write('starting bucket upload')
    try:
        bucketName = config[4]
        awsKeyId = config[5]
        awsKeySecret = config[6]
        conn = S3Connection(awsKeyId, awsKeySecret)
        bucket = conn.get_bucket(bucketName)
    except:
        exit_error('cannot initiate bucket connection',-1,log,jobId,db,workingFolder)
        
    try:
        for k in files:
            fileu = files[k]
            bk = bucket.new_key(fileu['key'])
            bk.set_contents_from_filename(fileu['file'])
            if fileu['public']:
                bk.set_acl('public-read')
    except:
        exit_error('error uploading files to bucket',-1,log,jobId,db,workingFolder)
    log.write('files uploaded to bucket')
