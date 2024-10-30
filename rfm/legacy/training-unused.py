#! .env/bin/python

import time
import sys
import tempfile
import os
import csv
import subprocess
import boto
import shutil
import MySQLdb
import json
from boto.s3.connection import S3Connection
from contextlib import closing
from a2pyutils.config import EnvironmentConfig


USAGE = """Runs a model training job.
{prog} job_id
    job_id - job id in database
""".format(prog=sys.argv[0])


if len(sys.argv) < 2:
    print USAGE
    sys.exit(-1)


configuration = EnvironmentConfig()
config = configuration.data()


try:
    db = MySQLdb.connect(
        host=config[0], user=config[1],
        passwd=config[2], db=config[3]
    )
except MySQLdb.Error as e:
    print "# fatal error cannot connect to database."
    sys.exit(-1)


jobId = int(sys.argv[1].strip("'"))
modelName = ''
project_id = -1

currDir = os.path.dirname(os.path.abspath(__file__))
currPython = sys.executable

bucketName = config[4]
awsKeyId = config[5]
awsKeySecret = config[6]

print 'started'
sys.stdout.flush()


with closing(db.cursor()) as cursor:
    cursor.execute("""
        SELECT J.`project_id`, J.`user_id`,
            JP.model_type_id, JP.training_set_id,
            JP.validation_set_id, JP.trained_model_id,
            JP.use_in_training_present,
            JP.use_in_training_notpresent,
            JP.use_in_validation_present,
            JP.use_in_validation_notpresent,
            JP.name
        FROM `jobs` J
        JOIN `job_params_training` JP ON JP.job_id = J.job_id
        WHERE J.`job_id` = %s
    """, [jobId])
    row = cursor.fetchone()

if not row:
    print "Could not find training job #{}".format(jobId)
    sys.exit(-1)


(
    project_id, user_id,
    model_type_id, training_set_id,
    validation_set_id, trained_model_id,
    use_in_training_present,
    use_in_training_notpresent,
    use_in_validation_present,
    use_in_validation_notpresent,
    name
) = row
modelName = name
tempFolders = str(configuration.pathsConfig['temp_dir'])

# select the model_type by its id
if model_type_id == 1:  # Pattern Matching (modified Alvarez thesis)
    progress_steps = 0
    # creating a temporary folder
    workingFolder = tempFolders+"/training_"+str(jobId)
    if os.path.exists(workingFolder):
        shutil.rmtree(workingFolder)
    os.makedirs(workingFolder)
    with closing(db.cursor()) as cursor:
        # create training file
        cursor.execute("""
            SELECT r.`recording_id`, ts.`species_id`, ts.`songtype_id`,
                ts.`x1`, ts.`x2`, ts.`y1`, ts.`y2`, r.`uri`
            FROM `training_set_roi_set_data` ts, `recordings` r
            WHERE r.`recording_id` = ts.`recording_id`
              AND ts.`training_set_id` = %s
        """, [training_set_id])
        db.commit()
        trainingFileName = os.path.join(
            workingFolder,
            'training_{}_{}.csv'.format(jobId, training_set_id)
        )
        # write training file to temporary folder
        with open(trainingFileName, 'wb') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',')
            numTrainingRows = int(cursor.rowcount)
            progress_steps = numTrainingRows
            for x in range(0, numTrainingRows):
                rowTraining = cursor.fetchone()
                spamwriter.writerow(rowTraining[0:7+1] + (jobId,))

        cursor.execute("""
            SELECT DISTINCT `recording_id`
            FROM `training_set_roi_set_data`
            where `training_set_id` = %s
        """, [training_set_id])
        db.commit()

        numrecordingsIds = int(cursor.rowcount)
        recordingsIds = []
        for x in range(0, numrecordingsIds):
            rowRec = cursor.fetchone()
            recordingsIds.append(rowRec[0])

        cursor.execute("""
            SELECT DISTINCT `species_id`, `songtype_id`
            FROM `training_set_roi_set_data`
            WHERE `training_set_id` = %s
        """, [training_set_id])
        db.commit()

        numSpeciesSongtype = int(cursor.rowcount)
        speciesSongtype = []
        for x in range(0, numSpeciesSongtype):
            rowSpecies = cursor.fetchone()
            speciesSongtype.append([rowSpecies[0], rowSpecies[1]])

    validationFile = workingFolder+'/validation_'+str(jobId)+'.csv'
    with open(validationFile, 'wb') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for x in range(0, numSpeciesSongtype):
            spst = speciesSongtype[x]
            with closing(db.cursor()) as cursor:
                cursor.execute("""
                    SELECT r.`uri` , `species_id` , `songtype_id` , `present`
                    FROM `recording_validations` rv, `recordings` r
                    WHERE r.`recording_id` = rv.`recording_id`
                      AND rv.`project_id` = %s
                      AND `species_id` = %s
                      AND `songtype_id` = %s
                """, [project_id, spst[0], spst[1]])
                # is this condition too harsh?
                # and r.`recording_id` NOT IN ("+ ','.join([str(x)
                # for x in recordingsIds]) +") " +

                db.commit()

                numValidationRows = int(cursor.rowcount)

                progress_steps = progress_steps + numValidationRows

                for x in range(0, numValidationRows):
                    rowValidation = cursor.fetchone()
                    spamwriter.writerow(rowValidation[0:4])

    # get Amazon S3 bucket
    conn = S3Connection(awsKeyId, awsKeySecret)
    bucket = conn.get_bucket(bucketName)
    valiKey = 'project_{}/validations/job_{}.csv'.format(project_id, jobId)

    # save validation file to bucket
    k = bucket.new_key(valiKey)
    k.set_contents_from_filename(validationFile)

    # save validation to DB
    progress_steps = progress_steps + 15
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            INSERT INTO `validation_set`(
                `validation_set_id`, `project_id`, `user_id`, `name`, `uri`,
                `params`, `job_id`
            ) VALUES (
                NULL, %s, %s, %s, %s, %s, %s
            )
        """, [
            project_id, user_id, modelName+" validation", valiKey,
            json.dumps({'name': modelName}),
            jobId
        ])
        db.commit()

        cursor.execute("""
            UPDATE `job_params_training`
            SET `validation_set_id` = %s
            WHERE `job_id` = %s
        """, [cursor.lastrowid, jobId])
        db.commit()

        cursor.execute("""
            UPDATE `jobs`
            SET `progress_steps` = %s, progress=0, state="processing"
            WHERE `job_id` = %s
        """, [progress_steps, jobId])
        db.commit()

    # start the job
    # use the pipe (mapreduce like)
    print 'started pipe'
    sys.stdout.flush()

    p1 = subprocess.Popen(
        ['/bin/cat', trainingFileName], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(
        [currPython, currDir + '/audiomapper/trainMap.py'],
        stdin=p1.stdout, stdout=subprocess.PIPE)
    p3 = subprocess.Popen(
        [currPython, currDir + '/audiomapper/roigen.py'],
        stdin=p2.stdout, stdout=subprocess.PIPE)
    p4 = subprocess.Popen(
        [currPython, currDir + '/audiomapper/align.py'],
        stdin=p3.stdout, stdout=subprocess.PIPE)
    p5 = subprocess.Popen(
        [currPython, currDir + '/audiomapper/recnilize.py'],
        stdin=p4.stdout, stdout=subprocess.PIPE)
    p6 = subprocess.Popen(
        [currPython, currDir + '/audiomapper/modelize.py'],
        stdin=p5.stdout, stdout=subprocess.PIPE)

    print p6.communicate()[0].strip('\n')
    sys.stdout.flush()
    # update job progress

else:
    print "Unkown model type requested\n"

with closing(db.cursor()) as cursor:
    cursor.execute("""
        UPDATE `jobs`
        SET `last_update`=now()
        WHERE `job_id` = %s
    """, [jobId])
    db.commit()

db.close()
