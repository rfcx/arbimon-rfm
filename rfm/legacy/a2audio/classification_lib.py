from a2pyutils.logger import Logger
from a2pyutils.config import EnvironmentConfig
from a2audio.recanalizer import Recanalizer
from a2pyutils.jobs_lib import cancelStatus
from soundscape.set_visual_scale_lib import *
import time
import MySQLdb
import contextlib
import tempfile
import shutil
import os
import traceback
import multiprocessing
from joblib import Parallel, delayed
import cPickle as pickle
import csv
import json
import sys

classificationCanceled =False

def get_classification_job_data(db,jobId):
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                SELECT J.`project_id`, J.`user_id`,
                    JP.model_id, JP.playlist_id,
                    JP.name , J.ncpu
                FROM `jobs` J
                JOIN `job_params_classification` JP ON JP.job_id = J.job_id
                WHERE J.`job_id` = %s
            """, [jobId])
            row = cursor.fetchone()
    except:
        exit_error("Could not query database with classification job #{}, {}".format(jobId, traceback.format_exc()))
    if not row:
        exit_error("Could not find classification job #{}, {}".format(jobId, traceback.format_exc()))
    return [row['model_id'],row['project_id'],row['user_id'],row['name'],row['playlist_id'],row['ncpu']]

def get_model_params(db,classifierId,log):
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                SELECT m.`model_type_id`,m.`uri`,ts.`species_id`,ts.`songtype_id`
                FROM `models`m ,`training_sets_roi_set` ts
                WHERE m.`training_set_id` = ts.`training_set_id`
                  AND `model_id` = %s
            """, [classifierId])
            db.commit()
            numrows = int(cursor.rowcount)
            if numrows < 1:
                exit_error('fatal error cannot fetch model params (classifier_id:{}) {}'.format(classifierId, traceback.format_exc()),-1,log)
            row = cursor.fetchone()
    except:
        exit_error("Could not query database for model params {}".format(traceback.format_exc()))
    return {
        'id': classifierId,
        'model_type_id': row['model_type_id'],
        'uri': row['uri'],
        'species': row['species_id'],
        'songtype': row['songtype_id'],
    }


def create_temp_dir(jobId,log):
    try:
        tempFolders = tempfile.gettempdir()
        workingFolder = tempFolders+"/job_"+str(jobId)+'/'
        if os.path.exists(workingFolder):
            shutil.rmtree(workingFolder)
        os.makedirs(workingFolder)
    except:
        exit_error("Could not create temporary directory, {}".format(traceback.format_exc()))
    if not os.path.exists(workingFolder):
        exit_error('fatal error creating directory, {}'.format(traceback.format_exc()),-1,log)
    return workingFolder

def get_playlist(db,playlistId,log):
    try:
        recsToClassify = []
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                SELECT r.`recording_id`, r.`uri`, IF(LEFT(r.uri, 8) = 'project_', 1, 0) legacy
                FROM `recordings` r JOIN `playlist_recordings` pr ON r.`recording_id` = pr.`recording_id`
                WHERE pr.`playlist_id` = %s
            """, [playlistId])
            db.commit()
            numrows = int(cursor.rowcount)
            for x in range(0, numrows):
                rowclassification = cursor.fetchone()
                recsToClassify.append(rowclassification)
    except:
        exit_error("Could not generate playlist array, {}".format(traceback.format_exc()))
    if len(recsToClassify) < 1:
        exit_error('No recordngs in playlist, {}'.format(traceback.format_exc()),-1,log)
    return recsToClassify

def set_progress_params(db,progress_steps, jobId):
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                UPDATE `jobs`
                SET `progress_steps`=%s, progress=0, state="processing"
                WHERE `job_id` = %s
            """, [progress_steps*2+5, jobId])
            db.commit()
    except:
        exit_error("Could not set progress params, {}".format(traceback.format_exc()))

def insert_rec_error(db, recId, jobId):
    error = traceback.format_exc()
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                INSERT INTO `recordings_errors`(`recording_id`, `job_id`, `error`)
                VALUES (%s, %s, %s)
            """, [recId, jobId, error])
            db.commit()
    except:
        exit_error("Could not insert recording error, {}.\n\n ORIGINAL ERROR: {}".format(traceback.format_exc(), error))


def classify_rec(rec, model_specs, workingFolder, log, config, jobId):
    global classificationCanceled
    if classificationCanceled:
        return None
    errorProcessing = False
    db = get_db(config)
    if cancelStatus(db,jobId,workingFolder,False):
        classificationCanceled = True
        quit()
    recAnalized = None
    model_data = model_specs['data']
    clfFeatsN = model_data[0].n_features_
    log.write('classify_rec try')
    try:
        useSsim = True
        oldModel = False
        useRansac = False
        bIndex = 0
        if len(model_data) > 7:
            bIndex  =  model_data[7]
        if len(model_data) > 6:
            useRansac =  model_data[6]
        if len(model_data) > 5:
            useSsim =  model_data[5]
        else:
            oldModel = True
        bucketName = config[4] if rec['legacy'] else config[7]
        recAnalized = Recanalizer(rec['uri'],
                                  model_data[1],
                                  float(model_data[2]),
                                  float(model_data[3]),
                                  workingFolder,
                                  bucketName,
                                  log,
                                  False,
                                  useSsim,
                                  modelSampleRate=model_specs['sample_rate'],
                                  legacy=rec['legacy'])
        log.write('recAnalized {}'.format(recAnalized.status))
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                UPDATE `jobs`
                SET `progress` = `progress` + 1, last_update = NOW()
                WHERE `job_id` = %s
            """, [jobId])
            db.commit()
    except:
        errorProcessing = True
        log.write('error rec analyzed {} '.format(traceback.format_exc()))
    log.write('finish')
    featvector = None
    fets = None
    if recAnalized.status == 'Processed':
        try:
            featvector = recAnalized.getVector()
            fets = recAnalized.features()
        except:
            errorProcessing = True
            log.write('error getting feature vectors {} '.format(traceback.format_exc()))
    else:
        errorProcessing = True
    res = None
    log.write('FEATS COMPUTED')
    if featvector is not None:
        try:
            clf = model_data[0]
            res = clf.predict([fets])
        except:
            errorProcessing = True
            log.write('error predicting {} '.format(traceback.format_exc()))
    else:
        errorProcessing = True
    if errorProcessing:
        insert_rec_error(db,rec['recording_id'],jobId)
        db.close()
        return None
    else:
        log.write('done processing this rec')
        db.close()
        return {'uri':rec['uri'],'id':rec['recording_id'],'f':featvector,'ft':fets,'r':res[0]}


def get_model(db, model_specs, config, log, workingFolder):
    log.write('reaching bucket.')
    modelLocal = workingFolder+'model.mod'
    bucket = get_bucket(config)
    try:
        log.write('getting aws file key...')
        k = bucket.get_key(model_specs['uri'], validate=False)
        log.write('contents to filename...')
        k.get_contents_to_filename(modelLocal)

    except:
        exit_error('fatal error model {} not found in aws, {}'.format(model_specs['uri'], traceback.format_exc()), -1, log)

    log.write('model in local file system.')
    model_specs['model'] = None

    log.write('loading model to memory...')
    if os.path.isfile(modelLocal):
        model_data = pickle.load(open(modelLocal, "rb"))
        if isinstance(model_data, dict):
            # future model formats (they should be pickled as a dict)
            model_specs = model_data
        else:
            # current style models (they're pickled as a list)
            model_specs['data'] = model_data
    else:
        exit_error('fatal error cannot load model, {}'.format(traceback.format_exc()), -1, log)
    log.write('model was loaded to memory.')
    log.write('model #%d for species %s songtype %s. template shape is %s, with frequencies from %s to %s' % (
        model_specs['id'],
        model_specs['species'],
        model_specs['songtype'],
        model_specs['data'][1].shape, float(model_specs['data'][2]), float(model_specs['data'][3])
    ))

    if "sample_rate" not in model_specs:
        log.write('sampling rate not specified in model. searching training data for sampling rate...')
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                SELECT R.sample_rate
                FROM models as M
                JOIN training_set_roi_set_data AS TSRSD ON M.training_set_id = TSRSD.training_set_id
                JOIN recordings AS R ON R.recording_id = TSRSD.recording_id
                WHERE M.model_id = %s
                AND TSRSD.species_id = %s
                AND TSRSD.songtype_id = %s
                LIMIT 1
            """, [
                model_specs['id'],
                model_specs['species'],
                model_specs['songtype'],
            ])
            model_specs["sample_rate"] = cursor.fetchone()["sample_rate"]
        log.write('model sampling rate is {}'.format(model_specs["sample_rate"]))

    return model_specs

def write_vector(recUri,tempFolder,featvector):
    print 'write_vector', recUri,tempFolder,featvector
    vectorLocal = None
    try:
        recName = recUri.split('/')
        recName = recName[len(recName)-1]
        vectorLocal = tempFolder+recName+'.vector'
        myfileWrite = open(vectorLocal, 'wb')
        wr = csv.writer(myfileWrite)
        wr.writerow(featvector)
        myfileWrite.close()
    except:
        print 'ERROR:: {}'.format(traceback.format_exc())
        return None
    return vectorLocal

def upload_vector(uri,filen,config,rid,db,jobId):
    try:
        bucket = get_bucket(config)
        k = bucket.new_key(uri)
        k.set_contents_from_filename(filen)
        k.set_acl('public-read')
        os.remove(filen)
    except:
        insert_rec_error(db, rid, jobId)

def insert_result_to_db(config,jId, recId, species, songtype, presence, maxV):
    db = None
    try:
        db = get_db(config)
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                INSERT INTO `classification_results` (
                    job_id, recording_id, species_id, songtype_id, present,
                    max_vector_value
                ) VALUES (%s, %s, %s, %s, %s,
                    %s
                )
            """, [jId, recId, species, songtype, presence, maxV])
            db.commit()
    except:
        insert_rec_error(db, recId, jId)
    db.close()

def processResults(res,workingFolder,config,modelUri,jobId,species,songtype,db, log):
    minVectorVal = 9999999.0
    maxVectorVal = -9999999.0
    processed = 0
    try:
        for r in res:
            with contextlib.closing(db.cursor()) as cursor:
                cursor.execute("""
                    UPDATE `jobs`
                    SET `progress` = `progress` + 1, last_update = NOW()
                    WHERE `job_id` = %s
                """, [jobId])
                db.commit()
            if r and 'id' in r:
                processed = processed + 1
                recName = r['uri'].split('/')
                recName = recName[len(recName)-1]
                localFile = write_vector(r['uri'],workingFolder,r['f'])
                if localFile is not None:
                    maxv = max(r['f'])
                    minv = min(r['f'])
                    if minVectorVal > float(minv):
                        minVectorVal = minv
                    if maxVectorVal < float(maxv):
                        maxVectorVal = maxv
                    vectorUri = '{}/classification_{}_{}.vector'.format(
                            modelUri.replace('.mod', ''), jobId, recName
                    )
                    upload_vector(vectorUri,localFile,config,r['id'],db,jobId)
                    log.write("inserting results from {rid} for {sp} {st} into the database ({r}, maxv:{maxv})".format(
                        rid=r['id'],
                        r=r['r'],
                        sp=species,
                        st=songtype,
                        maxv=maxv
                    ))
                    insert_result_to_db(config,jobId,r['id'], species, songtype,r['r'],maxv)
                else:
                    log.write('localFile is None')
                    insert_rec_error(db, r['id'], jobId)
    except:
        exit_error('cannot process results. {}'.format(traceback.format_exc()))
    return {"t":processed,"stats":{"minv": float(minVectorVal), "maxv": float(maxVectorVal)}}

def run_pattern_matching(jobId, model_specs, playlistId, log, config, ncpu):
    global classificationCanceled
    db = None
    try:
        db = get_db(config)
        num_cores = multiprocessing.cpu_count()
        if int(ncpu)>0:
            num_cores = int(ncpu)
        log.write('using Pattern Matching algorithm' )
        workingFolder = create_temp_dir(jobId,log)
        log.write('created working directory.')
        recsToClassify = get_playlist(db,playlistId,log)
        log.write('playlist generated.')
        cancelStatus(db,jobId,workingFolder)
        set_progress_params(db,len(recsToClassify), jobId)
        log.write('job progress set to start.')
        model_specs = get_model(db, model_specs, config, log, workingFolder)
        cancelStatus(db,jobId,workingFolder)
        log.write('model was fetched.')
    except:
        log.write('ERROR:: {}'.format(traceback.format_exc()))
        return False
    log.write('starting parallel for.')
    db.close()
    try:
        resultsParallel = Parallel(n_jobs=num_cores)(
            delayed(classify_rec)(rec, model_specs, workingFolder, log, config, jobId) for rec in recsToClassify
        )
    except:
        log.write('ERROR:: {}'.format(traceback.format_exc()))
        if classificationCanceled:
            log.write('job cancelled')
        return False
    log.write('done parallel execution.')
    db = get_db(config)
    cancelStatus(db,jobId,workingFolder)
    try:
        jsonStats = processResults(resultsParallel, workingFolder, config, model_specs['uri'], jobId, model_specs['species'], model_specs['songtype'], db, log)
    except:
        log.write('ERROR:: {}'.format(traceback.format_exc()))
        return False
    log.write('computed stats.')
    shutil.rmtree(workingFolder)
    log.write('removed folder.')
    statsJson = jsonStats['stats']
    if jsonStats['t'] < 1:
        exit_error('no recordings processed. {}'.format(traceback.format_exc()))
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                INSERT INTO `classification_stats` (`job_id`, `json_stats`)
                VALUES (%s, %s)
            """, [jobId, json.dumps(statsJson)])
            db.commit()
            cursor.execute("""
                UPDATE `jobs`
                SET `progress` = `progress_steps`, `completed` = 1,
                    state="completed", `last_update` = now()
                WHERE `job_id` = %s
            """, [jobId])
            db.commit()
        db.close()
        return True
    except:
        db.close()
        log.write('ERROR:: {}'.format(traceback.format_exc()))
        return False

def run_classification(jobId):
    try:
        start_time = time.time()
        log = Logger(jobId, 'classification.py', 'main')
        log.also_print = True
        configuration = EnvironmentConfig()
        config = configuration.data()
        bucketName = config[4]
        db = get_db(config)
        log.write('database connection succesful')
        (
            classifierId, projectId, userId,
            classificationName, playlistId, ncpu
        ) = get_classification_job_data(db,jobId)
        log.write('job data fetched.')

        model_specs = get_model_params(db, classifierId, log)
        log.write('model params fetched. %s' % str(model_specs))

        db.close()
    except:
        log.write('ERROR:: {}'.format(traceback.format_exc()))
        return False

    if model_specs['model_type_id'] in [4]:
        retValue = run_pattern_matching(jobId, model_specs, playlistId, log, config, ncpu)
        return retValue
    elif model_specs['model_type_id'] in [-1]:
        pass
        """Entry point for new model types"""
    else:
        log.write("Unkown model type")
        return False
