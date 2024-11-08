import contextlib
import tempfile
import shutil
import os
import traceback
import multiprocessing
from joblib import Parallel, delayed
import pickle
import csv
import json
import sys

from .a2pyutils.logger import Logger
from .a2audio.recanalizer import Recanalizer
from .db import connect, update_job_error, update_job_last_update, update_job_progress, update_validations
from .storage import upload_file, download_file


classificationCanceled = False

def exit_error(db, log, job_id, msg):
    log.write(msg)
    update_job_error(db, msg, job_id)
    remove_working_folder(job_id)
    sys.exit(-1)

def get_working_folder(job_id):
    temp_folder = tempfile.gettempdir()
    working_folder = temp_folder+"/job_"+str(job_id)+"/"
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)
    return working_folder

def remove_working_folder(job_id):
    working_folder = get_working_folder(job_id)
    if os.path.exists(working_folder):
        shutil.rmtree(working_folder)

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
        exit_error(db, log, job_id, "Could not generate playlist array, {}".format(traceback.format_exc()))
    if len(recsToClassify) < 1:
        exit_error(db, log, job_id, 'No recordngs in playlist, {}'.format(traceback.format_exc()),-1,log)
    return recsToClassify

def set_progress_params(db,progress_steps, job_id):
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                UPDATE `jobs`
                SET `progress_steps`=%s, progress=0, state="processing"
                WHERE `job_id` = %s
            """, [progress_steps*2+5, job_id])
            db.commit()
    except:
        exit_error(db, log, job_id, "Could not set progress params, {}".format(traceback.format_exc()))

def insert_rec_error(db, rec_id, job_id):
    error = traceback.format_exc()
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                INSERT INTO `recordings_errors`(`recording_id`, `job_id`, `error`)
                VALUES (%s, %s, %s)
            """, [rec_id, job_id, error])
            db.commit()
    except:
        exit_error(db, log, job_id, "Could not insert recording error, {}.\n\n ORIGINAL ERROR: {}".format(traceback.format_exc(), error))

def cancelStatus(db, job_id, rmFolder=None,quitj=True):
    status = None
    with contextlib.closing(db.cursor()) as cursor:
        cursor.execute('select `cancel_requested` from `jobs` where `job_id` = '+str(job_id))
        (status,) = cursor.fetchone()
        if status and int(status) > 0:
            cursor.execute('update `jobs` set `state` = "canceled", last_update = now() where `job_id` = '+str(job_id))
            db.commit()
            print('job canceled')
            if rmFolder:
                if os.path.exists(rmFolder):
                    shutil.rmtree(rmFolder)
            if quitj:
                quit()
            else:
                return True
        else:
            return False

def classify_rec(rec, model_specs, working_folder, log, job_id):
    global classificationCanceled
    if classificationCanceled:
        return None
    errorProcessing = False
    db = connect()
    if cancelStatus(db,job_id,working_folder,False):
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
                                  working_folder,
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
            """, [job_id])
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
        insert_rec_error(db,rec['recording_id'],job_id)
        db.close()
        return None
    else:
        log.write('done processing this rec')
        db.close()
        return {'uri':rec['uri'],'id':rec['recording_id'],'f':featvector,'ft':fets,'r':res[0]}


def get_model(db, model_specs, log, working_folder, job_id):
    log.write('downloading model from bucket')
    model_local = working_folder+'model.mod'
    try:
        download_file(model_specs['uri'], model_local)
    except:
        exit_error(db, log, job_id, 'fatal error model {} not found in aws, {}'.format(model_specs['uri'], traceback.format_exc()), -1, log)

    log.write('model in local file system')
    model_specs['model'] = None

    log.write('loading model to memory...')
    if os.path.isfile(model_local):
        model_data = pickle.load(open(model_local, "rb"))
        if isinstance(model_data, dict):
            # future model formats (they should be pickled as a dict)
            model_specs = model_data
        else:
            # current style models (they're pickled as a list)
            model_specs['data'] = model_data
    else:
        exit_error(db, log, job_id, 'fatal error cannot load model, {}'.format(traceback.format_exc()), -1, log)
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
        print('ERROR:: {}'.format(traceback.format_exc()))
        return None
    return vectorLocal

def upload_vector(uri,filen,rid,db,jobId):
    try:
        upload_file(filen, uri)
        os.remove(filen)
    except:
        insert_rec_error(db, rid, jobId)

def insert_result_to_db(db, jId, recId, species, songtype, presence, maxV):
    try:
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

def processResults(res,working_folder,modelUri,job_id,species,songtype,db,log):
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
                """, [job_id])
                db.commit()
            if r and 'id' in r:
                processed = processed + 1
                recName = r['uri'].split('/')
                recName = recName[len(recName)-1]
                localFile = write_vector(r['uri'],working_folder,r['f'])
                if localFile is not None:
                    maxv = max(r['f'])
                    minv = min(r['f'])
                    if minVectorVal > float(minv):
                        minVectorVal = minv
                    if maxVectorVal < float(maxv):
                        maxVectorVal = maxv
                    vectorUri = '{}/classification_{}_{}.vector'.format(
                            modelUri.replace('.mod', ''), job_id, recName
                    )
                    upload_vector(vectorUri,localFile,r['id'],db,job_id)
                    log.write("inserting results from {rid} for {sp} {st} into the database ({r}, maxv:{maxv})".format(
                        rid=r['id'],
                        r=r['r'],
                        sp=species,
                        st=songtype,
                        maxv=maxv
                    ))
                    insert_result_to_db(db, job_id,r['id'], species, songtype,r['r'],maxv)
                else:
                    log.write('localFile is None')
                    insert_rec_error(db, r['id'], job_id)
    except:
        exit_error(db, log, job_id, 'cannot process results. {}'.format(traceback.format_exc()))
    return {"t":processed,"stats":{"minv": float(minVectorVal), "maxv": float(maxVectorVal)}}

def run_classification(job_id):
    global classificationCanceled

    log = Logger(job_id, 'classification.py', 'main')
    log.also_print = True
    
    db = connect()
    (classifier_id, _, _, _, playlist_id, ncpu) = get_classification_job_data(db, job_id)
    log.write('job data fetched.')

    model_specs = get_model_params(db, classifier_id, log)
    log.write('model params fetched. %s' % str(model_specs))

    if model_specs['model_type_id'] != 4:
        log.write("unknown model type requested")
        sys.exit(-1)

    num_cores = multiprocessing.cpu_count()
    if int(ncpu) > 0:
        num_cores = int(ncpu)
    
    working_folder = get_working_folder(job_id)
    log.write('created working directory.')
    recs = get_playlist(db,playlist_id,log)
    log.write('playlist generated.')
    set_progress_params(db,len(recs), job_id)
    log.write('job progress set to start.')
    model_specs = get_model(db, model_specs, log, working_folder, job_id)
    log.write('model was fetched.')
    cancelStatus(db,job_id,working_folder)
    db.close()

    log.write('starting parallel for.')
    try:
        results = Parallel(n_jobs=num_cores)(
            delayed(classify_rec)(rec, model_specs, working_folder, log, job_id) for rec in recs
        )
    except:
        log.write('ERROR:: {}'.format(traceback.format_exc()))
        if classificationCanceled:
            log.write('job cancelled')
        return False
    log.write('done parallel execution.')
    
    db = connect()
    cancelStatus(db,job_id,working_folder)
    try:
        stats = processResults(results, working_folder, model_specs['uri'], job_id, model_specs['species'], model_specs['songtype'], db, log)
    except:
        log.write('ERROR:: {}'.format(traceback.format_exc()))
        return False
    log.write('computed stats.')
    shutil.rmtree(working_folder)
    log.write('removed folder.')
    statsJson = stats['stats']
    if stats['t'] < 1:
        exit_error('no recordings processed. {}'.format(traceback.format_exc()))
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                INSERT INTO `classification_stats` (`job_id`, `json_stats`)
                VALUES (%s, %s)
            """, [job_id, json.dumps(statsJson)])
            db.commit()
            cursor.execute("""
                UPDATE `jobs`
                SET `progress` = `progress_steps`, `completed` = 1,
                    state="completed", `last_update` = now()
                WHERE `job_id` = %s
            """, [job_id])
            db.commit()
        db.close()
        return True
    except:
        db.close()
        log.write('ERROR:: {}'.format(traceback.format_exc()))
        return False


        
