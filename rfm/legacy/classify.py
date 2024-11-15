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
from .db import connect, get_classification_job_data, get_model_params, get_playlist, insert_rec_error, set_progress_params, update_job_error
from .storage import upload_file, download_file, config as storage_config

FORCE_SEQUENTIAL_EXECUTION = os.getenv('FORCE_SEQUENTIAL_EXECUTION') == '1'

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


def cancel_status(db, job_id, rm_folder=None, quitj=True):
    status = None
    with contextlib.closing(db.cursor()) as cursor:
        cursor.execute('select `cancel_requested` from `jobs` where `job_id` = '+str(job_id))
        (status,) = cursor.fetchone()
        if status and int(status) > 0:
            cursor.execute('update `jobs` set `state` = "canceled", last_update = now() where `job_id` = '+str(job_id))
            db.commit()
            print('job canceled')
            if rm_folder:
                if os.path.exists(rm_folder):
                    shutil.rmtree(rm_folder)
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
    error_processing = False
    db = connect()
    if cancel_status(db,job_id,working_folder,False):
        classificationCanceled = True
        quit()
    rec_analized = None
    model_data = model_specs['data']
    try:
        use_ssim = True
        if len(model_data) > 5:
            use_ssim = model_data[5]
        bucket_name = storage_config['s3_legacy_bucket_name'] if rec['legacy'] else storage_config['s3_bucket_name']
        rec_analized = Recanalizer(rec['uri'],
                                  model_data[1],
                                  float(model_data[2]),
                                  float(model_data[3]),
                                  working_folder,
                                  bucket_name,
                                  log,
                                  False,
                                  use_ssim,
                                  modelSampleRate=model_specs['sample_rate'],
                                  legacy=rec['legacy'])
        log.write('recAnalized {}'.format(rec_analized.status))
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                UPDATE `jobs`
                SET `progress` = `progress` + 1, last_update = NOW()
                WHERE `job_id` = %s
            """, [job_id])
            db.commit()
    except Exception:
        error_processing = True
        log.write('error rec analyzed {} '.format(traceback.format_exc()))
    log.write('finish')
    featvector = None
    fets = None
    if rec_analized.status == 'Processed':
        try:
            featvector = rec_analized.getVector()
            fets = rec_analized.features()
        except Exception:
            error_processing = True
            log.write('error getting feature vectors {} '.format(traceback.format_exc()))
    else:
        error_processing = True
    res = None
    log.write('FEATS COMPUTED')
    if featvector is not None:
        try:
            clf = model_data[0]
            res = clf.predict([fets])
        except Exception:
            error_processing = True
            log.write('error predicting {} '.format(traceback.format_exc()))
    else:
        error_processing = True
    if error_processing:
        try:
            insert_rec_error(db,rec['recording_id'],job_id)
        except Exception:
            exit_error(db, log, job_id, "Could not insert recording error, {}".format(traceback.format_exc()))
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
    except Exception:
        exit_error(db, log, job_id, 'fatal error model {} not found in aws, {}'.format(model_specs['uri'], traceback.format_exc()))

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
        exit_error(db, log, job_id, 'fatal error cannot load model, {}'.format(traceback.format_exc()))
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
            model_specs["sample_rate"] = cursor.fetchone()[0]
        log.write('model sampling rate is {}'.format(model_specs["sample_rate"]))

    return model_specs

def write_vector(rec_uri, temp_folder, featvector):
    vector_local = None
    try:
        rec_name = rec_uri.split('/')
        rec_name = rec_name[len(rec_name)-1]
        vector_local = temp_folder+rec_name+'.vector'
        file = open(vector_local, 'w')
        wr = csv.writer(file)
        wr.writerow(featvector)
        file.close()
    except Exception:
        print('ERROR writing {}'.format(traceback.format_exc()))
        return None
    return vector_local

def upload_vector(uri,filen,rid,db,job_id):
    try:
        upload_file(filen, uri)
        os.remove(filen)
    except Exception:
        insert_rec_error(db, rid, job_id)

def insert_result_to_db(db, job_id, rec_id, species, songtype, presence, max_v):
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                INSERT INTO `classification_results` (
                    job_id, recording_id, species_id, songtype_id, present,
                    max_vector_value
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, [job_id, rec_id, species, songtype, presence, float(max_v)])
            db.commit()
    except Exception:
        print('ERROR writing {}'.format(traceback.format_exc()))
        insert_rec_error(db, rec_id, job_id)

def process_results(res, working_folder, model_uri, job_id, species, songtype, db, log):
    min_vector_val = 9999999.0
    max_vector_val = -9999999.0
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
                rec_name = r['uri'].split('/')
                rec_name = rec_name[len(rec_name)-1]
                local_file = write_vector(r['uri'],working_folder,r['f'])
                if local_file is not None:
                    maxv = max(r['f'])
                    minv = min(r['f'])
                    if min_vector_val > float(minv):
                        min_vector_val = minv
                    if max_vector_val < float(maxv):
                        max_vector_val = maxv
                    vector_uri = '{}/classification_{}_{}.vector'.format(
                            model_uri.replace('.mod', ''), job_id, rec_name
                    )
                    upload_vector(vector_uri,local_file,r['id'],db,job_id)
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
    except Exception:
        exit_error(db, log, job_id, 'cannot process results. {}'.format(traceback.format_exc()))
    return {"t":processed,"stats":{"minv": float(min_vector_val), "maxv": float(max_vector_val)}}

def run_classification(job_id):
    global classificationCanceled

    log = Logger(job_id, 'classification.py', 'main')
    log.also_print = True
    
    db = connect()
    try:
        (classifier_id, _, _, _, playlist_id, ncpu) = get_classification_job_data(db, job_id)
    except Exception:
        exit_error(db, log, job_id, "could not get classification job #{}, {}".format(job_id, traceback.format_exc()))
    log.write('job data fetched.')

    try:
        model_specs = get_model_params(db, classifier_id)
    except Exception:
        exit_error(db, log, job_id, "could not get model params {}".format(traceback.format_exc()))
    log.write('model params fetched. %s' % str(model_specs))

    if model_specs['model_type_id'] != 4:
        log.write("unknown model type requested")
        sys.exit(-1)

    num_cores = multiprocessing.cpu_count()
    if int(ncpu) > 0:
        num_cores = int(ncpu)
    
    working_folder = get_working_folder(job_id)
    log.write('created working directory')
    try:
        recs = get_playlist(db, playlist_id)
    except Exception:
        exit_error(db, log, job_id, "could not get playlist, {}".format(traceback.format_exc()))
    if len(recs) < 1:
        exit_error(db, log, job_id, 'no recordings in playlist, {}'.format(traceback.format_exc()))
    log.write('playlist generated')
    try:
        set_progress_params(db,len(recs), job_id)
    except Exception:
        exit_error(db, log, job_id, "could not set progress params, {}".format(traceback.format_exc()))
    log.write('job progress set to start')
    model_specs = get_model(db, model_specs, log, working_folder, job_id)
    log.write('model was fetched')
    cancel_status(db, job_id, working_folder)
    db.close()

    log.write('starting parallel classify of recs')
    try:
        if FORCE_SEQUENTIAL_EXECUTION:
            log.write('sequential mode for testing')
            results = []
            for rec in recs:
                result = classify_rec(rec, model_specs, working_folder, log, job_id)
                results.append(result)
        else:
            results = Parallel(n_jobs=num_cores)(
                delayed(classify_rec)(rec, model_specs, working_folder, log, job_id) for rec in recs
            )
    except Exception:
        log.write('ERROR::parallel classify_rec {}'.format(traceback.format_exc()))
        if classificationCanceled:
            log.write('job cancelled')
        return False
    log.write('done parallel classify')
    
    db = connect()
    cancel_status(db, job_id, working_folder)
    try:
        stats = process_results(results, working_folder, model_specs['uri'], job_id, model_specs['species'], model_specs['songtype'], db, log)
    except Exception:
        log.write('ERROR:: {}'.format(traceback.format_exc()))
        return False
    log.write('computed stats')
    shutil.rmtree(working_folder)
    log.write('removed folder')
    stats_json = stats['stats']
    if stats['t'] < 1:
        exit_error(db, log, job_id, 'no recordings processed. {}'.format(traceback.format_exc()))
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                INSERT INTO `classification_stats` (`job_id`, `json_stats`)
                VALUES (%s, %s)
            """, [job_id, json.dumps(stats_json)])
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
    except Exception:
        db.close()
        log.write('ERROR:: {}'.format(traceback.format_exc()))
        return False


        
