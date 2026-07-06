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
import concurrent.futures

from .a2pyutils.logger import Logger
from .a2audio.recanalizer import Recanalizer
from .db import connect, get_classification_job_data, get_model_params, get_playlist, insert_rec_error, set_progress_params, update_job_error
from .storage import upload_file, download_file, config as storage_config

FORCE_SEQUENTIAL_EXECUTION = os.getenv('FORCE_SEQUENTIAL_EXECUTION') == '1'

classificationCanceled = False

def exit_error(db, log, job_id, msg):
    log.write(msg)
    # NOTE: db.update_job_error signature is (db, job_id, msg); pass in order.
    update_job_error(db, job_id, msg)
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
    log.write('running classification...')
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
        log.write('recAnalized {}'.format('starting download and analize'))
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
    if rec_analized is not None and rec_analized.status == 'Processed':
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

# Phase-2 knobs. Kept env-overridable so behaviour can be tuned without a
# rebuild. Defaults preserve exact semantics but amortize DB work and overlap
# vector writes/uploads, which dominate large playlist result-write phases.
RESULT_BATCH_SIZE = max(1, int(os.getenv('CLASSIFY_RESULT_BATCH_SIZE', '250')))
VECTOR_UPLOAD_WORKERS = max(1, int(os.getenv('CLASSIFY_VECTOR_UPLOAD_WORKERS', '6')))
VECTOR_UPLOAD_MAX_OUTSTANDING = max(
    VECTOR_UPLOAD_WORKERS * 4,
    int(os.getenv('CLASSIFY_VECTOR_UPLOAD_MAX_OUTSTANDING', str(RESULT_BATCH_SIZE)))
)

def _existing_result_recording_ids(db, job_id, species, songtype, log):
    """Return recordings that already have a classification_result for this job.

    This makes phase 2 restart-safe after a pod/deadline failure: a re-run still
    recomputes feature vectors (so final min/max stats remain deterministic),
    but it skips duplicate vector uploads and classification_results inserts for
    rows that were already committed before the failure.
    """
    try:
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                SELECT `recording_id`
                FROM `classification_results`
                WHERE `job_id` = %s
                  AND `species_id` = %s
                  AND `songtype_id` = %s
            """, [job_id, species, songtype])
            return set(row[0] for row in cursor)
    except Exception:
        log.write('could not load existing classification_results for resume: {}'.format(traceback.format_exc()))
        return set()


def _flush_result_batch(db, job_id, pending_rows, pending_progress, log):
    """Commit one batch: bump progress by the number of records seen since the
    last flush, and bulk-insert the accumulated classification_results rows.

    On a bulk-insert failure we fall back to per-row inserts so a single bad
    row does not lose the whole batch and still gets a recordings_errors
    entry, mirroring the original per-record path. Returns ([], 0) so callers
    can reset the accumulators in one assignment.
    """
    if pending_progress <= 0 and not pending_rows:
        return [], 0
    try:
        with contextlib.closing(db.cursor()) as cursor:
            if pending_progress > 0:
                cursor.execute("""
                    UPDATE `jobs`
                    SET `progress` = `progress` + %s, last_update = NOW()
                    WHERE `job_id` = %s
                """, [pending_progress, job_id])
            if pending_rows:
                cursor.executemany("""
                    INSERT INTO `classification_results` (
                        job_id, recording_id, species_id, songtype_id, present,
                        max_vector_value
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, pending_rows)
        db.commit()
    except Exception:
        # Bulk path failed: recover progress + salvage rows individually so a
        # single offending record cannot drop the batch's results/progress.
        log.write('batch flush failed, falling back to per-row inserts: {}'.format(traceback.format_exc()))
        try:
            db.rollback()
        except Exception:
            pass
        if pending_progress > 0:
            try:
                with contextlib.closing(db.cursor()) as cursor:
                    cursor.execute("""
                        UPDATE `jobs`
                        SET `progress` = `progress` + %s, last_update = NOW()
                        WHERE `job_id` = %s
                    """, [pending_progress, job_id])
                db.commit()
            except Exception:
                log.write('progress catch-up update failed: {}'.format(traceback.format_exc()))
        for row in pending_rows:
            # row = [job_id, rec_id, species, songtype, presence, max_v]
            insert_result_to_db(db, row[0], row[1], row[2], row[3], row[4], row[5])
    return [], 0

def _vector_result_row(r, working_folder, model_uri, job_id, species, songtype):
    """Write and upload one vector file, returning the DB row to insert.

    This function intentionally does not touch the DB so it is safe to run in a
    bounded thread pool. The parent thread preserves the legacy DB semantics:
    - vector write failure: record an error and skip classification_results row
    - vector upload failure: record an error but still insert the result row
      (matching the old upload_vector() path, which swallowed upload errors)
    """
    rec_name = r['uri'].split('/')[-1]
    local_file = write_vector(r['uri'], working_folder, r['f'])
    if local_file is None:
        return {
            'row': None,
            'error_rec_id': r['id'],
            'error': 'localFile is None',
            'minv': None,
            'maxv': None,
        }

    maxv = max(r['f'])
    minv = min(r['f'])
    vector_uri = '{}/classification_{}_{}.vector'.format(
        model_uri.replace('.mod', ''), job_id, rec_name
    )
    upload_error = None
    try:
        upload_file(local_file, vector_uri)
        os.remove(local_file)
    except Exception:
        upload_error = traceback.format_exc()
        try:
            if os.path.exists(local_file):
                os.remove(local_file)
        except Exception:
            pass
    return {
        'row': [job_id, r['id'], species, songtype, r['r'], float(maxv)],
        'error_rec_id': r['id'] if upload_error else None,
        'error': upload_error,
        'minv': float(minv),
        'maxv': float(maxv),
    }


def _handle_vector_result(item, db, job_id, pending_rows, log):
    if item.get('error_rec_id') is not None:
        if item.get('error'):
            log.write('vector upload/write error for rec {rid}: {err}'.format(
                rid=item['error_rec_id'], err=item['error']))
        insert_rec_error(db, item['error_rec_id'], job_id)
    if item.get('row') is not None:
        pending_rows.append(item['row'])
    return pending_rows


def process_results(res, working_folder, model_uri, job_id, species, songtype, db, log):
    min_vector_val = 9999999.0
    max_vector_val = -9999999.0
    processed = 0
    seen = 0
    pending_rows = []
    pending_progress = 0
    futures = set()

    def consume_done(done):
        nonlocal min_vector_val, max_vector_val, pending_rows, pending_progress, seen
        for fut in done:
            item = fut.result()
            pending_progress += 1
            seen += 1
            if item.get('minv') is not None and min_vector_val > item['minv']:
                min_vector_val = item['minv']
            if item.get('maxv') is not None and max_vector_val < item['maxv']:
                max_vector_val = item['maxv']
            pending_rows = _handle_vector_result(item, db, job_id, pending_rows, log)
            if pending_progress >= RESULT_BATCH_SIZE:
                pending_rows, pending_progress = _flush_result_batch(
                    db, job_id, pending_rows, pending_progress, log)
                log.write('processed {seen} results ({ins} valid so far) for {sp} {st}'.format(
                    seen=seen, ins=processed, sp=species, st=songtype))

    try:
        existing_result_ids = _existing_result_recording_ids(db, job_id, species, songtype, log)
        log.write('processing classification results with batch_size={b}, vector_upload_workers={w}, existing_results={e}'.format(
            b=RESULT_BATCH_SIZE, w=VECTOR_UPLOAD_WORKERS, e=len(existing_result_ids)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=VECTOR_UPLOAD_WORKERS) as executor:
            for r in res:
                if r and 'id' in r:
                    processed += 1
                    if r['id'] in existing_result_ids:
                        # Recomputed vector still participates in final stats,
                        # but committed row/vector output is not duplicated.
                        maxv = max(r['f'])
                        minv = min(r['f'])
                        if min_vector_val > float(minv):
                            min_vector_val = float(minv)
                        if max_vector_val < float(maxv):
                            max_vector_val = float(maxv)
                        pending_progress += 1
                        seen += 1
                        if pending_progress >= RESULT_BATCH_SIZE:
                            pending_rows, pending_progress = _flush_result_batch(
                                db, job_id, pending_rows, pending_progress, log)
                            log.write('processed {seen} results ({ins} valid so far) for {sp} {st}'.format(
                                seen=seen, ins=processed, sp=species, st=songtype))
                        continue
                    futures.add(executor.submit(
                        _vector_result_row, r, working_folder, model_uri, job_id, species, songtype))
                    if len(futures) >= VECTOR_UPLOAD_MAX_OUTSTANDING:
                        done, futures = concurrent.futures.wait(
                            futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        consume_done(done)
                else:
                    # Progress advances once per result (valid or error),
                    # matching the original per-record phase-2 semantics.
                    pending_progress += 1
                    seen += 1
                    if pending_progress >= RESULT_BATCH_SIZE:
                        pending_rows, pending_progress = _flush_result_batch(
                            db, job_id, pending_rows, pending_progress, log)
                        log.write('processed {seen} results ({ins} valid so far) for {sp} {st}'.format(
                            seen=seen, ins=processed, sp=species, st=songtype))
            while futures:
                done, futures = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED)
                consume_done(done)
        # Final flush: exact remainder so progress + results stay precise.
        pending_rows, pending_progress = _flush_result_batch(
            db, job_id, pending_rows, pending_progress, log)
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


        
