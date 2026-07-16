import json
import os
import time
import traceback
from contextlib import closing

# mysql2pg Phase 5.5 (W4, 2026-07-16): dialect-aware DB layer.
# ARBIMON_DB_DIALECT=mysql (DEFAULT - byte-for-byte today's behavior) or
# postgres. The PG path activates ONLY via env at the coordinated jobs-plane
# flip; nothing changes for existing deploys. Env names stay DB_* for both
# dialects (the flip changes env VALUES, not names; ARBIMON_DB_PORT
# overrides DB_PORT when set).
#
# SQL portability changes in this module (validated equivalent both ways):
#   - backticks stripped (the schema names are all lowercase; PG identifiers
#     unquoted, MySQL accepts unquoted lowercase identically)
#   - double-quoted STRING literals -> single quotes ("processing" is an
#     IDENTIFIER to PG and would hard-error)
#   - ORDER BY rand() -> the dialect-appropriate random function
#   - IF(LEFT(uri,8)='project_',1,0) -> CASE WHEN ... THEN 1 ELSE 0 END
#     (identical result both engines; MySQL IF() does not exist in PG)
#   - cursor.lastrowid -> INSERT ... RETURNING on PG (helper below)

DIALECT = os.getenv('ARBIMON_DB_DIALECT', 'mysql').lower()
IS_PG = DIALECT in ('postgres', 'postgresql', 'pg')

if IS_PG:
    import psycopg2
    import psycopg2.errors
else:
    import mysql.connector

config = {
    'db_host': os.getenv('DB_HOST'),
    'db_port': int(os.getenv('ARBIMON_DB_PORT') or os.getenv('DB_PORT', '3306')),
    'db_user': os.getenv('DB_USER'),
    'db_password': os.getenv('DB_PASSWORD'),
    'db_name': os.getenv('DB_NAME'),
}

# Resilience knobs (env-overridable so behaviour can be tuned without a
# rebuild). Analysis workers open a DB connection up front and only use it
# after a slow (often multi-second, load-dependent) S3 download; under storage
# strain that idle connection can be reaped by MaxScale / wait_timeout, and the
# server itself can transiently refuse connections. Retrying the *connect* and
# transparently reconnecting a *dropped* connection turns those load-induced
# blips into a slightly slower run instead of a lost job.
# (mysql2pg: the same semantics are preserved verbatim on the postgres path;
# psycopg2 has no reconnect(), so ensure_connection re-dials via connect().)
DB_CONNECT_RETRIES = max(1, int(os.getenv('DB_CONNECT_RETRIES', '5')))
DB_CONNECT_BACKOFF = float(os.getenv('DB_CONNECT_BACKOFF', '1.5'))
DB_CONNECT_BACKOFF_MAX = float(os.getenv('DB_CONNECT_BACKOFF_MAX', '30'))

def _new_connection():
    if IS_PG:
        return psycopg2.connect(
            host=config['db_host'],
            port=config['db_port'],
            user=config['db_user'],
            password=config['db_password'],
            dbname=config['db_name'],
            connect_timeout=10,
        )
    return mysql.connector.connect(
        host=config['db_host'],
        port=config['db_port'],
        user=config['db_user'],
        password=config['db_password'],
        database=config['db_name']
    )

def _connect_error_types():
    if IS_PG:
        return (psycopg2.Error,)
    return (mysql.connector.Error,)

def connect(log=None):
    """Open a DB connection, retrying transient failures with backoff.

    A cold cluster / strained DB can refuse or slow-drop new connections; a
    single attempt made analysis jobs fail purely due to load. We retry a
    bounded number of times with exponential backoff before giving up.
    """
    last_exc = None
    delay = DB_CONNECT_BACKOFF
    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            return _new_connection()
        except _connect_error_types() as exc:
            last_exc = exc
            if attempt >= DB_CONNECT_RETRIES:
                break
            if log is not None:
                try:
                    log.write('db connect attempt {}/{} failed ({}); retrying in {:.1f}s'.format(
                        attempt, DB_CONNECT_RETRIES, exc, delay))
                except Exception:
                    pass
            time.sleep(delay)
            delay = min(delay * 2, DB_CONNECT_BACKOFF_MAX)
    raise last_exc

def _is_live(db):
    if db is None:
        return False
    if IS_PG:
        try:
            return db.closed == 0
        except Exception:
            return False
    try:
        return db.is_connected()
    except Exception:
        return False

def ensure_connection(db, log=None):
    """Return a live connection, reconnecting transparently if the current one
    has been dropped (idle-reaped during a slow download, failover, etc.).

    MySQL path: prefers mysql.connector's own reconnect (preserves the handle
    so callers keep using the same object); falls back to a fresh connection.
    PG path: psycopg2 has no reconnect(); a dead handle gets a fresh
    connection via connect() (same retry/backoff). Returns the usable
    connection (possibly a new object).
    """
    if _is_live(db):
        return db
    if not IS_PG:
        # Try to revive the existing handle first (keeps the object identity).
        try:
            if db is not None:
                db.reconnect(attempts=DB_CONNECT_RETRIES, delay=int(DB_CONNECT_BACKOFF))
                if db.is_connected():
                    if log is not None:
                        try:
                            log.write('db connection reconnected')
                        except Exception:
                            pass
                    return db
        except Exception as exc:
            if log is not None:
                try:
                    log.write('db reconnect failed ({}); opening a fresh connection'.format(exc))
                except Exception:
                    pass
    # Last resort: a brand-new connection (with connect() retry/backoff).
    return connect(log=log)

# --- tiny dialect helpers -------------------------------------------------

_RAND = 'random()' if IS_PG else 'rand()'
# IF(LEFT(uri,8)='project_',1,0) equivalent, identical result both engines:
_LEGACY_EXPR = "CASE WHEN LEFT(r.uri, 8) = 'project_' THEN 1 ELSE 0 END"

def insert_returning_id(cursor, sql, params, id_column):
    """INSERT and return the generated id, dialect-appropriately.

    MySQL: execute + cursor.lastrowid. PG: append RETURNING <id_column> and
    fetch. The caller's SQL must NOT already carry RETURNING.
    """
    if IS_PG:
        cursor.execute(sql + ' RETURNING ' + id_column, params)
        return cursor.fetchone()[0]
    cursor.execute(sql, params)
    return cursor.lastrowid

# --- queries ---------------------------------------------------------------

def get_training_job(db, job_id):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT j.project_id, j.user_id, jp.model_type_id, jp.training_set_id, jp.name
            FROM jobs j JOIN job_params_training jp ON jp.job_id = j.job_id
            WHERE j.job_id = %s""", [job_id])
        (project_id, user_id, model_type_id, training_set_id, model_name) = cursor.fetchone()
    return (project_id, user_id, model_type_id, training_set_id, model_name)

def get_retraining_job(db, job_id):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT trained_job_id
            FROM job_params_retraining
            WHERE job_id = %s""", [job_id])
        (trained_job_id) = cursor.fetchone()
    return (trained_job_id)

def get_training_job_params(db, job_id):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT use_in_training_present, use_in_training_notpresent, 
                use_in_validation_present, use_in_validation_notpresent FROM job_params_training 
            WHERE job_id = %s""", [job_id])
        (tp, tnp, vp, vnp) = cursor.fetchone()
    return (tp, tnp, vp, vnp)

def get_training_data(db, training_set_id): 
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT r.recording_id, ts.species_id, ts.songtype_id,
                ts.x1, ts.x2, ts.y1, ts.y2, r.uri, """ + _LEGACY_EXPR + """ legacy
            FROM training_set_roi_set_data ts
            JOIN recordings r ON r.recording_id = ts.recording_id
            WHERE ts.training_set_id = %s
        """, [training_set_id])
        training_data = [row for row in cursor]            
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT DISTINCT species_id, songtype_id
            FROM training_set_roi_set_data
            WHERE training_set_id = %s
        """, [training_set_id])
        species_songtypes = [[species_id, songtype_id] for (species_id, songtype_id) in cursor]
    return training_data, species_songtypes

def get_validation_data(db, project_id, species_id, songtype_id, num_positive, num_negative):
    with closing(db.cursor()) as cursor:
        cursor.execute(
            """
            (SELECT r.uri , rv.species_id , rv.songtype_id , present , present_review , r.recording_id, """ + _LEGACY_EXPR + """ legacy
            FROM recording_validations rv 
            JOIN recordings r ON r.recording_id = rv.recording_id
            WHERE rv.project_id = %s
            AND rv.species_id = %s
            AND rv.songtype_id = %s
            AND (present = 1 OR present_review > 0)
            ORDER BY """ + _RAND + """
            LIMIT %s)
            UNION
            (SELECT r.uri , rv.species_id , rv.songtype_id , present , present_review , r.recording_id, """ + _LEGACY_EXPR + """ legacy
            FROM recording_validations rv 
            JOIN recordings r ON r.recording_id = rv.recording_id
            WHERE rv.project_id = %s
            AND rv.species_id = %s
            AND rv.songtype_id = %s
            AND present = 0
            AND present_review = 0
            ORDER BY """ + _RAND + """
            LIMIT %s)
        """, [project_id, species_id, songtype_id, num_positive, project_id, species_id, songtype_id, num_negative])
        results = [row for row in cursor]
    return results

def update_validations(db, project_id, user_id, model_name, validations_key, job_id, progress_steps):
    with closing(db.cursor()) as cursor:
        validation_set_id = insert_returning_id(cursor, """
            INSERT INTO validation_set(
                project_id,
                user_id,
                name,
                uri,
                params,
                job_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s
            )""", [
                project_id, user_id, model_name+" validation", validations_key,
                json.dumps({'name': model_name}), job_id],
            'validation_set_id')
        db.commit()

        cursor.execute("""
            UPDATE job_params_training SET validation_set_id = %s
            WHERE job_id = %s""", [validation_set_id, job_id])
        db.commit()

        _set_steps_then_processing(cursor, db, job_id, progress_steps)

    return validation_set_id

def _set_steps_then_processing(cursor, db, job_id, progress_steps):
    """Two-step, trigger-aware job progress init (2026-07-08).

    arbimon2 has a BEFORE UPDATE trigger on jobs (jobs_BEFORE_UPDATE) that
    force-sets NEW.state='completed' whenever NEW.progress >= OLD.progress_steps.
    A fresh job row has progress_steps=0, so the old single combined UPDATE
    (steps=N, progress=0, state='processing') evaluated 0 >= 0 -> the trigger
    overrode the state we were writing to 'completed' IN THE SAME STATEMENT,
    and since the classify phase-1 progress bumps never write state, the job
    showed 'completed' for its entire run — and a KILLED job was left
    state='completed', completed=0, zero results (silent-failure ghost; see
    rfcx-local OPEN-ITEMS #28 addendum, observed live on job 167683).

    Fix mirrors the PM driver's _init() (pm_drive.py, PR #6 2026-06-12):
    (1) raise progress_steps FIRST in its own UPDATE (this one may still trip
    the trigger against the OLD 0 — harmless), then (2) set progress/state —
    by then OLD.progress_steps is the real total so the trigger can't fire,
    and this write corrects any flip from step 1.

    mysql2pg note: the trigger is NOT ported to PG (deliberately — it IS the
    #28 bug class), so on the postgres path this two-step is simply a safe
    ordered init; keep it identical for both engines.
    """
    cursor.execute("""
        UPDATE jobs SET progress_steps = %s
        WHERE job_id = %s""", [progress_steps, job_id])
    db.commit()
    cursor.execute("""
        UPDATE jobs SET progress=0, state='processing', last_update=now()
        WHERE job_id = %s""", [job_id])
    db.commit()

def set_progress_steps(db, job_id, progress_steps):
     with closing(db.cursor()) as cursor:
        _set_steps_then_processing(cursor, db, job_id, progress_steps)


def update_job_error(db, job_id, msg):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            UPDATE jobs
            SET remarks = %s,
                state='error',
                completed = 1 ,
                last_update = now()
            WHERE job_id = %s
        """, ['Error: '+str(msg), job_id])
        db.commit()

def update_job_last_update(db, job_id):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            UPDATE jobs
            SET last_update=now()
            WHERE job_id = %s
        """, [job_id])
        db.commit()

def update_job_progress(db, job_id: int, progress_increment = 1):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            UPDATE jobs
            SET state = 'processing', progress = progress + %s, last_update = now()
            WHERE job_id = %s
        """, [progress_increment, job_id])
        db.commit()

def set_progress_params(db, progress_steps, job_id):
    # Two-step trigger-aware init — see _set_steps_then_processing. This is
    # the classification path that produced the live 'completed'-while-running
    # mislabels (OPEN-ITEMS #28 addendum).
    with closing(db.cursor()) as cursor:
        _set_steps_then_processing(cursor, db, job_id, progress_steps*2+5)

def get_classification_job_data(db, job_id):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT jp.model_id, j.project_id, j.user_id,
                jp.name, jp.playlist_id, j.ncpu
            FROM jobs j
            JOIN job_params_classification jp ON jp.job_id = j.job_id
            WHERE j.job_id = %s
        """, [job_id])
        (model_id, project_id, user_id, name, playlist_id, ncpu) = cursor.fetchone()
    return model_id, project_id, user_id, name, playlist_id, ncpu

def get_model_params(db, classifier_id):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT m.model_type_id,m.uri,ts.species_id,ts.songtype_id
            FROM models m JOIN training_sets_roi_set ts ON m.training_set_id = ts.training_set_id
            WHERE model_id = %s
        """, [classifier_id])
        (model_type_id, uri, species_id, songtype_id) = cursor.fetchone()
    return {
        'id': classifier_id,
        'model_type_id': model_type_id,
        'uri': uri,
        'species': species_id,
        'songtype': songtype_id,
    }

def get_playlist(db, playlist_id):
    recs = []
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT r.recording_id, r.uri, """ + _LEGACY_EXPR + """ legacy
            FROM recordings r JOIN playlist_recordings pr ON r.recording_id = pr.recording_id
            WHERE pr.playlist_id = %s
        """, [playlist_id])
        recs = [{"recording_id": r_id, "uri": uri, "legacy": legacy} for (r_id, uri, legacy) in cursor]
    return recs

def insert_rec_error(db, rec_id, job_id):
    error = traceback.format_exc()
    # mysql2pg W4-1 (2026-07-16 adversarial review): this is frequently called
    # from an except-branch after a prior statement failed. On PostgreSQL that
    # leaves the transaction aborted (InFailedSqlTransaction), so this INSERT
    # would fail too. Roll back any aborted txn first (no-op-safe on MySQL).
    try:
        db.rollback()
    except Exception:
        pass
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            INSERT INTO recordings_errors(recording_id, job_id, error)
            VALUES (%s, %s, %s)
        """, [rec_id, job_id, error])
        db.commit()