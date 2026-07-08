"""Focused tests for the phase-2 batched process_results path.

These avoid the heavy runtime deps (joblib/opencv/sklearn/mysql) by stubbing
the modules that rfm.legacy.classify imports, then exercising the pure
batching logic against an in-memory fake DB. The goal is to prove:

  * classification_results are bulk-inserted via cursor.executemany
  * progress is advanced exactly once per result (valid OR error), batched
  * the final partial batch is flushed (exact totals)
  * a failing bulk insert falls back to per-row inserts (no lost rows/progress)
  * returned stats (t / minv / maxv) are unchanged vs the per-record version
"""
import os
import sys
import types
import importlib
import contextlib

import pytest

HERE = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(HERE, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _install_stubs():
    """Register light stand-ins for the heavy imports pulled in by classify.py
    so it can be imported in a bare test environment."""
    # joblib
    joblib = types.ModuleType('joblib')
    joblib.Parallel = lambda *a, **k: (lambda gen: list(gen))
    joblib.delayed = lambda fn: fn
    sys.modules.setdefault('joblib', joblib)

    # rfm.legacy.a2pyutils.logger.Logger
    logger_mod = types.ModuleType('rfm.legacy.a2pyutils.logger')

    class _Logger:
        def __init__(self, *a, **k):
            self.also_print = False

        def write(self, *a, **k):
            pass
    logger_mod.Logger = _Logger
    sys.modules['rfm.legacy.a2pyutils.logger'] = logger_mod

    # rfm.legacy.a2audio.recanalizer.Recanalizer
    reca_mod = types.ModuleType('rfm.legacy.a2audio.recanalizer')
    reca_mod.Recanalizer = object
    sys.modules['rfm.legacy.a2audio.recanalizer'] = reca_mod

    # rfm.legacy.db (only the names imported by classify.py)
    db_mod = types.ModuleType('rfm.legacy.db')
    # NB: keep this list in sync with classify.py's `from .db import ...`
    # line (PR #4 added ensure_connection; missing names break collection).
    for name in ('connect', 'ensure_connection', 'get_classification_job_data',
                 'get_model_params', 'get_playlist', 'set_progress_params',
                 'update_job_error'):
        setattr(db_mod, name, lambda *a, **k: None)

    _rec_errors = []

    def _insert_rec_error(db, rec_id, job_id):
        _rec_errors.append((rec_id, job_id))
    db_mod.insert_rec_error = _insert_rec_error
    db_mod._rec_errors = _rec_errors
    sys.modules['rfm.legacy.db'] = db_mod

    # rfm.legacy.storage
    storage_mod = types.ModuleType('rfm.legacy.storage')
    storage_mod.config = {'s3_bucket_name': 'b', 's3_legacy_bucket_name': 'lb'}

    def _upload_file(filen, uri):
        return None
    storage_mod.upload_file = _upload_file
    storage_mod.download_file = lambda *a, **k: None
    sys.modules['rfm.legacy.storage'] = storage_mod
    return db_mod


_install_stubs()
classify = importlib.import_module('rfm.legacy.classify')


class FakeCursor:
    def __init__(self, db):
        self.db = db

    def execute(self, sql, params=None):
        self.db.executes.append((sql, params))
        self.rows = []
        if 'FROM `classification_results`' in sql and 'SELECT `recording_id`' in sql:
            self.rows = [(rid,) for rid in self.db.existing_result_ids]
        if 'UPDATE `jobs`' in sql and 'progress' in sql:
            # params[0] is the increment; stays pending until commit (a
            # rollback discards it), mirroring real transactional semantics
            # the batching fallback relies on.
            self.db.pending_progress += params[0]

    def __iter__(self):
        return iter(getattr(self, 'rows', []))

    def executemany(self, sql, seq):
        if self.db.fail_executemany:
            raise RuntimeError('simulated bulk insert failure')
        self.db.pending_rows.extend(list(seq))

    def close(self):
        pass


class FakeDB:
    def __init__(self, fail_executemany=False, existing_result_ids=None):
        self.progress = 0
        self.existing_result_ids = set(existing_result_ids or [])
        self.inserted_rows = []
        self.executes = []
        self.commits = 0
        self.rollbacks = 0
        self.fail_executemany = fail_executemany
        self.pending_progress = 0
        self.pending_rows = []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1
        self.progress += self.pending_progress
        self.inserted_rows.extend(self.pending_rows)
        self.pending_progress = 0
        self.pending_rows = []

    def rollback(self):
        self.rollbacks += 1
        self.pending_progress = 0
        self.pending_rows = []


class FakeLog:
    def write(self, *a, **k):
        pass


@pytest.fixture(autouse=True)
def _no_real_io(monkeypatch, tmp_path):
    # write_vector returns a path; make it a no-op file so upload_vector runs.
    def fake_write_vector(uri, folder, featvector):
        p = os.path.join(str(tmp_path), uri.split('/')[-1] + '.vector')
        with open(p, 'w') as f:
            f.write('x')
        return p
    monkeypatch.setattr(classify, 'write_vector', fake_write_vector)
    # upload_vector: no network, just remove the temp file.
    def fake_upload_vector(uri, filen, rid, db, job_id):
        try:
            os.remove(filen)
        except OSError:
            pass
    monkeypatch.setattr(classify, 'upload_vector', fake_upload_vector)
    yield


def _mk_result(i, present=1):
    return {'uri': 'proj/rec_{}.opus'.format(i), 'id': 1000 + i,
            'f': [0.1 * i, 0.2 * i, 0.05], 'ft': [], 'r': present}


def test_bulk_insert_and_progress_exact():
    classify.RESULT_BATCH_SIZE = 10
    n = 25
    results = [_mk_result(i) for i in range(n)]
    db = FakeDB()
    stats = classify.process_results(results, '/tmp', 'model.mod', 42, 7, 3, db, FakeLog())

    # Every valid result inserted exactly once.
    assert len(db.inserted_rows) == n
    assert stats['t'] == n
    # Progress advanced exactly once per result.
    assert db.progress == n
    # Bulk path used: far fewer commits than records (batches of 10 -> 3 flushes).
    assert db.commits == 3
    # Inserted row shape matches the schema tuple order; parallel vector uploads
    # do not guarantee row order.
    rows_by_rec = {row[1]: row for row in db.inserted_rows}
    assert rows_by_rec[1000] == [42, 1000, 7, 3, 1, pytest.approx(0.05)]


def test_progress_counts_errored_records_too():
    classify.RESULT_BATCH_SIZE = 100
    # Mix valid results with None entries (errored classify_rec) and dict w/o id.
    results = [_mk_result(0), None, _mk_result(1), {'no_id': True}, _mk_result(2)]
    db = FakeDB()
    stats = classify.process_results(results, '/tmp', 'model.mod', 9, 1, 1, db, FakeLog())
    # Progress = total results seen (5), inserted = 3 valid.
    assert db.progress == 5
    assert stats['t'] == 3
    assert len(db.inserted_rows) == 3


def test_final_partial_batch_flushed():
    classify.RESULT_BATCH_SIZE = 10
    results = [_mk_result(i) for i in range(7)]  # < batch size
    db = FakeDB()
    classify.process_results(results, '/tmp', 'model.mod', 5, 2, 2, db, FakeLog())
    assert db.progress == 7
    assert len(db.inserted_rows) == 7
    assert db.commits == 1  # single final flush


def test_stats_minv_maxv():
    classify.RESULT_BATCH_SIZE = 100
    results = [
        {'uri': 'p/a.opus', 'id': 1, 'f': [-2.0, 3.0], 'ft': [], 'r': 1},
        {'uri': 'p/b.opus', 'id': 2, 'f': [1.0, 5.0], 'ft': [], 'r': 0},
    ]
    db = FakeDB()
    stats = classify.process_results(results, '/tmp', 'model.mod', 1, 1, 1, db, FakeLog())
    assert stats['stats']['minv'] == -2.0
    assert stats['stats']['maxv'] == 5.0


def test_bulk_failure_falls_back_per_row():
    classify.RESULT_BATCH_SIZE = 10
    db_mod = sys.modules['rfm.legacy.db']
    db_mod._rec_errors.clear()
    results = [_mk_result(i) for i in range(5)]
    db = FakeDB(fail_executemany=True)
    stats = classify.process_results(results, '/tmp', 'model.mod', 3, 1, 1, db, FakeLog())
    # Fallback path: executemany failed -> per-row insert_result_to_db runs,
    # which uses cursor.execute INSERT (recorded in db.executes), progress still
    # advanced via the catch-up update.
    assert stats['t'] == 5
    assert db.progress == 5
    insert_execs = [e for e in db.executes if 'INSERT INTO `classification_results`' in e[0]]
    assert len(insert_execs) == 5
    assert db.rollbacks >= 1

def test_vector_upload_failure_preserves_result_row(monkeypatch):
    classify.RESULT_BATCH_SIZE = 10
    classify.VECTOR_UPLOAD_WORKERS = 2
    classify.VECTOR_UPLOAD_MAX_OUTSTANDING = 4
    db_mod = sys.modules['rfm.legacy.db']
    db_mod._rec_errors.clear()

    def fail_upload(local_path, key):
        raise RuntimeError('simulated upload failure')
    monkeypatch.setattr(classify, 'upload_file', fail_upload)

    db = FakeDB()
    stats = classify.process_results([_mk_result(1)], '/tmp', 'model.mod', 77, 7, 3, db, FakeLog())

    # Legacy upload_vector swallowed upload errors, inserted a recordings_errors
    # row, and classification_results insertion still happened afterward.
    assert stats['t'] == 1
    assert db.progress == 1
    assert len(db.inserted_rows) == 1
    assert db_mod._rec_errors == [(1001, 77)]


def test_vector_write_failure_skips_result_row(monkeypatch):
    classify.RESULT_BATCH_SIZE = 10
    classify.VECTOR_UPLOAD_WORKERS = 2
    classify.VECTOR_UPLOAD_MAX_OUTSTANDING = 4
    db_mod = sys.modules['rfm.legacy.db']
    db_mod._rec_errors.clear()

    monkeypatch.setattr(classify, 'write_vector', lambda uri, folder, featvector: None)

    db = FakeDB()
    stats = classify.process_results([_mk_result(2)], '/tmp', 'model.mod', 88, 7, 3, db, FakeLog())

    # This matches the old localFile is None branch: progress advances and a
    # recording error is recorded, but no classification_results row is written.
    assert stats['t'] == 1
    assert db.progress == 1
    assert len(db.inserted_rows) == 0
    assert db_mod._rec_errors == [(1002, 88)]

def test_existing_results_are_skipped_but_count_for_progress_and_stats(monkeypatch):
    classify.RESULT_BATCH_SIZE = 10
    classify.VECTOR_UPLOAD_WORKERS = 2
    classify.VECTOR_UPLOAD_MAX_OUTSTANDING = 4
    uploaded = []

    def fake_upload(local_path, key):
        uploaded.append(key)
    monkeypatch.setattr(classify, 'upload_file', fake_upload)

    # Recording 1001 already has a committed classification_results row from a
    # previous phase-2 attempt. We still recompute/use its vector for stats and
    # progress, but must not upload/insert it again.
    db = FakeDB(existing_result_ids={1001})
    results = [_mk_result(0), _mk_result(1), _mk_result(2)]
    stats = classify.process_results(results, '/tmp', 'model.mod', 55, 7, 3, db, FakeLog())

    assert stats['t'] == 3
    assert db.progress == 3
    inserted_ids = {row[1] for row in db.inserted_rows}
    assert inserted_ids == {1000, 1002}
    assert len(uploaded) == 2
    assert stats['stats']['minv'] == pytest.approx(0.0)
    assert stats['stats']['maxv'] == pytest.approx(0.4)

