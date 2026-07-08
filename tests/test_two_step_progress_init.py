"""Tests for the two-step, trigger-aware job progress init (OPEN-ITEMS #28
addendum, 2026-07-08).

arbimon2's jobs_BEFORE_UPDATE trigger force-sets state='completed' whenever
NEW.progress >= OLD.progress_steps. A single combined
`SET progress_steps=N, progress=0, state='processing'` UPDATE on a fresh row
(OLD.progress_steps=0) evaluates 0 >= 0 -> trigger overrides the state in the
same statement, mislabeling the whole run. The fix writes progress_steps in
its OWN UPDATE first, then progress/state in a second.

These tests exercise set_progress_params / set_progress_steps /
update_validations against a fake DB that EMULATES the trigger, proving:
  * the final row state is 'processing' (the old combined form yields
    'completed' under the same trigger emulation)
  * progress_steps is set before the state write (statement ordering)
"""
import os
import re
import sys
import types
from contextlib import closing

HERE = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(HERE, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# rfm.legacy.db imports mysql.connector + boto3 transitively via .storage?
# It imports json/os/contextlib and mysql.connector lazily? Check: it imports
# `mysql.connector` at module level -> stub it if absent.
try:
    import mysql.connector  # noqa: F401
except Exception:
    mysql_mod = types.ModuleType('mysql')
    connector_mod = types.ModuleType('mysql.connector')
    connector_mod.connect = lambda *a, **k: None
    connector_mod.Error = Exception
    mysql_mod.connector = connector_mod
    sys.modules.setdefault('mysql', mysql_mod)
    sys.modules.setdefault('mysql.connector', connector_mod)
try:
    import pymysql  # noqa: F401
except Exception:
    pymysql_mod = types.ModuleType('pymysql')
    pymysql_mod.connect = lambda *a, **k: None
    sys.modules.setdefault('pymysql', pymysql_mod)

from rfm.legacy import db as rfm_db  # noqa: E402


class TriggerEmulatingDB:
    """Minimal fake of the jobs row + the jobs_BEFORE_UPDATE trigger."""

    def __init__(self):
        # fresh job row shape (the dispatcher claims it at state=waiting)
        self.row = {'progress': 0, 'progress_steps': 0, 'state': 'waiting'}
        self.statements = []

    # -- connection surface used by the helpers -------------------------
    def cursor(self):
        return _Cursor(self)

    def commit(self):
        pass

    # -- trigger emulation ----------------------------------------------
    def apply_update(self, assignments):
        old_steps = self.row['progress_steps']
        new = dict(self.row)
        new.update(assignments)
        # jobs_BEFORE_UPDATE: IF NEW.progress >= OLD.progress_steps THEN
        #   SET NEW.state = 'completed'
        if new['progress'] >= old_steps:
            new['state'] = 'completed'
        self.row = new


class _Cursor:
    def __init__(self, fakedb):
        self.fakedb = fakedb

    def execute(self, sql, params=None):
        self.fakedb.statements.append(re.sub(r'\s+', ' ', sql).strip())
        assignments = {}
        m = re.search(r'`?progress_steps`?\s*=\s*%s', sql)
        params = list(params or [])
        if m:
            assignments['progress_steps'] = params.pop(0)
        if re.search(r'(?<!_)\bprogress\s*=\s*0', sql):
            assignments['progress'] = 0
        m = re.search(r'state\s*=\s*"(\w+)"', sql)
        if m:
            assignments['state'] = m.group(1)
        if assignments:
            self.fakedb.apply_update(assignments)

    def close(self):
        pass


def test_set_progress_params_survives_trigger():
    db = TriggerEmulatingDB()
    rfm_db.set_progress_params(db, 100, 42)  # progress_steps -> 100*2+5
    assert db.row['progress_steps'] == 205
    assert db.row['progress'] == 0
    assert db.row['state'] == 'processing', (
        'trigger flipped the state: init is not two-step / order is wrong')


def test_set_progress_steps_survives_trigger():
    db = TriggerEmulatingDB()
    rfm_db.set_progress_steps(db, 42, 500)
    assert db.row['progress_steps'] == 500
    assert db.row['state'] == 'processing'


def test_steps_written_before_state():
    db = TriggerEmulatingDB()
    rfm_db.set_progress_params(db, 10, 42)
    steps_stmt = next(i for i, s in enumerate(db.statements)
                      if 'progress_steps' in s)
    state_stmt = next(i for i, s in enumerate(db.statements)
                      if 'processing' in s)
    assert steps_stmt < state_stmt, 'progress_steps must be raised FIRST'
    assert 'processing' not in db.statements[steps_stmt], (
        'steps and state must be separate statements')


def test_old_combined_form_would_have_failed():
    """Sanity check on the emulation itself: the OLD single-statement form
    (steps + progress + state together) trips the trigger."""
    db = TriggerEmulatingDB()
    with closing(db.cursor()) as cursor:
        cursor.execute(
            'UPDATE `jobs` SET `progress_steps`=%s, progress=0, '
            'state="processing" WHERE `job_id` = %s', [205, 42])
    assert db.row['state'] == 'completed', (
        'emulated trigger should flip the combined form (proves the test '
        'actually exercises the bug)')
