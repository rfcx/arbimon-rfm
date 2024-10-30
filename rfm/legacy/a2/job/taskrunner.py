"""
TaskRunner class
"""
# pylint: disable=W0703, R0903, C0103

import multiprocessing
# import multiprocessing.pool
import traceback
import tasks

import a2.runtime.asyncpopen
import threading

class TaskRunner(object):
    "Class for handling the running of tasks"
    def __init__(self, config, max_concurrency, runner_script):
        self.max_concurrency = int(max_concurrency) or multiprocessing.cpu_count()
        self.config = config
        self.tasks = []
        self.runner_script = runner_script
        self.lock = threading.Lock()
        a2.runtime.asyncpopen.init()
        # self.pool = multiprocessing.pool.Pool(
        #     self.max_concurrency
        # )
        self.reporter_uri = None

    def run(self, task, callback=None):
        "Runs the given task in parallel, calling the callback afterwards"
        with self.lock:
            print "Running tasks :: ", len(self.tasks)
            if len(self.tasks) >= self.max_concurrency:
                raise AtMaximumConcurrencyError()
            
        entry = {'task':task}

        def resolve(retcode):
            "called when the tsk is resolved"
            print "resolved :: ", task
            try:
                with self.lock:
                    self.tasks.remove(entry)
                    print "Running tasks :: ", len(self.tasks)
            except ValueError:
                print "Task {} not in list while trying to remove it.".format(task)

            if callback:
                callback(retcode)

        with self.lock:
            self.tasks.append(entry)
            
        args = (
            tuple(self.runner_script 
             if type(self.runner_script) in (list, tuple) 
             else [self.runner_script]
            ) + 
            (str(task), )
        )
        
        entry['popen'] = a2.runtime.asyncpopen.popen(args, callback=resolve)

        return {
            "task": task
        }

class TaskRunnerError(StandardError):
    "An error associated to the task runnner."
    pass

class AtMaximumConcurrencyError(TaskRunnerError):
    "The task runner is at maximum concurrency."
    pass

def execute_task(taskId):
    "Executes the task of the given task id."
    try:
        print "Executing ze task...", taskId
        task = tasks.Task.fromTaskId(taskId)
        print "task :: ", task
        retval = task.run()
        task.finish()
        return True, retval
    except Exception:
        exc = traceback.format_exc()
        try:
            tasks.Task.markTaskAs(taskId, 'error', exc)
        except Exception:
            print "Exception caught while handling exception."
            traceback.print_exc()
            print "Original exception:\n", exc
        return False, exc

def sample_task():
    "sample task."
    import time
    import random
    import os

    pid = os.getpid()
    delay = int(random.uniform(1, 10))
    a = random.uniform(1, 10)
    b = random.uniform(1, 10)

    print "{} :: sleeping for {}s".format(pid, delay)
    time.sleep(delay)

    return a + b
