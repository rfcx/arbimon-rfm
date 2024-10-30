"""
Job Planner abstract class module.
Defines the JobPlanner abstract class, wich contains utility functions for
adding job tasks.
"""
# pylint: disable=C0103, E0401, R0201

import json

import a2.runtime as runtime

JOB_PREPARE_WORKSPACE_TASK = 15
JOB_END_TASK = 9
SYNC_TASK = 8

class JobPlanner(object):
    """
    Abstract JobPlanner class.
    Provides an API and contains utility functions for adding job tasks.
    """
    def __init__(self, jobId, taskId):
        self.jobId = jobId
        self.taskId = taskId

    def plan(self):
        "Analizes and inserts the tasks needed for the job."
        pass

    def addTask(self, step, typeId, dependencies=None, args=None):
        "Adds a task of the given typeId, in the given step, with the given dependencies and args."
        task_id = runtime.db.insert("""
            INSERT INTO job_tasks(job_id, step, type_id, dependency_counter, status, remark, timestamp, args)
            VALUES (%s, %s, %s, %s, 'waiting', NULL, NOW(), %s)
        """, [
            self.jobId, step, typeId,
            len(dependencies or []),
            json.dumps(args) if args else None
        ])

        self.addTaskDependencies([task_id], dependencies)

        return task_id

    def addSyncTask(self, step, dependencies):
        "Adds a sync task, wich helps in waiting for parallel tasks to complete."
        return self.addTask(step, SYNC_TASK, dependencies)

    def addJobEndTask(self, step, dependencies):
        "Adds a task for cleaning up any temporary shared space, and marking the job as complete."
        return self.addTask(step, JOB_END_TASK, dependencies)

    def addPrepareWorkspaceTask(self, step, dependencies, folders=None):
        "Adds a task for setting up the temporary shared workspace."
        return self.addTask(step, JOB_PREPARE_WORKSPACE_TASK, dependencies, folders)

    def addTaskDependency(self, task_id, dependency_id):
        "Adds a given dependency to a given task."
        self.addTaskDependencies([task_id], [dependency_id])

    def addTaskDependencies(self, task_ids, dependency_ids):
        "Adds a list of dependencies to a list of tasks."
        if not task_ids or not dependency_ids:
            return

        for task_id in task_ids:
            for dependency_id in dependency_ids:
                runtime.db.insert("""
                    INSERT INTO job_task_dependencies(task_id, dependency_id, satisfied)
                    VALUES (%s, %s, 0)
                """, [task_id, dependency_id])

    def deleteStepsHigherThan(self, step):
        "Removes any tasks with steps above the given step."
        runtime.db.execute("""
            DELETE FROM job_tasks 
            WHERE job_id = %s and step > %s
        """, [self.jobId, step])
