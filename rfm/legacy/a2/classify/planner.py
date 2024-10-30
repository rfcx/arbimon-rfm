import a2.job.planner
import a2.runtime as runtime

CLASSIFY_RECORDING_TASK = 3
CLASSIFY_STATISTICS_TASK = 5

@runtime.tags.tag('job.planner', 'classification')
class ClassificationJobPlanner(a2.job.planner.JobPlanner):
    def plan(self):
        # add statistics task
        with runtime.db.cursor() as cursor:
            # add classification tasks
            self.add_classify_tasks(cursor)
            # add statistics task
            self.add_statistics_task(cursor)
            runtime.db.commit()
            
    def add_classify_tasks(self, cursor):
        # one task per recording in playlist
        cursor.execute("""
            INSERT INTO job_tasks(job_id, step, type_id, dependency_counter, status, remark, timestamp, args)
            SELECT %s, %s, %s, 0, 'waiting', NULL, NOW(), CONCAT('[', PR.recording_id, ',', JPC.model_id ,']')
            FROM job_params_classification JPC
            JOIN playlist_recordings PR ON JPC.playlist_id = PR.playlist_id
        """, [
            self.jobId,
            1,
            CLASSIFY_RECORDING_TASK,
            
        ])

    def add_statistics_task(self, cursor):
        cursor.execute("""
            INSERT INTO job_tasks(job_id, step, type_id, dependency_counter, status, remark, timestamp, args)
            VALUES (%s, %s, %s, 1, 'waiting', NULL, NOW(), NULL)
        """, [
            self.jobId,
            2,
            CLASSIFY_STATISTICS_TASK
        ])
        resolution_task = cursor.lastrowid
        cursor.execute("""
            INSERT INTO job_task_dependencies(task_id, dependency_id, satisfied)
            SELECT %s, JT.task_id, 0
            FROM job_tasks JT
            WHERE JT.job_id = %s AND JT.step = 1
        """, [
            self.jobId,
            1
        ])
        cursor.execute("""
            UPDATE job_tasks 
            SET dependency_counter = (
                SELECT COUNT(*) 
                FROM job_task_dependencies JTD
                WHERE JTD.task_id = %s
            ) 
            WHERE task_id = %s
        """, [
            resolution_task,
            resolution_task
        ])

