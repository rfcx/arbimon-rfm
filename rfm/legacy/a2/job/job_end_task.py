import shutil
import tasks
import a2.runtime as runtime


@runtime.tags.tag('task_type', 'job.end')
class JobEndTask(tasks.Task):
    "Task that clears any workspace data and marks the job as done"
    def run(self):

        shutil.rmtree(self.get_workspace_path(),  False)

        runtime.db.execute("""
            UPDATE jobs
            SET state = 'completed'
            WHERE job_id = %s
        """, [self.get_job_id()])
        

