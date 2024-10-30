import MySQLdb
import MySQLdb.cursors
from contextlib import closing
import tempfile
import traceback
import os.path
import shutil
import dill
import a2pyutils.plan
import a2pyutils.plan_runner


def pickleable():
    def register_pickler(C):
        dill.register(C)(C.pickle)
        return C
    return register_pickler


class JobError(StandardError):
    def __init__(self, message, chain=None):
        if chain:
            message += (
                '\ncaused by :\n' + 
                traceback.format_exc()
            )
        super(JobError, self).__init__(message)

class Job(object):
    def __init__(self, job_id, log=None, configuration=None, job_data=None):
        """Contructs a new job instance"""
        self.job_id = job_id
        self.job_canceled=False
        self.db = None
        self.plan = None
        self.log = log
        self.configuration = configuration
        self.config = configuration.data()
        self.bucket_name = self.config[4]
        self.working_folder = None
        
        if job_data:
            self.restore_job_data(job_data)
        else:
            self.fetch_job_data()
            self.log.write('job data fetched.')


    def restore_job_data(self):
        """Restores the job's parameters and associated data."""
        pass

    def fetch_job_data(self):
        """Fetches the job's parameters and associated data."""
        pass

    def get_db(self):
        """Returns a database connection instance."""
        if self.db:
            return self.db
            
        try:
            self.db = MySQLdb.connect(
                host=self.config[0], user=self.config[1], 
                passwd=self.config[2], db=self.config[3],
                cursorclass=MySQLdb.cursors.DictCursor
            )
        except MySQLdb.Error as e:
            raise JobError("cannot connect to database.")
        if not self.db:
            raise JobError("cannot connect to database.")

        return self.db
    
    def close_db(self):
        if self.db:
            self.db.close()
            self.db = None
    
    def run(self):
        """Runs the job"""
        if not self.plan:
            self.plan_run()
            
        self.prepare_run()
            
        result = a2pyutils.plan_runner.PlanRunner(
            self.plan,
            self.log,
            self.update_progress,
            self.check_job_cancel_status
        ).execute()
        
        self.finish_run()
        
        return result
    
    def update_progress(self, progress, relative = False):
        db = self.get_db()
        with closing(db.cursor()) as cursor:
            cursor.execute("""
                UPDATE `jobs`
                SET `progress` = """ + (
                    "`progress` + %s" if relative else "%s"
                )+ """
                WHERE `job_id` = %s
            """, [
                progress,
                self.job_id
            ])
            db.commit()       


        
    def finish_run(self):
        "finishes the current run. setting the completed state and so on"
        db = self.get_db()
        with closing(db.cursor()) as cursor:
            cursor.execute("""
                UPDATE `jobs`
                SET `progress` = `progress_steps`, `completed` = 1,
                    state="completed", `last_update` = now()
                WHERE `job_id` = %s
            """, [self.job_id])
            db.commit()
    
    def plan_run(self):
        """Plans the job, computing the number of steps and so on."""
        if not self.plan:
            self.plan = a2pyutils.plan.Plan()
            
        self.plan.prepend({
            "name":"setup working environment",
            "fn"  : self.setup_work_environment
        })
            
        self.plan.append({
            "name":"tear down working environment",
            "fn"  : self.tear_down_work_environment,
            "inputs": [[-1, True]]
        })
        
        self.plan.compute_plan_cost()
            
        return self.plan

        
    def prepare_run(self):
        """Prepares the working environment for the job to run."""
        try:
            db = self.get_db()
            
            with closing(db.cursor()) as cursor:
                cursor.execute("""
                    UPDATE `jobs`
                    SET `progress_steps`=%s, progress=0, state="processing"
                    WHERE `job_id` = %s
                """, [
                    self.plan.cost, self.job_id
                ])
                db.commit()
        except:
            raise JobError("Could not set progress params")
        
    def fetch_playlist_recordings(self):
        "Fetches the playlist associated to this job."
        self.playlist_recordings = []
        db = self.get_db()
        
        try:
            with closing(db.cursor()) as cursor:
                cursor.execute("""
                    SELECT R.`recording_id`, R.`uri`
                    FROM `recordings` R, `playlist_recordings` PR
                    WHERE R.`recording_id` = PR.`recording_id`
                      AND PR.`playlist_id` = %s
                """, [self.playlist_id])
                db.commit()
                numrows = int(cursor.rowcount)
                for x in range(0, numrows):
                   rowclassification = cursor.fetchone()
                   self.playlist_recordings.append(rowclassification)
        except:
            raise JobError("Could not generate playlist array")
        if len(self.playlist_recordings) < 1:
            raise JobError('No recordngs in playlist')
            
        return self.playlist_recordings

    def setup_work_environment(self, step, inputs):
        self.mk_working_folder()
        self.log.write('created working directory.')

    def tear_down_work_environment(self, step, inputs):
        shutil.rmtree(self.working_folder)
        self.log.write('removed folder.')
        return inputs
        
    def get_working_folder(self):
        self.working_folder = os.path.join(
            tempfile.gettempdir(),
            "job_" + str(self.job_id)
        )
        return self.working_folder
        

    def mk_working_folder(self):
        try:
            self.get_working_folder()
            
            if os.path.exists(self.working_folder):
                shutil.rmtree(self.working_folder)
            os.makedirs(self.working_folder)
        except:
            raise JobError("Could not create temporary directory")

        if not os.path.exists(self.working_folder):
            raise JobError('Fatal error creating directory')

        return self.working_folder

    def check_job_cancel_status(self, rmFolder=None, quitj=True):
        job, db = self.job_id, self.get_db()
        with closing(db.cursor()) as cursor:
            cursor.execute("SELECT `state`, `cancel_requested` FROM `jobs` WHERE `job_id` = %s", [job])
            row = cursor.fetchone()
            if row:
                should_cancel = int(row['cancel_requested']) > 0
                is_canceled = row['state'] == 'canceled'
                if should_cancel and not is_canceled:
                    cursor.execute('UPDATE `jobs` SET `state`="canceled" WHERE `job_id` = %s', [job])
                    db.commit()
                    is_canceled = True                    
            else:
                is_canceled = True
            
            self.canceled = is_canceled
            
            return is_canceled
                
                
