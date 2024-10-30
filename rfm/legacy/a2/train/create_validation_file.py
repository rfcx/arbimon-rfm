import json
import csv

import a2.job.tasks
import a2.runtime as runtime
import a2.runtime.tmp

import a2.audio.recording

import planner

@runtime.tags.tag('task_type', 'train.validations.create_file')
class CreateValidationFileTask(a2.job.tasks.Task):
    """Task that creates a validation file using the input of the reclinize tasks in the given step in this job.
        Inputs:[
            recnilizeTasksStep == resolves to ==> [
                recording_id,
                [species_id, songtype_id],
                present
            ]
        ]

        Output:
            s3://~/validations/job_{:job}.csv
    """
    def run(self):
        key = "project_{}/validations/job_{}.csv".format(
            self.get_project_id(),
            self.get_job_id()
        )
        
        with a2.runtime.tmp.tmpfile(suffix="csv") as tmpfile:
            csvwriter = csv.writer(tmpfile.file, delimiter=',')            
            for row in self.generate_validations():
                csvwriter.writerow(row)
            tmpfile.close_file()
            runtime.bucket.upload_filename(key, tmpfile.filename)
            
        self.insert_to_db(key)
        
    def get_model_name(self):
        return runtime.db.queryOne("""
            SELECT JP.name
            FROM `job_tasks` JT
            JOIN `job_params_training` JP ON JP.job_id = JT.job_id
            WHERE JT.`task_id` = %s
        """, [
            self.taskId
        ])['name']


    def insert_to_db(self, valiKey):
        modelName = self.get_model_name()
        project, user, job = self.get_project_id(), self.get_user_id(), self.get_job_id()
        with runtime.db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO `validation_set`(
                    `project_id`, `user_id`,
                    `name`, `uri`,
                    `params`, `job_id`
                ) VALUES (
                    %s, %s, %s, %s, %s, %s
                )
            """, [
                project, user, modelName+" validation", valiKey,
                json.dumps({'name': modelName}),
                job
            ])
            runtime.db.commit()

            cursor.execute("""
                UPDATE `job_params_training`
                SET `validation_set_id` = %s
                WHERE `job_id` = %s
            """, [cursor.lastrowid, job])
            runtime.db.commit()


    def generate_validations(self):
        recnilize_tasks_step = self.get_args()[0]
        for row in runtime.db.queryGen("""
            SELECT JT.args
            FROM job_tasks JT
            WHERE JT.job_id = %s
              AND JT.type_id = %s
              AND JT.step = %s
        """, [
            self.get_job_id(),
            planner.ANALIZE_RECORDINGS_TASK,
            recnilize_tasks_step
        ]):
            recording_id, (species, songtype), present, _ = json.loads(row['args'])
            recording = a2.audio.recording.Recording(recording_id)
            
            yield recording.get_uri(), species, songtype, present, recording_id

