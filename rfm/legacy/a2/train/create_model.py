import os
import json

import numpy

import a2.job.tasks
import a2.runtime as runtime
import a2.runtime.tmp

import a2.audio.model


@runtime.tags.tag('task_type', 'train.rf_model.create')
class CreateModelTask(a2.job.tasks.Task):
    """Task that creates a decision model from a validation dataset in efs.
        Inputs:[
            roi_class
        ]
        efs://~/{:class}/stats/*.npz
        efs://~/{:class}/surface.npz

        Output:
            s3://~/models/job_{:job}_{:class}.mod
            s3://~/validations/job_{:job}_vals.csv
    """
    def run(self):
        roi_class = "{species_id}_{songtype_id}".format(**self.get_args()[0])

        species, songtype = roi_class.split('_')
        
        base_path = self.get_workspace_path(roi_class)
        rois_path = os.path.join(base_path, 'rois')
        stats_path = os.path.join(base_path, 'stats')

        model_key = "project_{}/models/job_{}_{}.mod".format(
            self.get_project_id(),
            self.get_job_id(),
            roi_class
        )
        pngKey = "project_{}/models/job_{}_{}.png".format(
            self.get_project_id(),
            self.get_job_id(),
            roi_class
        )
        
        with numpy.load(os.path.join(base_path, 'surface.npz')) as surface:
            model = a2.audio.model.Model(roi_class, surface['roi'], self.get_job_id())
            
            tally = [0, 0]
            for statsfile in os.listdir(stats_path):
                with numpy.load(os.path.join(stats_path, statsfile)) as validation:
                    model.addSample(
                        validation['present'],
                        validation['features'],
                        validation['uri']
                    )
                    tally[int(bool(validation['present']))] += 1


            params = self.get_job_parameters(tally)

            model.splitData(params['use'][1][0], params['use'][0][0], params['use'][1][1], params['use'][0][1])

            model.train()

            if params['use'][1][1] > 0:
                model.validate()
                self.upload_validations(model)

            self.upload_model(model_key, model, surface)

            stats = self.compute_model_stats(
                model.modelStats(), surface, 
                pngKey, params['training_set_id']
            )

        #save model to DB
        self.save_model_to_db(
            params['training_set_id'], stats,
            params['modelname'], params['model_type_id'], model_key,
            params['project_id'], params['user_id'], params['valiId'],
            species, songtype
        )

    def get_job_parameters(self, tally):
        """Get params from database"""

        row = runtime.db.queryOne("""
            SELECT J.`project_id`, J.`user_id`, 
                JPT.`model_type_id`, JPT.`training_set_id`,
                JPT.`use_in_training_present`, JPT.`use_in_training_notpresent`,
                JPT.`use_in_validation_present`, JPT.`use_in_validation_notpresent`,
                VS.`params`, VS.`validation_set_id`
            FROM job_tasks JT
            JOIN `jobs` J ON J.`job_id` = JT.`job_id`
            JOIN `job_params_training` JPT ON JPT.`job_id` = J.`job_id`
            JOIN `validation_set` VS ON VS.`job_id` = J.`job_id`
            WHERE JT.`task_id` = %s
        """, [
            self.taskId
        ])

        decoded = json.loads(row['params'])
        params = {
            'project_id' : row['project_id'],
            'user_id' : row['user_id'],
            'model_type_id' : row['model_type_id'],
            'training_set_id' : row['training_set_id'],
            'use' : [
                [row['use_in_training_notpresent'], row['use_in_validation_notpresent']],
                [row['use_in_training_present'], row['use_in_validation_present']]
            ],
            'modelname' : decoded['name'],
            'valiId' : row['validation_set_id'],
        }

        for v_use, v_tally in zip(params['use'], tally):
            if sum(v_use) > v_tally:
                v_use[0] = min(v_tally - 1, v_use[0])
                v_use[1] = v_tally - v_use[0]

        return params


    def upload_model(self, model_key, model, surface):
        with a2.runtime.tmp.tmpfile(suffix=".mod") as tmpfile:
            tmpfile.close_file()
            model.save(tmpfile.filename, surface['fbounds'][0], surface['fbounds'][1], int(surface['max_cols']))
            runtime.bucket.upload_filename(model_key, tmpfile.filename, 'public-read')
        return model_key

    def upload_validations(self, model):
        validationsKey = 'project_{}/validations/job_{}_vals.csv'.format(
            self.get_project_id(), self.get_job_id()
        )

        with a2.runtime.tmp.tmpfile(suffix=".csv") as tmpfile:
            tmpfile.close_file()
            model.saveValidations(tmpfile.filename)
            runtime.bucket.upload_filename(validationsKey, tmpfile.filename, 'public-read')


    def compute_model_stats(self, modelStats, surface, pngKey, training_set_id):
        tset_stats = runtime.db.queryOne("""
            SELECT max(ts.`x2` -  ts.`x1`) as max_length, min(ts.`y1`) as min_freq, max(ts.`y2`) as max_freq,
                COUNT(*) as total
            FROM `training_set_roi_set_data` ts
            WHERE  ts.`training_set_id` =  %s
        """, [training_set_id])

        return {
            "roicount": tset_stats['total'], 
            "roilength": tset_stats['max_length'], 
            "roilowfreq": tset_stats['min_freq'], 
            "roihighfreq": tset_stats['max_freq'],
            "accuracy": modelStats[0],
            "precision": modelStats[1],
            "sensitivity": modelStats[2], 
            "forestoobscore": modelStats[3], 
            "roisamplerate": int(surface['sample_rate']), 
            "roipng": pngKey, 
            "specificity": modelStats[5], 
            "tp": modelStats[6], "fp": modelStats[7], 
            "tn": modelStats[8], "fn": modelStats[9], 
            "minv": modelStats[10], "maxv": modelStats[11]
        }


    def save_model_to_db(self, training_set_id, stats,
            modelname, model_type_id, modKey,
            project_id, user_id, valiId,
            species, songtype
        ):

        with runtime.db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO `models`(
                    `name`, `model_type_id`, `uri`, `date_created`, `project_id`, 
                    `user_id`, `training_set_id`, `validation_set_id`
                ) VALUES (
                    %s, %s, %s, NOW(), %s, %s, %s, %s
                )
            """, [
                modelname, model_type_id, modKey, project_id, 
                user_id, training_set_id, valiId
            ])
            
            insertmodelId = cursor.lastrowid

            cursor.execute("""
                INSERT INTO `model_stats`(`model_id`, `json_stats`) 
                VALUES (%s, %s)
            """, [insertmodelId, json.dumps(stats)])

            cursor.execute("""
                INSERT INTO `model_classes`(`model_id`, `species_id`, `songtype_id`) 
                VALUES (%s, %s, %s)
            """, [insertmodelId, species, songtype])

            cursor.execute("""
                UPDATE `job_params_training` 
                SET `trained_model_id` = %s
                WHERE `job_id` = %s
            """, [insertmodelId, self.get_job_id()])

            runtime.db.commit()
