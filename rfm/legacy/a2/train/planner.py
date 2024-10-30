import os.path
import a2.job.planner
import a2.runtime as runtime

EXTRACT_ROI_TASK = 10
ALIGN_ROIS_AND_CREATE_SURFACE_TASK = 11
ANALIZE_RECORDINGS_TASK = 12
CREATE_VALIDATION_FILE_TASK = 13
CREATE_RF_MODEL_TASK = 14

@runtime.tags.tag('job.planner', 'training')
class TrainingJobPlanner(a2.job.planner.JobPlanner):
    def plan(self):
        job = self.get_job_parameters()
        
        if job['model_type_id'] != 4:
            raise StandardError("Cannot train unkown model type.")

        species_songtypes = self.get_species_songtypes(job)
        
        self.deleteStepsHigherThan(0)

        prepTask = self.addPrepareWorkspaceTask(1, None, [
            '{}_{}/{}'.format(
                specie_songtype['species_id'], 
                specie_songtype['songtype_id'],
                subdir
            )
            for specie_songtype in species_songtypes
            for subdir in ['rois', 'stats']
        ])
        
        alignTasks = []
        for specie_songtype in species_songtypes:
            roiTasks = self.add_exract_roi_tasks(2, job, [prepTask], specie_songtype)
            alignTasks.append(self.addTask(3, ALIGN_ROIS_AND_CREATE_SURFACE_TASK, roiTasks, [
                specie_songtype['species_id'],
                specie_songtype['songtype_id']
            ]))

        syncTask1 = self.addSyncTask(4, alignTasks)
            
        recnilizeTasksStep = 5
        recnilizeTasks = self.add_analize_recording_tasks(recnilizeTasksStep, job, [syncTask1])
        
        syncTask = self.addSyncTask(6, recnilizeTasks)
        valTask = self.addTask(7, CREATE_VALIDATION_FILE_TASK, [syncTask], [
            recnilizeTasksStep
        ])

        rfTasks = []
        for specie_songtype in species_songtypes:
            rfTasks.append(self.addTask(8, CREATE_RF_MODEL_TASK, [valTask], [
                specie_songtype
            ]))
        
        self.addJobEndTask(9, rfTasks)
    
    def get_job_parameters(self):
        return runtime.db.queryOne("""
            SELECT J.`project_id`, J.`user_id`,
                JP.model_type_id, JP.training_set_id,
                JP.validation_set_id, JP.trained_model_id,
                JP.use_in_training_present,
                JP.use_in_training_notpresent,
                JP.use_in_validation_present,
                JP.use_in_validation_notpresent,
                JP.name
            FROM `jobs` J
            JOIN `job_params_training` JP ON JP.job_id = J.job_id
            WHERE J.`job_id` = %s
        """, [self.jobId])

    def get_species_songtypes(self, job):
        return runtime.db.query("""
            SELECT DISTINCT `species_id`, `songtype_id`
            FROM `training_set_roi_set_data`
            WHERE `training_set_id` = %s
        """, [job['training_set_id']])

    def add_exract_roi_tasks(self, step, job, dependencies, specie_songtype):
        # one task per roi in training set
        first_id, last_id = runtime.db.insertMany("""
            INSERT INTO job_tasks(`job_id`, `step`, `type_id`, `dependency_counter`, `status`, `remark`, `timestamp`, `args`)
            SELECT %s, %s, %s, %s, 'waiting', NULL, NOW(), CONCAT('[', 
                TSRSD.recording_id, ',',
                '[', 
                    TSRSD.species_id, ',', 
                    TSRSD.songtype_id, 
                '],[', 
                    TSRSD.x1, ',', 
                    TSRSD.x2, ',', 
                    TSRSD.y1, ',', 
                    TSRSD.y2,
                ']',
            ']')
            FROM training_set_roi_set_data TSRSD
            WHERE TSRSD.training_set_id = %s
              AND TSRSD.species_id = %s
              AND TSRSD.songtype_id = %s
        """, [
            self.jobId,
            step,
            EXTRACT_ROI_TASK,
            len(dependencies),
            job['training_set_id'],
            specie_songtype['species_id'],
            specie_songtype['songtype_id']
        ])
        
        taskIds = range(first_id, last_id + 1)
        
        self.addTaskDependencies(taskIds, dependencies)
        
        return taskIds

    def add_analize_recording_tasks(self, step, job, dependencies):
        limitsByPresence = [
            [1, job['use_in_training_present'] + job['use_in_validation_present']],
            [0, job['use_in_training_notpresent'] + job['use_in_validation_notpresent']]
        ]
        speciesSongtypes = runtime.db.queryGen("""
            SELECT DISTINCT `species_id`, `songtype_id`
            FROM `training_set_roi_set_data`
            WHERE `training_set_id` = %s
        """, [job['training_set_id']])
        tasks = []
        
        for speciesSongtype in speciesSongtypes:
            for presence, sampleLimit in limitsByPresence:
                first_id, last_id = runtime.db.insertMany("""
                    INSERT INTO job_tasks(`job_id`, `step`, `type_id`, `dependency_counter`, `status`, `remark`, `timestamp`, `args`)
                    SELECT %s, %s, %s, %s, 'waiting', NULL, NOW(), CONCAT('[', 
                        RV.recording_id, ',', 
                        '[', RV.species_id, ',', RV.songtype_id, '],', 
                        RV.present, ',',
                        %s,
                    ']')
                    FROM `recording_validations` RV
                    WHERE RV.`project_id` = %s
                      AND `species_id` = %s
                      AND `songtype_id` = %s
                      AND `present` = %s
                      ORDER BY rand()
                      LIMIT %s
                """, [
                    self.jobId, step,
                    ANALIZE_RECORDINGS_TASK,
                    len(dependencies),
                    job['model_type_id'],
                    job['project_id'],
                    speciesSongtype['species_id'], speciesSongtype['songtype_id'],
                    presence, sampleLimit
                ])                
                # one task per roi in validation set
                tasks.extend(xrange(first_id, last_id + 1))
        
        self.addTaskDependencies(tasks, dependencies)    
        
        return tasks
