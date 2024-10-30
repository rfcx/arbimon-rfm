import a2pyutils.job
import a2pyutils.plan
import a2audio.rec
import a2audio.segmentation
import contextlib
import dill
import json
import matplotlib.mlab

@a2pyutils.job.pickleable()
class AudioEventDetectionJob(a2pyutils.job.Job):
        
    def __init__(self, job_id, log=None, configuration=None, job_data=None, aed_id=None):
        super(AudioEventDetectionJob, self).__init__(job_id, log, configuration)
        self.aed_id = aed_id
        
        if job_data:
            self.name = job_data.get("name")
            self.project_id = job_data.get("project_id")
            self.user_id = job_data.get("user_id")
            self.algorithm = job_data.get("algorithm")
            self.playlist_id = job_data.get("playlist_id")
            self.configuration_id = job_data.get("configuration_id")
            self.params = job_data.get("parameters")
            self.statistics = job_data.get("statistics")

    def plan_run(self):
        self.fetch_playlist_recordings()
        self.log.write('playlist generated.')
        
        self.plan = a2pyutils.plan.Plan(
            {"name":"creating new audio event detection",
             "fn"  : self.create_aed_entry,
             "cost" : 1
            },
            {"name":"process recordings",
             "steps": len(self.playlist_recordings),
             "parallelizable" : True,
             "data": self.playlist_recordings,
             "fn"  : self.process_recording,
             "cost" : 4
            }
        )
        
        super(AudioEventDetectionJob, self).plan_run()
        
        return self.plan

    def get_segmenter(self):
        return a2audio.segmentation.AudioSegmenter.instantiate(self.algorithm, **self.params)

    def get_stats_computer(self):
        return a2audio.segmentation.stats.MultipleSCStatsCalculator([
            # a2audio.segmentation.stats.FitEllipseRoiStatsCalculator(), # ('mux','muy','Sxx','Syy','Sxy')
            a2audio.segmentation.stats.MaxPointRoiStatsCalculator(), # ('x_max','y_max')
            a2audio.segmentation.stats.CoverageRoiStatsCalculator() # ('Cov','dur','bw')
        ])
        
    def fetch_recording(self, recording):
        return a2audio.rec.Rec(
            recording['uri'], 
            self.get_working_folder(),
            self.bucket_name,
            self.log
        )
        
    def get_spectrogram(self, rec):
        return matplotlib.mlab.specgram(rec.original, NFFT=512, Fs=rec.sample_rate, noverlap=256)

    def process_recording(self, subindex, data, step, inputs):
        print("#{} : {}".format(subindex, data['uri']))
        
        segmenter = self.get_segmenter()
        roi_stats_computer = self.get_stats_computer()
        
        roi_count = 0
        
        recording = self.fetch_recording(data)            
        spectrum, freqs, times = self.get_spectrogram(recording)

        duration = recording.samples * 1.0 / recording.sample_rate  # seconds
        max_freq = recording.sample_rate / 2.0
        specH, specW = spectrum.shape
        p2sec = duration / specW
        p2hz = max_freq / specH
        origin, scale = [0, 0], (p2sec, p2hz)
        
        with RoiAdder(self.get_db(), self.job_id, self.aed_id, data['recording_id']) as roi_adder:
            for i, roi in enumerate(segmenter.segment(
                spectrum, storage=None, sample_rate=recording.sample_rate
            )):
                roi_count += 1
                y0, x0, y1, x1 = roi.bounds
                w, h = x1 - x0 + 1, y1 - y0 + 1
                t0, t1 = x0 * p2sec, x1 * p2sec
                f0, f1 = y0 * p2hz, y1 * p2hz
                roidata = {
                    'idx':i, 'x':x0, 'y':specH-y1, 'w':w, 'h':h,
                    't0':t0, 'f0':f0, 't1':t1, 'f1':f1
                }
                origin[1] = y0
                roidata.update(roi_stats_computer(roi, origin, scale))

                roi_adder.add(roidata)
        print("#{} : {} rois".format(subindex, roi_count))

        return [subindex, roi_count]

    def create_aed_entry(self, step, inputs):
        if self.aed_id:
            return self.aed_id
            
        db = self.get_db()
        with contextlib.closing(db.cursor()) as cursor:
            cursor.execute("""
                INSERT INTO audio_event_detections(configuration_id, project_id, name, playlist_id, statistics, date_created)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, [self.configuration_id, self.project_id, self.name, self.playlist_id, json.dumps(self.statistics)])
            self.aed_id = cursor.lastrowid
            db.commit()
        return self.aed_id

    def fetch_job_data(self):
        try:
            with contextlib.closing(self.get_db().cursor()) as cursor:
                cursor.execute("""
                    SELECT J.`project_id`, J.`user_id`,
                        JP.`configuration_id`,
                        AEDC.`parameters`, 
                        AEDA.`name` as algorithm,
                        JP.`statistics`,
                        JP.`playlist_id`,
                        JP.`name`
                    FROM `jobs` J
                    JOIN `job_params_audio_event_detection` JP ON JP.`job_id` = J.`job_id`
                    JOIN `audio_event_detection_algorithm_configurations` AEDC ON JP.`configuration_id` = AEDC.`aedc_id`
                    JOIN `audio_event_detection_algorithms` AEDA ON AEDC.`algorithm_id` = AEDA.`id`
                    WHERE J.`job_id` = %s
                """, [self.job_id])
                row = cursor.fetchone()
        except StandardError, e:
            raise a2pyutils.job.JobError("Could not query database with audio event detection job #{}".format(self.job_id), e)
            
        if not row:
            raise a2pyutils.job.JobError("Could not find classification job #{}".format(self.job_id))
            
        row['parameters'] = json.loads(row['parameters'])
        row['statistics'] = json.loads(row['statistics'])
        self.restore_job_data(row)

    def restore_job_data(self, data):
        """Restores the job's parameters and associated data."""
        
        self.name = data['name']
        self.project_id = data['project_id']
        self.user_id = data['user_id']
        self.configuration_id = data['configuration_id']
        self.algorithm = data['algorithm']
        self.playlist_id = data['playlist_id']
        self.params = data['parameters']
        self.statistics = data['statistics']
            
    def insert_rec_error(self, db, recId, jobId):
        try:
            with contextlib.closing(db.cursor()) as cursor:
                cursor.execute("""
                    INSERT INTO `recordings_errors`(`recording_id`, `job_id`)
                    VALUES (%s, %s)
                """, [recId, jobId])
                db.commit()
        except:
            exit_error("Could not insert recording error")
            
    def insert_result_to_db(self, config, jId, recId, species, songtype, presence, maxV):
        db = None
        try:
            db = self.get_db()
            with contextlib.closing(db.cursor()) as cursor:
                cursor.execute("""
                    INSERT INTO `classification_results` (
                        job_id, recording_id, species_id, songtype_id, present,
                        max_vector_value
                    ) VALUES (%s, %s, %s, %s, %s,
                        %s
                    )
                """, [jId, recId, species, songtype, presence, maxV])
                db.commit()
        except:
            self.insert_rec_error(db, recId, jId)
        db.close()

    @staticmethod
    def unpickle(job_id, log, configuration, aed_id, job_data):
        return AudioEventDetectionJob(job_id, log, configuration, job_data, aed_id)

    @classmethod
    def pickle(cls, pickler, job):
        pickler.save_reduce(cls.unpickle, (
            job.job_id, 
            job.log,
            job.configuration, 
            job.aed_id,
            {
                "name" : job.name,
                "project_id" : job.project_id,
                "user_id" : job.user_id,
                "algorithm" : job.algorithm,
                "playlist_id" : job.playlist_id,
                "configuration_id" : job.configuration_id,
                "parameters" : job.params,
                "statistics" : job.statistics,
            }
        ), obj=job)


class RoiAdder(object):
    def __init__(self, db, job_id, aed_id, recording_id):
        self.db = db
        self.cursor=None
        self.job_id = job_id
        self.aed_id = aed_id
        self.recording_id = recording_id

    def __enter__(self):
        self.cursor = self.db.cursor()
        return self

    def __exit__(self, _type, value, traceback):
        self.cursor.close()
        self.db.commit()
        
    def add(self, roi):
        self.cursor.execute("""
            INSERT INTO recording_audio_events(recording_id, aed_id, t0, t1, f0, f1, bw, dur, area, coverage, max_y)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [
            self.recording_id, self.aed_id,
            roi['t0'], roi['t1'], roi['f0'], roi['f1'],
            roi['bw'], roi['dur'], roi['area'], roi['Cov'], 
            roi['y_max']
        ])

