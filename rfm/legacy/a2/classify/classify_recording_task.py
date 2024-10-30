"""
    Module encapsulating the ClassifyRecordingTask class.
"""
import csv
import pickle
import os.path
import tasks
import a2.runtime as runtime
import a2audio.recanalizer

def noarg_memoized(func):
    "Memoizes a noarg function"
    mem = {}

    def memoized_fn():
        "memoized function"
        if 'val' not in mem:
            mem['val'] = func()
        return mem['val']

    memoized_fn.__name__ = func.__name__
    memoized_fn.__doc__ = func.__doc__

    return memoized_fn

@runtime.tags.tag('task_type', 'classify.recording')
class ClassifyRecordingTask(tasks.Task):
    """ ClassifyRecordingTask
        Task that runs a species classification model through a recording and writes
        its results to the database, along with the generated feature vector in the bucket.
    """

    model = None
    recording_id = None
    model_id = None

    def run(self):
        "Runs the classify recording task"
        self.recording_id, self.model_id = self.get_args()

        with runtime.tmpdir.tmpdir() as tmpdir:
            # get model
            model = self.get_model()
            # compute features and classify
            featvector, fets, res = self.compute_features(
                model,
                self.get_recording_uri(),
                tmpdir
            )
            # store results and output
            self.process_results(tmpdir, featvector, fets, res)

    def process_results(self, tmpdir, featvector, fets, res):
        "Processes the results of the classification and inserts them to database"


        maxv = max(featvector)
        minv = min(featvector)

        self.upload_vector(featvector, tmpdir)

        self.insert_result_to_db(
            res[0],
            maxv, minv
        )


    def upload_vector(self, featvector, tmpdir):
        "Uploads the feature vector to the bucket"
        rec_uri = self.get_recording_uri()
        rec_name = rec_uri.split('/')[:-1]

        model_data = self.get_model_data()

        vector_uri = '{}/classification_{}_{}.vector'.format(
            model_data['uri'].replace('.mod', ''), self.jobId, rec_name
        )

        vector_file = self.write_vector(rec_uri, tmpdir, featvector)

        k = runtime.bucket.get_bucket().get_key(vector_uri)
        k.set_contents_from_filename(vector_file)
        k.set_acl('public-read')
        os.remove(vector_file)

    def insert_result_to_db(self, presence, maxv, minv):
        "Inserts the results in the database"
        model_data = self.get_model_data()

        runtime.db.insert("""
            INSERT INTO `classification_results` (
                job_id, recording_id, species_id, songtype_id, present,
                max_vector_value
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            )
        """, [
            self.jobId, self.recording_id,
            model_data['species'], model_data['songtype'],
            presence, maxv, minv
        ])
        
    @noarg_memoized
    def get_model_data(self):
        "Returns the model's data and parameters"
        return runtime.db.queryOne("""
            SELECT M.`model_type_id`, M.`uri`, 
                TS.`species_id` as species, TS.`songtype_id` as songtype
            FROM `models` M
            JOIN `training_sets_roi_set` TS ON M.`training_set_id` = ts.`training_set_id`
            WHERE `model_id` = %s
        """, [self.model_id])

    @noarg_memoized
    def get_recording_uri(self):
        "returns the recording's uri"
        recording_data = runtime.db.queryOne("""
            SELECT uri FROM recordings WHERE recording_id = %s
        """, [self.recording_id])
        return recording_data['uri'] if recording_data else None

    @noarg_memoized
    def get_model(self):
        "Fetches the model from the bucket and returns it"
        model_uri = self.get_model_data()['uri']
        k = runtime.bucket.get_bucket().get_key(model_uri, validate=False)
        self.model = pickle.loads(k.get_contents_as_string())

        return self.model,


def write_vector(rec_uri, tmpdir, featvector):
    "Writes the vector to a file in the given folder"
    rec_name = rec_uri.split('/')[-1]
    vector_file = os.path.join(tmpdir, rec_name + '.vector')
    with open(vector_file, 'wb') as fout:
        writer = csv.writer(fout)
        writer.writerow(featvector)

    return vector_file

def compute_features(model, recording_uri, tmpdir):
    "Runs the models against the recording and returns the results"
    use_ssim = True
    old_model = False
    use_ransac = False

    b_index = 0

    if len(model) > 7:
        b_index = model[7]
    if len(model) > 6:
        use_ransac = model[6]
    if len(model) > 5:
        use_ssim = model[5]
    else:
        old_model = True

    analizer = a2audio.recanalizer.Recanalizer(
        recording_uri,
        model[1], float(model[2]), float(model[3]),
        tmpdir,
        runtime.config.get_config().awsConfig['bucket_name'],
        None,
        False,
        use_ssim
    )

    featvector = analizer.getVector()
    fets = analizer.features()
    res = model[0].predict(fets)

    return featvector, fets, res
