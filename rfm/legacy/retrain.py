import sys
import os
import csv
import multiprocessing
import traceback
import shutil
import numpy
import png
import tempfile
from contextlib import closing
from joblib import Parallel, delayed
from pylab import *

from .a2audio.model import Model
from .a2audio.roiset import Roiset
from .a2audio.training import recnilize, roigen
from .a2pyutils.logger import Logger
from .db import connect, get_training_job, get_training_job_params, get_training_data, get_validation_data, update_job_error, update_job_last_update, update_job_progress, update_validations, get_retraining_job
from .storage import upload_file, download_file

num_cores = multiprocessing.cpu_count()

def exit_error(db, log, job_id, msg):
    log.write(msg)
    update_job_error(db, msg, job_id)
    remove_working_folder(job_id)
    sys.exit(-1)

def exit_error_no_db(log, job_id, msg):
    log.write(msg)
    remove_working_folder(job_id)
    sys.exit(-1)

def get_working_folder(job_id):
    temp_folder = tempfile.gettempdir()
    working_folder = temp_folder+"/training_"+str(job_id)+"/"
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)
    return working_folder

def remove_working_folder(job_id):
    working_folder = get_working_folder(job_id)
    if os.path.exists(working_folder):
        shutil.rmtree(working_folder)

def write_training_data(training_set_id, job_id, working_folder, training_data):
    training_file_name = os.path.join(
        working_folder,
        'training_{}_{}.csv'.format(job_id, training_set_id)
    )
    # write training file to temporary folder
    with open(training_file_name, 'w') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for row in training_data:
            spamwriter.writerow(row + (job_id,))

# return format
# 0 = uri
# 1 = species_id
# 2 = songtype_id
# 3 = present
# 4 = species_id_songtype_id
# 5 = recording_id
# 6 = legacy (1, 0)
def read_trained_data_csv(path):
    trained_data = []
    with open(path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        # row indexes
        # 0 = uri
        # 1 = present
        # 2 = NA
        # 3 = validation/training
        for row in reader:
            if row[3] == 'training':
                legacy = 1 if row[0][:8] == 'project_' else 0
                trained_data.append([row[0], 0, 0, row[1], '0', 0, legacy])
    return trained_data
    

def retrain(job_id: int):
    log = Logger(job_id, 'retrain.py', 'main')
    log.also_print = True

    db = connect()
    
    # get trained job
    try:
        (trained_job_id) = get_retraining_job(db, job_id)
    except Exception:
        log.write("could not find retraining job #{}".format(job_id))
        sys.exit(-1)
    
    # get job from trained_job_id
    try:
        (project_id, user_id, model_type_id, training_set_id, model_name) = get_training_job(db, trained_job_id)
    except Exception:
        log.write("could not find trained job #{}".format(job_id))
        sys.exit(-1)
    log.write("project_id={} training_set_id={} model_name={}".format(project_id, training_set_id, model_name))
    if model_type_id != 4:
        log.write("unknown model type requested")
        sys.exit(-1)

    # creating a temporary folder
    remove_working_folder(job_id)
    working_folder = get_working_folder(job_id)
    if not os.path.exists(working_folder):
        exit_error(db, log, job_id, 'cannot create temporary directory')

    # training data file creation
    try:
        training_data, species_songtypes = get_training_data(db, training_set_id)
    except Exception:
        exit_error(db, log, job_id, 'cannot create training csvs files or access training data from db. {}'.format(traceback.format_exc()))
    progress_steps = len(training_data)
    
    """Roigenerator"""
    try:
        # roigen defined in a2audio.training
        rois = Parallel(n_jobs=num_cores)(delayed(roigen)(line,working_folder,job_id,log) for line in training_data)
    except Exception:
        exit_error(db, log, job_id, 'roigenerator failed. {}'.format(traceback.format_exc()))

    if len(rois) == 0:
        exit_error(db, log, job_id, 'cannot create rois from recordings')
        
    log.write('rois created')
    classes: dict[str, Roiset] = {}
    pattern_surfaces = {}
    models = {}

    """Align rois"""
    try:
        for roi in rois:
            if 'err' not in roi:
                classid = roi[1]
                lowFreq = roi[0].lowF
                highFreq = roi[0].highF
                sample_rate = roi[0].sample_rate
                spec = roi[0].spec
                rows = spec.shape[0]
                columns = spec.shape[1]
                if classid in classes:
                    classes[classid].addRoi(
                        float(lowFreq),
                        float(highFreq),
                        float(sample_rate),
                        spec,
                        rows,
                        columns
                    )
                else:
                    classes[classid] = Roiset(classid, float(sample_rate))
                    classes[classid].addRoi(
                        float(lowFreq),
                        float(highFreq),
                        float(sample_rate),
                        spec,
                        rows,
                        columns
                    )

        for i in classes:
            classes[i].alignSamples()
            pattern_surfaces[i] = [
                classes[i].getSurface(),
                classes[i].setSampleRate,
                classes[i].lowestFreq,
                classes[i].highestFreq,
                classes[i].maxColumns
            ]
            models[i] = Model(i,pattern_surfaces[i][0],job_id)
    except Exception:
        exit_error(db, log, job_id, 'cannot align rois. {}'.format(traceback.format_exc()))

    if len(pattern_surfaces) == 0:
        exit_error(db, log, job_id, 'cannot create pattern surface from rois')
    log.write('rois aligned, pattern surface created')
    
    """Download trained data from csv"""
    try:
        trainedKey = 'project_'+str(project_id)+'/validations/job_'+str(trained_job_id)+'_vals.csv'
        trained_local_path = working_folder + f'/trained_data.csv'
        download_file(trainedKey, trained_local_path)
    except Exception:
        exit_error_no_db(log, job_id, 'cannot download trained data from s3 or read trained data csv')
    
    """Get trained data from csv"""
    try:
        trained_data = read_trained_data_csv(trained_local_path)
        print(str(trained_data))
    except Exception:
        exit_error_no_db(log, job_id, 'cannot download trained data from s3 or read trained data csv')

    """ Create and save model """
    savedModel = False
    for i in models:
        """Recnilize"""
        try:
            results = Parallel(n_jobs=num_cores)(delayed(recnilize)(line,working_folder,job_id,(pattern_surfaces[i]),log,True,False,True) for line in trained_data)
        except Exception as e:
            print(f"An error occurred: {e}")
            exit_error(db, log, job_id, 'cannot analyze recordings in parallel {}'.format(traceback.format_exc()))
        log.write('validation recordings analyzed')
        # Add sample from csv file in s3
        log.write('total results '+str(len(results)))
        no_errors = 0
        errors_count = 0
        try:
            for res in results:
                if 'err' not in res:
                    no_errors = no_errors + 1                               
                    models[i].addSample(res['info'][1],res['fets'],res['info'][6])
                else:
                    errors_count = errors_count + 1
        except Exception:
            exit_error(db, log, job_id, 'cannot add samples to model. {}'.format(traceback.format_exc()))
        log.write('errors: '+str(errors_count)+" processed: "+ str(no_errors))
        
        # Train model
        try:
            models[i].retrain()
        except Exception:
            exit_error(db, log, job_id, 'error training model. {}'.format(traceback.format_exc()))
        
        # Create mod file   
        modFile = working_folder+"model_"+str(trained_job_id)+"_"+str(i)+".mod"
        try:
            models[i].save(modFile,pattern_surfaces[i][2] ,pattern_surfaces[i][3],pattern_surfaces[i][4])
        except Exception:
            exit_error(db, log, job_id, 'error saving model file to local storage. {}'.format(traceback.format_exc()))
        
        pngKey = None
        try:
            pngFilename = working_folder+'job_'+str(trained_job_id)+'_'+str(i)+'.png'
            pngKey = 'project_'+str(project_id)+'/models/job_'+str(trained_job_id)+'_'+str(i)+'.png'
            specToShow = numpy.zeros(shape=(0,int(pattern_surfaces[i][0].shape[1])))
            rowsInSpec = pattern_surfaces[i][0].shape[0]
            spec = pattern_surfaces[i][0]
            spec[spec == -10000] = float('nan')
            for j in range(0,rowsInSpec):
                if abs(sum(spec[j,:])) > 0.0:
                    specToShow = numpy.vstack((specToShow,spec[j,:]))
            specToShow[specToShow[:,:]==0] = numpy.min(numpy.min(specToShow))
            smin = min([min((specToShow[j])) for j in range(specToShow.shape[0])])
            smax = max([max((specToShow[j])) for j in range(specToShow.shape[0])])
            x = 255*(1-((specToShow - smin)/(smax-smin)))
            png.from_array(x, 'L;8').save(pngFilename)
        except Exception:
            exit_error(db, log, job_id, 'error creating pattern PNG. {}'.format(traceback.format_exc()))
        
        modKey = None  
        log.write('uploading files')
        try:
            #save model file to bucket
            modKey = 'project_'+str(project_id)+'/models/job_'+str(trained_job_id)+'_'+str(i)+'.mod'
            upload_file(modFile, modKey)
            #save vocalization surface png to bucket
            upload_file(pngFilename, pngKey)
        except Exception:
            exit_error(db, log, job_id, 'error uploading files to bucket. {}'.format(traceback.format_exc()))

    if savedModel :
        log.write("model saved")
    else:
        exit_error(db, log, job_id, 'error saving model')

    update_job_last_update(db, job_id)

    remove_working_folder(job_id)
    db.close()
    log.write("script ended")

