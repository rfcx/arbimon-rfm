import sys
import os
import csv
import multiprocessing
import traceback
import shutil
import json
import boto3
import numpy
import png
import tempfile
from contextlib import closing
from joblib import Parallel, delayed
from pylab import *

from .a2audio.model import Model
from .a2audio.roiset import Roiset
from .a2audio.training import roigen
from .a2pyutils.logger import Logger
from .db import connect, get_training_job, get_training_job_params, get_training_data, get_validation_data, update_job_error, update_job_last_update, update_validations

num_cores = multiprocessing.cpu_count()

config = {
    's3_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    's3_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
    's3_bucket_name': os.getenv('S3_BUCKET_NAME'),
    's3_legacy_bucket_name': os.getenv('S3_LEGACY_BUCKET_NAME'),
    's3_endpoint': os.getenv('S3_ENDPOINT')
}

def exit_error(db, log, job_id, msg):
    log.write(msg)
    update_job_error(db, msg, job_id)
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

def upload_file(local_path, key):
    s3 = boto3.resource('s3', aws_access_key_id=config['s3_access_key_id'], 
                        aws_secret_access_key=config['s3_secret_access_key'], endpoint_url=config['s3_endpoint'])
    bucket = s3.Bucket(config['s3_legacy_bucket_name'])
    bucket.upload_file(local_path, key, ExtraArgs={'ACL': 'public-read'})

def run_train(job_id: int):
    log = Logger(job_id, 'train.py', 'main')
    log.also_print = True
    currDir = os.path.dirname(os.path.abspath(__file__)) # todo: remove?

    db = connect()

    # get job
    try:
        (project_id, user_id, model_type_id, training_set_id, model_name) = get_training_job(db, job_id)
        (use_training_p, use_training_np, use_validation_p, use_validation_np) = get_training_job_params(db, job_id)
    except Exception:
        log.write("could not find training job #{}".format(job_id))
        sys.exit(-1)
    log.write("project_id={} training_set_id={} model_name={}".format(project_id, training_set_id, model_name))
    if model_type_id != 4:
        log.write("unknown model type requested")

    # creating a temporary folder
    remove_working_folder(job_id)
    working_folder = get_working_folder(job_id)
    if not os.path.exists(working_folder):
        exit_error(db, log, job_id, 'cannot create temporary directory')

    # training data file creation
    try:
        training_data, species_songtypes = get_training_data(db, training_set_id)
        progress_steps = len(training_data)
        log.write(f'training data contains {progress_steps} rows')
        write_training_data(training_set_id, job_id, working_folder, training_data)
    except Exception:
        exit_error(db, log, job_id, 'cannot create training csvs files or access training data from db. {}'.format(traceback.format_exc()))
    log.write('training data retrieved')

    # validation data file creation    
    validation_data = []
    try:
        validation_file = working_folder+'/validation_'+str(job_id)+'.csv'
        with open(validation_file, 'w') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',')
            for (species_id, songtype_id) in species_songtypes:
                validation_rows = get_validation_data(db, project_id, species_id, songtype_id, use_training_p+use_validation_p, use_training_np+use_validation_np)
                progress_steps = progress_steps + len(validation_rows)
                for row in validation_rows:
                    ispresent = 0
                    if (row[3]==1) or (row[4]>0):
                        ispresent = 1
                    cc = (str(row[1])+"_"+str(row[2]))
                    validation_data.append([row[0], row[1], row[2], ispresent, cc, row[5], row[6]])
                    spamwriter.writerow([row[0], row[1], row[2], row[3], cc, row[4]])
    except Exception:
        exit_error(db, log, job_id, 'cannot access validation data from db. {}'.format(traceback.format_exc()))

    # save validation file to bucket and update db
    try:
        validations_key = 'project_{}/validations/job_{}.csv'.format(project_id, job_id)
        upload_file(validation_file, validations_key)

        progress_steps = progress_steps + 15
        update_validations(db, project_id, user_id, model_name, validations_key, job_id, progress_steps)
    except Exception:
        exit_error(db, log, job_id, 'cannot create validation csvs files or access validation data from db. {}'.format(traceback.format_exc()))
    log.write('validation data retrieved')
    

    """Roigenerator"""
    try:
        # roigen defined in a2audio.training
        rois = Parallel(n_jobs=1)(delayed(roigen)(line,working_folder,currDir,job_id,log) for line in training_data)
    except Exception:
        exit_error(db, log, job_id, 'roigenerator failed. {}'.format(traceback.format_exc()))

    if len(rois) == 0:
        exit_error(db, log, job_id, 'cannot create rois from recordings')

    log.write('rois created')
    classes: dict[str, Roiset] = {}
    pattern_surfaces = {}

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
    except Exception:
        exit_error(db, log, job_id, 'cannot align rois. {}'.format(traceback.format_exc()))

    if len(pattern_surfaces) == 0:
        exit_error(db, log, job_id, 'cannot create pattern surface from rois')
    log.write('rois aligned, pattern surface created')
    results = None
    """Recnilize"""
    try:
        results = Parallel(n_jobs=num_cores)(delayed(recnilize)(line,working_folder,currDir,job_id,(pattern_surfaces[line[4]]),log,True,False) for line in validation_data)
    except Exception:
        
        exit_error(db, log, job_id,' cannot analize recordings in parallel {}'.format(traceback.format_exc()))
    
    if results is None:
        exit_error(db, log, job_id, 'cannot analize recordings')
    log.write('validation recordings analyzed')
    presentsCount = 0
    ausenceCount = 0
    processed_count = 0
    for res in results:
        if 'err' not in res:
            if int(res['info'][1]) == 0:
                ausenceCount = ausenceCount + 1
            if int(res['info'][1]) == 1:
                presentsCount = presentsCount + 1            
        else:
            log.write(res)

    if presentsCount < 2 and ausenceCount < 2:
        exit_error(db, log, job_id, 'not enough validations to create model')
    
    """Add samples to model"""
    models = {}
    log.write('total results '+str(len(results)))
    no_errors = 0
    errors_count = 0
    try:
        for res in results:
            if 'err' not in res:
                no_errors = no_errors + 1                               
                classid = res['info'][0]
                if classid in models:
                    models[classid].addSample(res['info'][1],res['fets'],res['info'][6])
                else:
                    models[classid] = Model(classid,pattern_surfaces[classid][0],job_id)
                    models[classid].addSample(res['info'][1],res['fets'],res['info'][6])
            else:
                errors_count = errors_count + 1

    except Exception:
        exit_error(db, log, job_id, 'cannot add samples to model. {}'.format(traceback.format_exc()))
    log.write('errors : '+str(errors_count)+" processed: "+ str(no_errors))
    log.write('model trained')
    project_id = None
    user_id = None
    modelname = None
    valiId = None
    model_type_id = None	
    training_set_id = None
    use_training_p = None
    use_training_np = None
    use_validation_p = None
    use_validation_np = None
    """Get params from database"""
    try:
        with closing(db.cursor()) as cursor:
        
            cursor.execute("SELECT `project_id`,`user_id` FROM `jobs` WHERE `job_id` = "+str(job_id))
            db.commit()
            row = cursor.fetchone()
            project_id = row[0]	
            user_id = row[1] 	
        
            cursor.execute("SELECT * FROM `job_params_training` WHERE `job_id` = "+str(job_id))
            db.commit()
            row = cursor.fetchone()
            model_type_id = row[1]	
            training_set_id = row[2]
            use_training_p = row[5]
            use_training_np = row[6]
            use_validation_p = row[7]
            use_validation_np = row[8]
        
            cursor.execute("SELECT `params`,`validation_set_id` FROM `validation_set` WHERE `job_id` = "+str(job_id))
            db.commit()
            row = cursor.fetchone()
            
            cursor.execute('update `jobs` set `state`="processing", `progress` = `progress` + 5 where `job_id` = '+str(job_id))
            db.commit()
            
            decoded = json.loads(row[0])
            modelname = decoded['name']
            valiId = row[1]
    except Exception:
        exit_error(db, log, job_id, 'error querying database. {}'.format(traceback.format_exc()))

    log.write('user requested : '+" "+str(use_training_p)+" "+str(use_training_np)+" "+str( use_validation_p)+" "+str(use_validation_np ))
    log.write('available validations : presents: '+str(presentsCount)+' ausents: '+str(ausenceCount) )
    if (use_training_p + use_validation_p) > presentsCount:
        if presentsCount <= use_training_p:
            use_training_p = presentsCount - 1
            use_validation_p = 1
        else:
            use_validation_p = presentsCount - use_training_p
    
    if (use_training_np + use_validation_np) > ausenceCount:
        if ausenceCount <= use_training_np:
            use_training_np = ausenceCount - 1
            use_validation_np = 1
        else:
            use_validation_np = ausenceCount - use_training_np
    log.write('user requested : '+" "+str(use_training_p)+" "+str(use_training_np)+" "+str( use_validation_p)+" "+str(use_validation_np ))

    savedModel = False
    # """ Create and save model """
    for i in models:
        resultSplit = False
        try:
            resultSplit = models[i].splitData(use_training_p,use_training_np,use_validation_p,use_validation_np)
        except Exception:
            exit_error(db, log, job_id, 'error spliting data for validation. {}'.format(traceback.format_exc()))
            log.write('error spliting data for validation.')
        validationsKey =  'project_'+str(project_id)+'/validations/job_'+str(job_id)+'_vals.csv'
        validationsLocalFile = working_folder+'job_'+str(job_id)+'_vals.csv'
        try:
            models[i].train()
        except Exception:
            exit_error(db, log, job_id, 'error training model. {}'.format(traceback.format_exc()))

        if use_validation_p > 0:
            try:
                models[i].validate()
                models[i].saveValidations(validationsLocalFile)
            except Exception:
                exit_error(db, log, job_id, 'error validating model. {}'.format(traceback.format_exc()))
            
        modFile = working_folder+"model_"+str(job_id)+"_"+str(i)+".mod"
        try:
            models[i].save(modFile,pattern_surfaces[i][2] ,pattern_surfaces[i][3],pattern_surfaces[i][4])
        except Exception:
            exit_error(db, log, job_id, 'error saving model file to local storage. {}'.format(traceback.format_exc()))
            
        modelStats = None
        try:
            modelStats = models[i].modelStats()
        except Exception:
            exit_error(db, log, job_id, 'cannot get stats from model. {}'.format(traceback.format_exc()))
        pngKey = None
        try:
            
            pngFilename = modelFilesLocation+'job_'+str(job_id)+'_'+str(i)+'.png'
            pngKey = 'project_'+str(project_id)+'/models/job_'+str(job_id)+'_'+str(i)+'.png'
            specToShow = numpy.zeros(shape=(0,int(modelStats[4].shape[1])))
            rowsInSpec = modelStats[4].shape[0]
            spec = modelStats[4]
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
        log.write('uploading png')
        try:
            conn = S3Connection(config['s3_access_key_id'], config['s3_secret_access_key'])
            bucket = conn.get_bucket(bucketName)
            modKey = 'project_'+str(project_id)+'/models/job_'+str(job_id)+'_'+str(i)+'.mod'
            #save model file to bucket
            k = bucket.new_key(modKey)
            k.set_contents_from_filename(modFile)
            #save validations results to bucket
            k = bucket.new_key(validationsKey)
            k.set_contents_from_filename(validationsLocalFile)
            k.set_acl('public-read')
            #save vocalization surface png to bucket
            k = bucket.new_key(pngKey)
            k.set_contents_from_filename(pngFilename)
            k.set_acl('public-read')
        except Exception:
            exit_error(db, log, job_id, 'error uploading files to amazon bucket. {}'.format(traceback.format_exc()))
        log.write('saving to db')        
        species,songtype = i.split("_")
        try:
            #save model to DB
            with closing(db.cursor()) as cursor:
                cursor.execute('update `jobs` set `state`="processing", `progress` = `progress` + 5 where `job_id` = '+str(job_id))
                db.commit()        
                cursor.execute("SELECT   max(ts.`x2` -  ts.`x1`) , min(ts.`y1`) , max(ts.`y2`) "+
                    "FROM `training_set_roi_set_data` ts "+
                    "WHERE  ts.`training_set_id` =  "+str(training_set_id))
                db.commit()
                row = cursor.fetchone()
                lengthRoi = row[0]	
                minFrequ = row[1]
                maxFrequ = row[2]
                
                cursor.execute("SELECT   count(*) "+
                    "FROM `training_set_roi_set_data` ts "+
                    "WHERE  ts.`training_set_id` =  "+str(training_set_id))
                db.commit()
                row = cursor.fetchone()
                totalRois = row[0]
                
                statsJson = '{"roicount":'+str(totalRois)+' , "roilength":'+str(lengthRoi)+' , "roilowfreq":'+str(minFrequ)+' , "roihighfreq":'+str(maxFrequ)
                statsJson = statsJson + ',"accuracy":'+str(modelStats[0])+' ,"precision":'+str(modelStats[1])+',"sensitivity":'+str(modelStats[2])
                statsJson = statsJson + ', "forestoobscore" :'+str(modelStats[3])+' , "roisamplerate" : '+str(pattern_surfaces[i][1])+' , "roipng":"'+pngKey+'"'
                statsJson = statsJson + ', "specificity":'+str(modelStats[5])+' , "tp":'+str(modelStats[6])+' , "fp":'+str(modelStats[7])+' '
                statsJson = statsJson + ', "tn":'+str(modelStats[8])+' , "fn":'+str(modelStats[9])+' , "minv": '+str(modelStats[10])+', "maxv": '+str(modelStats[11])+'}'
            
                cursor.execute("INSERT INTO `models`(`name`, `model_type_id`, `uri`, `date_created`, `project_id`, `user_id`,"+
                            " `training_set_id`, `validation_set_id`) " +
                            " VALUES ('"+modelname+"', "+str(model_type_id)+" , '"+modKey+"' , now() , "+str(project_id)+","+
                            str(user_id)+" ,"+str(training_set_id)+", "+str(valiId)+" )")
                db.commit()
                insertmodelId = cursor.lastrowid
                
                cursor.execute("INSERT INTO `model_stats`(`model_id`, `json_stats`) VALUES ("+str(insertmodelId)+",'"+statsJson+"')")
                db.commit()
                
                cursor.execute("INSERT INTO `model_classes`(`model_id`, `species_id`, `songtype_id`) VALUES ("+str(insertmodelId)
                            +","+str(species)+","+str(songtype)+")")
                db.commit()       
                
                cursor.execute('update `job_params_training` set `trained_model_id` = '+str(insertmodelId)+' where `job_id` = '+str(job_id))
                db.commit()
                
                cursor.execute('update `jobs` set `last_update` = now() where `job_id` = '+str(job_id))
                db.commit()
                cursor.execute('update `jobs` set `state`="completed", `progress` = `progress_steps` ,  `completed` = 1 , `last_update` = now() where `job_id` = '+str(job_id))
                db.commit()
                log.write('saved to db correctly')
                savedModel  = True
        except Exception:
            exit_error(db, log, job_id, 'error saving model into database. {}'.format(traceback.format_exc()))
            
    if savedModel :
        log.write("model saved")
    else:
        exit_error(db, log, job_id, 'error saving model')

    update_job_last_update(db, job_id)

    remove_working_folder(job_id)
    db.close()
    log.write("script ended")

