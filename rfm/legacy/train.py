import sys
import sys
import unidecode
import os
import csv
import traceback
import shutil
import MySQLdb
import traceback
import json
from boto.s3.connection import S3Connection
from contextlib import closing
from a2audio.training import *
from a2pyutils.config import EnvironmentConfig
from a2pyutils.logger import Logger
import multiprocessing
from joblib import Parallel, delayed
from a2audio.roiset import Roiset
from a2audio.model import Model
import numpy
import png
from pylab import *

num_cores = multiprocessing.cpu_count()

def run_train(job_id: int):
    modelName = ''
    project_id = -1
    configuration = EnvironmentConfig()
    config = configuration.data()
    log = Logger(job_id, 'train.py', 'main')
    log.also_print = True

    log.write('script started with job id:'+str(job_id))

    try:
        db = MySQLdb.connect(
            host=config[0], user=config[1],
            passwd=config[2], db=config[3]
        )
    except MySQLdb.Error as e:
        log.write("fatal error cannot connect to database. {}".format(traceback.format_exc()))
        sys.exit(-1)


    def exit_error(db, workingFolder, log, jobId, msg):
        with closing(db.cursor()) as cursor:
            cursor.execute("""
                UPDATE `jobs`
                SET `remarks` = %s,
                    `state`="error",
                    `completed` = 1 ,
                    `last_update` = now()
                WHERE `job_id` = %s
            """, ['Error: '+str(msg), int(jobId)])
            db.commit()

        log.write(msg)
        if os.path.exists(workingFolder):
            shutil.rmtree(workingFolder)
        sys.exit(-1)

    currDir = os.path.dirname(os.path.abspath(__file__))
    currPython = sys.executable

    bucketName = config[4]
    awsKeyId = config[5]
    awsKeySecret = config[6]

    sys.stdout.flush()

    with closing(db.cursor()) as cursor:
        cursor.execute("""
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
        """, [job_id])
        row = cursor.fetchone()

    if not row:
        log.write("Could not find training job #{}".format(job_id))
        sys.exit(-1)

    (
        project_id, user_id,
        model_type_id, training_set_id,
        validation_set_id, trained_model_id,
        use_in_training_present,
        use_in_training_notpresent,
        use_in_validation_present,
        use_in_validation_notpresent,
        name
    ) = row
    modelName = unicode(name, "latin-1")
    modelName = unidecode.unidecode(modelName)
    tempFolders = str(configuration.pathsConfig['temp_dir'])
    # select the model_type by its id
    if model_type_id in [4]:
        """Pattern Matching (modified Alvarez thesis)"""
        ssim = True
        if model_type_id == 2:
            ssim = False
        searchMatch = False
        if model_type_id == 3:
            searchMatch = True

        log.write("Pattern Matching (modified Alvarez thesis)")

        progress_steps = 1
        # creating a temporary folder
        workingFolder = tempFolders+"/training_"+str(job_id)+"/"
        if os.path.exists(workingFolder):
            shutil.rmtree(workingFolder)
        os.makedirs(workingFolder)
        if not os.path.exists(workingFolder):
            exit_error(db, workingFolder, log, job_id, 'cannot create temporary directory')
        trainingData = []

        """ Training data file creation """
        try:
            with closing(db.cursor()) as cursor:
                # create training file
                cursor.execute("""
                    SELECT r.`recording_id`, ts.`species_id`, ts.`songtype_id`,
                        ts.`x1`, ts.`x2`, ts.`y1`, ts.`y2`, r.`uri`, IF(LEFT(r.uri, 8) = 'project_', 1, 0) legacy
                    FROM `training_set_roi_set_data` ts
                    JOIN `recordings` r ON r.`recording_id` = ts.`recording_id`
                    WHERE ts.`training_set_id` = %s
                """, [training_set_id])
                db.commit()
                trainingFileName = os.path.join(
                    workingFolder,
                    'training_{}_{}.csv'.format(job_id, training_set_id)
                )
                # write training file to temporary folder
                with open(trainingFileName, 'wb') as csvfile:
                    spamwriter = csv.writer(csvfile, delimiter=',')
                    numTrainingRows = int(cursor.rowcount)
                    progress_steps = numTrainingRows
                    for x in range(0, numTrainingRows):
                        rowTraining = cursor.fetchone()
                        trainingData.append(rowTraining)
                        spamwriter.writerow(rowTraining[0:7+1] + (job_id,))

                cursor.execute("""
                    SELECT DISTINCT `recording_id`
                    FROM `training_set_roi_set_data`
                    where `training_set_id` = %s
                """, [training_set_id])
                db.commit()

                numrecordingsIds = int(cursor.rowcount)
                recordingsIds = []
                for x in range(0, numrecordingsIds):
                    rowRec = cursor.fetchone()
                    recordingsIds.append(rowRec[0])

                cursor.execute("""
                    SELECT DISTINCT `species_id`, `songtype_id`
                    FROM `training_set_roi_set_data`
                    WHERE `training_set_id` = %s
                """, [training_set_id])
                db.commit()

                numSpeciesSongtype = int(cursor.rowcount)
                speciesSongtype = []
                for x in range(0, numSpeciesSongtype):
                    rowSpecies = cursor.fetchone()
                    speciesSongtype.append([rowSpecies[0], rowSpecies[1]])
        except StandardError, e:
            exit_error(db, workingFolder, log, job_id, 'cannot create training csvs files or access training data from db. {}'.format(traceback.format_exc()))

        log.write('training data retrieved')
        useTrainingPresent = None
        useTrainingNotPresent = None
        useValidationPresent = None
        useValidationNotPresent = None

        try:
            with closing(db.cursor()) as cursor:
                cursor.execute("SELECT * FROM `job_params_training` WHERE `job_id` = "+str(job_id))
                db.commit()
                row = cursor.fetchone()
                useTrainingPresent = row[5]
                useTrainingNotPresent = row[6]
                useValidationPresent = row[7]
                useValidationNotPresent = row[8]
        except StandardError, e:
            exit_error(db,workingFolder,log,job_id,'cannot retrieve training data from db. {}'.format(traceback.format_exc()))
            
        validationData = []
        """ Validation file creation """
        try:
            validationFile = workingFolder+'/validation_'+str(job_id)+'.csv'
            with open(validationFile, 'wb') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=',')
                for x in range(0, numSpeciesSongtype):
                    spst = speciesSongtype[x]
                    with closing(db.cursor()) as cursor:
                        cursor.execute(
                            """
                            (SELECT r.`uri` , `species_id` , `songtype_id` , `present` , `present_review` , r.`recording_id`, IF(LEFT(r.uri, 8) = 'project_', 1, 0) legacy
                            FROM `recording_validations` rv 
                            JOIN `recordings` r ON r.`recording_id` = rv.`recording_id`
                            WHERE rv.`project_id` = %s
                            AND `species_id` = %s
                            AND `songtype_id` = %s
                            AND (`present` = 1 OR `present_review` > 0)
                            ORDER BY rand()
                            LIMIT %s)
                            UNION
                            (SELECT r.`uri` , `species_id` , `songtype_id` , `present` , `present_review` , r.`recording_id`, IF(LEFT(r.uri, 8) = 'project_', 1, 0) legacy
                            FROM `recording_validations` rv 
                            JOIN `recordings` r ON r.`recording_id` = rv.`recording_id`
                            WHERE rv.`project_id` = %s
                            AND `species_id` = %s
                            AND `songtype_id` = %s
                            AND `present` = 0
                            AND `present_review` = 0
                            ORDER BY rand()
                            LIMIT %s)
                        """, [project_id, spst[0], spst[1], (int(useTrainingPresent)+int(useValidationPresent )) ,
                            project_id, spst[0], spst[1], (int(useTrainingNotPresent)+int(useValidationNotPresent )) ])

                        db.commit()

                        numValidationRows = int(cursor.rowcount)

                        progress_steps = progress_steps + numValidationRows

                        for x in range(0, numValidationRows):
                            rowValidation = cursor.fetchone()
                            ispresent = 0
                            if (rowValidation[3]==1) or (rowValidation[4]>0):
                                ispresent = 1
                            cc = (str(rowValidation[1])+"_"+str(rowValidation[2]))
                            validationData.append([
                                rowValidation[0], rowValidation[1],
                                rowValidation[2], ispresent, cc,
                                rowValidation[5], rowValidation[6]
                            ])
                            spamwriter.writerow([rowValidation[0] ,rowValidation[1] ,rowValidation[2] ,rowValidation[3] , cc,rowValidation[4]])

            # get Amazon S3 bucket
            conn = S3Connection(awsKeyId, awsKeySecret)
            bucket = conn.get_bucket(bucketName)
            valiKey = 'project_{}/validations/job_{}.csv'.format(project_id, job_id)

            # save validation file to bucket
            k = bucket.new_key(valiKey)
            k.set_contents_from_filename(validationFile)
            k.set_acl('public-read')
            # save validation to DB
            progress_steps = progress_steps + 15
            with closing(db.cursor()) as cursor:
                cursor.execute("""
                    INSERT INTO `validation_set`(
                        `validation_set_id`,
                        `project_id`,
                        `user_id`,
                        `name`,
                        `uri`,
                        `params`,
                        `job_id`
                    ) VALUES (
                        NULL, %s, %s, %s, %s, %s, %s
                    )
                """, [
                    project_id, user_id, modelName+" validation", valiKey,
                    json.dumps({'name': modelName}),
                    job_id
                ])
                db.commit()

                cursor.execute("""
                    UPDATE `job_params_training`
                    SET `validation_set_id` = %s
                    WHERE `job_id` = %s
                """, [cursor.lastrowid, job_id])
                db.commit()

                cursor.execute("""
                    UPDATE `jobs`
                    SET `progress_steps` = %s, progress=0, state="processing"
                    WHERE `job_id` = %s
                """, [progress_steps, job_id])
                db.commit()
        except StandardError, e:
            exit_error(db, workingFolder, log, job_id, 'cannot create validation csvs files or access validation data from db. {}'.format(traceback.format_exc()))

        log.write('validation data retrieved')
        if len(trainingData) == 0:
            exit_error(db, workingFolder, log, job_id, 'cannot create validation csvs files or access validation data from db. (no error)')

        classes = {}
        rois = None

        """Roigenerator"""
        try:
            # roigen defined in a2audio.training
            rois = Parallel(n_jobs=1)(delayed(roigen)(line,workingFolder,currDir,job_id,log) for line in trainingData)
        except StandardError, e:
            exit_error(db, workingFolder, log, job_id, 'roigenerator failed. {}'.format(traceback.format_exc()))

        if rois is None or len(rois) == 0:
            exit_error(db,workingFolder,log,job_id,'cannot create rois from recordings')

        log.write('rois created')
        patternSurfaces = {}

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
                patternSurfaces[i] = [
                    classes[i].getSurface(),
                    classes[i].setSampleRate,
                    classes[i].lowestFreq,
                    classes[i].highestFreq,
                    classes[i].maxColumns
                ]
        except StandardError, e:
            exit_error(db, workingFolder, log, job_id, 'cannot align rois. {}'.format(traceback.format_exc()))

        if len(patternSurfaces) == 0:
            exit_error(db, workingFolder, log, job_id, 'cannot create pattern surface from rois')
        log.write('rois aligned, pattern surface created')
        results = None
        """Recnilize"""
        try:
            results = Parallel(n_jobs=num_cores)(delayed(recnilize)(line,workingFolder,currDir,job_id,(patternSurfaces[line[4]]),log,ssim,searchMatch) for line in validationData)
        except StandardError, e:
            
            exit_error(db,workingFolder,log,job_id,'cannot analize recordings in parallel {}'.format(traceback.format_exc()))
        
        if results is None:
            exit_error(db,workingFolder,log,job_id,'cannot analize recordings')
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
            exit_error(db,workingFolder,log,job_id,'not enough validations to create model')
        
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
                        models[classid] = Model(classid,patternSurfaces[classid][0],job_id)
                        models[classid].addSample(res['info'][1],res['fets'],res['info'][6])
                else:
                    errors_count = errors_count + 1

        except StandardError, e:
            exit_error(db,workingFolder,log,job_id,'cannot add samples to model. {}'.format(traceback.format_exc()))
        log.write('errors : '+str(errors_count)+" processed: "+ str(no_errors))
        log.write('model trained')    
        modelFilesLocation = tempFolders+"/training_"+str(job_id)+"/"
        project_id = None
        user_id = None
        modelname = None
        valiId = None
        model_type_id = None	
        training_set_id = None
        useTrainingPresent = None
        useTrainingNotPresent = None
        useValidationPresent = None
        useValidationNotPresent = None
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
                useTrainingPresent = row[5]
                useTrainingNotPresent = row[6]
                useValidationPresent = row[7]
                useValidationNotPresent = row[8]
            
                cursor.execute("SELECT `params`,`validation_set_id` FROM `validation_set` WHERE `job_id` = "+str(job_id))
                db.commit()
                row = cursor.fetchone()
                
                cursor.execute('update `jobs` set `state`="processing", `progress` = `progress` + 5 where `job_id` = '+str(job_id))
                db.commit()
                
                decoded = json.loads(row[0])
                modelname = decoded['name']
                valiId = row[1]
        except StandardError, e:
            exit_error(db,workingFolder,log,job_id,'error querying database. {}'.format(traceback.format_exc()))

        log.write('user requested : '+" "+str(useTrainingPresent)+" "+str(useTrainingNotPresent)+" "+str( useValidationPresent)+" "+str(useValidationNotPresent ))
        log.write('available validations : presents: '+str(presentsCount)+' ausents: '+str(ausenceCount) )
        if (useTrainingPresent+useValidationPresent) > presentsCount:
            if presentsCount <= useTrainingPresent:
                useTrainingPresent = presentsCount - 1
                useValidationPresent = 1
            else:
                useValidationPresent = presentsCount - useTrainingPresent
        
        if (useTrainingNotPresent + useValidationNotPresent) > ausenceCount:
            if ausenceCount <= useTrainingNotPresent:
                useTrainingNotPresent = ausenceCount - 1
                useValidationNotPresent = 1
            else:
                useValidationNotPresent = ausenceCount - useTrainingNotPresent
        log.write('user requested : '+" "+str(useTrainingPresent)+" "+str(useTrainingNotPresent)+" "+str( useValidationPresent)+" "+str(useValidationNotPresent ))

        savedModel = False
        # """ Create and save model """
        for i in models:
            resultSplit = False
            try:
                resultSplit = models[i].splitData(useTrainingPresent,useTrainingNotPresent,useValidationPresent,useValidationNotPresent)
            except StandardError, e:
                exit_error(db,workingFolder,log,job_id,'error spliting data for validation. {}'.format(traceback.format_exc()))
                log.write('error spliting data for validation.')
            validationsKey =  'project_'+str(project_id)+'/validations/job_'+str(job_id)+'_vals.csv'
            validationsLocalFile = modelFilesLocation+'job_'+str(job_id)+'_vals.csv'
            try:
                models[i].train()
            except StandardError, e:
                exit_error(db,workingFolder,log,job_id,'error training model. {}'.format(traceback.format_exc()))

            if useValidationPresent > 0:
                try:
                    models[i].validate()
                    models[i].saveValidations(validationsLocalFile)
                except StandardError, e:
                    exit_error(db,workingFolder,log,job_id,'error validating model. {}'.format(traceback.format_exc()))
                
            modFile = modelFilesLocation+"model_"+str(job_id)+"_"+str(i)+".mod"
            try:
                models[i].save(modFile,patternSurfaces[i][2] ,patternSurfaces[i][3],patternSurfaces[i][4])
            except StandardError, e:
                exit_error(db,workingFolder,log,job_id,'error saving model file to local storage. {}'.format(traceback.format_exc()))
                
            modelStats = None
            try:
                modelStats = models[i].modelStats()
            except StandardError, e:
                exit_error(db,workingFolder,log,job_id,'cannot get stats from model. {}'.format(traceback.format_exc()))
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
            except StandardError, e:
                exit_error(db,workingFolder,log,job_id,'error creating pattern PNG. {}'.format(traceback.format_exc()))
            modKey = None  
            log.write('uploading png')
            try:
                conn = S3Connection(awsKeyId, awsKeySecret)
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
            except StandardError, e:
                exit_error(db,workingFolder,log,job_id,'error uploading files to amazon bucket. {}'.format(traceback.format_exc()))
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
                    statsJson = statsJson + ', "forestoobscore" :'+str(modelStats[3])+' , "roisamplerate" : '+str(patternSurfaces[i][1])+' , "roipng":"'+pngKey+'"'
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
            except StandardError, e:
                exit_error(db,workingFolder,log,job_id,'error saving model into database. {}'.format(traceback.format_exc()))
                
        if savedModel :
            log.write("model saved")
        else:
            exit_error(db,workingFolder,log,job_id,'error saving model')
        
    else:
        log.write("Unkown model type requested")

    with closing(db.cursor()) as cursor:
        cursor.execute("""
            UPDATE `jobs`
            SET `last_update`=now()
            WHERE `job_id` = %s
        """, [job_id])
        db.commit()

    shutil.rmtree(tempFolders+"/training_"+str(job_id))
    db.close()
    log.write("script ended")

