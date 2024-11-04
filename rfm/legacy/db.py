import json
import os
import mysql.connector
from contextlib import closing

config = {
    'db_host': os.getenv('DB_HOST'),
    'db_user': os.getenv('DB_USER'),
    'db_password': os.getenv('DB_PASSWORD'),
    'db_name': os.getenv('DB_NAME'),
}

def connect():
    return mysql.connector.connect(
        host=config['db_host'],
        user=config['db_user'],
        password=config['db_password'], 
        database=config['db_name']
    )

def get_training_job(db, job_id):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT j.`project_id`, j.`user_id`, jp.model_type_id, jp.training_set_id, jp.name
            FROM `jobs` j JOIN `job_params_training` jp ON jp.job_id = j.job_id
            WHERE j.`job_id` = %s""", [job_id])
        (project_id, user_id, model_type_id, training_set_id, model_name) = cursor.fetchone()
    return (project_id, user_id, model_type_id, training_set_id, model_name)

def get_training_job_params(db, job_id):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT use_in_training_present, use_in_training_notpresent, 
                use_in_validation_present, use_in_validation_notpresent FROM `job_params_training` 
            WHERE `job_id` = %s""", [job_id])
        (tp, tnp, vp, vnp) = cursor.fetchone()
    return (tp, tnp, vp, vnp)

def get_training_data(db, training_set_id): 
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT r.`recording_id`, ts.`species_id`, ts.`songtype_id`,
                ts.`x1`, ts.`x2`, ts.`y1`, ts.`y2`, r.`uri`, IF(LEFT(r.uri, 8) = 'project_', 1, 0) legacy
            FROM `training_set_roi_set_data` ts
            JOIN `recordings` r ON r.`recording_id` = ts.`recording_id`
            WHERE ts.`training_set_id` = %s
        """, [training_set_id])
        training_data = [row for row in cursor]            
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            SELECT DISTINCT `species_id`, `songtype_id`
            FROM `training_set_roi_set_data`
            WHERE `training_set_id` = %s
        """, [training_set_id])
        species_songtypes = [[species_id, songtype_id] for (species_id, songtype_id) in cursor]
    return training_data, species_songtypes

def get_validation_data(db, project_id, species_id, songtype_id, num_positive, num_negative):
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
        """, [project_id, species_id, songtype_id, num_positive, project_id, species_id, songtype_id, num_negative])
        results = [row for row in cursor]
    return results

def update_validations(db, project_id, user_id, model_name, validations_key, job_id, progress_steps):
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
            )""", [
                project_id, user_id, model_name+" validation", validations_key,
                json.dumps({'name': model_name}), job_id])
        db.commit()

        cursor.execute("""
            UPDATE `job_params_training` SET `validation_set_id` = %s
            WHERE `job_id` = %s""", [cursor.lastrowid, job_id])
        db.commit()

        cursor.execute("""
            UPDATE `jobs` SET `progress_steps` = %s, progress=0, state="processing"
            WHERE `job_id` = %s""", [progress_steps, job_id])
        db.commit()


def update_job_error(db, job_id, msg):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            UPDATE `jobs`
            SET `remarks` = %s,
                `state`="error",
                `completed` = 1 ,
                `last_update` = now()
            WHERE `job_id` = %s
        """, ['Error: '+str(msg), int(job_id)])
        db.commit()


def update_job_last_update(db, job_id):
    with closing(db.cursor()) as cursor:
        cursor.execute("""
            UPDATE `jobs`
            SET `last_update`=now()
            WHERE `job_id` = %s
        """, [job_id])
        db.commit()