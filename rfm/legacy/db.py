import base64
import hashlib
import os
import mysql.connector
from typing import Union

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

def get_automated_user(conn):
    cursor = conn.cursor()
    automated_user = 'automated-user'

    cursor.execute('select user_id from users where login = %s', (automated_user, ))
    result = cursor.fetchone()
    if result is not None:
        cursor.close()
        (user_id,) = result
        return user_id
    
    cursor.execute('''insert into users (login, password, firstname, lastname, email) values (%s, '',  'Automated', 'Job', 'automated-user@arbimon.org')''', (automated_user,))
    conn.commit()
    user_id = cursor.lastrowid

    cursor.close()
    return user_id

def find_project(conn, url_or_id):
    cursor = conn.cursor()
    
    conditions = [
        'url = %s',
        'external_id = %s',
        'project_id = %s'
    ]
    for condition in conditions:
        cursor.execute(f'select project_id from projects where {condition}', (url_or_id, ))
        result = cursor.fetchone()
        if result is not None:
            cursor.close()
            (project_id,) = result
            return project_id
    
    cursor.close()
    return None

def find_aggregation(conn, identifier) -> Union[int,None]:
    cursor = conn.cursor()

    # Get aggregation id from identifier
    cursor.execute('select soundscape_aggregation_type_id from soundscape_aggregation_types where identifier = %s', (identifier,))
    row = cursor.fetchone()
    if row is None:
        cursor.close()
        return None
    
    cursor.close()
    (aggregation_type_id, ) = row
    return aggregation_type_id

