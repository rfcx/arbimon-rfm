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

def get_sites(conn, project_id, query = None):
    cursor = conn.cursor()

    if query is None:
        # all sites in project
        sql = 'select site_id, name from sites where project_id = %s and deleted_at is null'
        cursor.execute(sql, (project_id,))
        results = {site_id: name for (site_id, name) in cursor}
    else:
        # only sites matching a query term, e.g. AB*,CD*,EF05,EF06
        results = {}
        for term in query.split(','):
            term = term.replace('*', '%')
            cursor.execute('select site_id, name from sites where name like %s and project_id = %s and deleted_at is null', (term, project_id))
            for (site_id, name) in cursor:
                results[site_id] = name

    cursor.close()
    return results

def create_playlist(conn, project_id, site_id, site_name, year):
    playlist_name = f'{site_name} ({site_id}) {year}'
    cursor = conn.cursor()

    cursor.execute('select count(*) from recordings where site_id = %s and year(datetime) = %s', (site_id, year))
    (total_recordings,) = cursor.fetchone()

    # No recordings
    if total_recordings == 0:
        return None

    # Shallow check for duplicate playlist
    cursor.execute('select playlist_id, name from playlists where project_id = %s and name = %s and total_recordings = %s', (project_id, playlist_name, total_recordings))
    result = cursor.fetchone()
    if result is not None:
        cursor.close()
        return result

    # Find unique playlist name
    cursor.execute('select playlist_id from playlists where project_id = %s and name = %s', (project_id, playlist_name))
    result = cursor.fetchone()
    if result is not None:
        playlist_name += f' {total_recordings}'
    
    # Create playlists row
    cursor.execute('insert into playlists (project_id, name, playlist_type_id, total_recordings) values (%s, %s, 1, %s)', (project_id, playlist_name, total_recordings))
    conn.commit()
    playlist_id = cursor.lastrowid

    # Create playlist_recordings rows
    cursor.execute('''insert into playlist_recordings (playlist_id, recording_id)
        select %s, recording_id from recordings where site_id = %s and year(datetime) = %s''', (playlist_id, site_id, year))
    conn.commit()

    if cursor.rowcount != total_recordings:
        print('WARN: total recordings does not match inserted recordings for playlist', playlist_id)

    cursor.close()
    return playlist_id, playlist_name

def create_job(conn, playlist_id, user_id, aggregation = 'time_of_day', bin_size = 344, threshold = 0.05, normalize = 1, soundscape_name = None) -> Union[int,None]:
    cursor = conn.cursor()

    # Get aggregation id from identifier
    aggregation_type_id = find_aggregation(conn, aggregation)
    if aggregation_type_id is None:
        cursor.close()
        return None

    # Get the playlist
    cursor.execute('select project_id, name, total_recordings from playlists where playlist_id = %s', (playlist_id, ))
    row = cursor.fetchone()
    if row is None:
        cursor.close()
        return None
    (project_id, playlist_name, total_recordings) = row

    # Additional parameters
    max_hertz = 24000 # TODO compute this from the recordings
    job_name = playlist_name if soundscape_name is None else soundscape_name
    # job_name_suffix = soundscape_hash(playlist_id, aggregation_type_id, bin_size, threshold, normalize) # TODO make job name unique

    cursor.execute(
        '''insert into jobs (job_type_id, date_created, last_update, project_id, user_id, state, progress_steps, remarks, uri, hidden) 
        values (4, now(), now(), %s, %s, 'initializing', %s, '', '', 0)''',
        (project_id, user_id, total_recordings))
    conn.commit()
    job_id = cursor.lastrowid

    cursor.execute(
        '''insert into job_params_soundscape (job_id, playlist_id, name, max_hertz, soundscape_aggregation_type_id, bin_size, threshold, normalize) 
        values (%s, %s, %s, %s, %s, %s, %s, %s)''',
        (job_id, playlist_id, job_name, max_hertz, aggregation_type_id, bin_size, threshold, normalize))
    conn.commit()
    cursor.close()

    return job_id

def soundscape_exists(conn, playlist_id, aggregation, bin_size, threshold, normalize) -> bool:
    aggregation_type_id = find_aggregation(conn, aggregation)
    if aggregation_type_id is None:
        return False
    
    # Find a matching soundscape
    cursor = conn.cursor()
    cursor.execute('''select soundscape_id from soundscapes where playlist_id = %s and soundscape_aggregation_type_id = %s and bin_size = %s and abs(threshold - %s) < 0.001 and normalized = %s limit 1''',
                   (playlist_id, aggregation_type_id, bin_size, threshold, normalize))
    row = cursor.fetchone()
    cursor.close()
    return row is not None

def soundscape_hash(playlist_id: int, aggregation_id: int, bin_size: int, threshold: float, normalize: int) -> str:
    plain = f'{playlist_id}_{aggregation_id}_{bin_size}_{threshold}_{normalize}'
    d = hashlib.md5(plain).digest()
    return str(base64.b64encode(d))[:5]
