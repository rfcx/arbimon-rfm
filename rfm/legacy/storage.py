import boto3
import os

config = {
    's3_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    's3_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
    's3_bucket_name': os.getenv('S3_BUCKET_NAME'),
    's3_legacy_bucket_name': os.getenv('S3_LEGACY_BUCKET_NAME'),
    's3_endpoint': os.getenv('S3_ENDPOINT')
}

def upload_file(local_path, key):
    s3 = boto3.resource('s3', aws_access_key_id=config['s3_access_key_id'], 
                        aws_secret_access_key=config['s3_secret_access_key'], endpoint_url=config['s3_endpoint'])
    bucket = s3.Bucket(config['s3_legacy_bucket_name'])
    bucket.upload_file(local_path, key, ExtraArgs={'ACL': 'public-read'})

def download_file(key, local_path):
    s3 = boto3.resource('s3', aws_access_key_id=config['s3_access_key_id'], 
                        aws_secret_access_key=config['s3_secret_access_key'], endpoint_url=config['s3_endpoint'])
    bucket = s3.Bucket(config['s3_legacy_bucket_name'])
    bucket.download_file(key, local_path)
