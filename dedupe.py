import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from awsglue.job import Job
import pandas as pd
import time
# getting the Job Name
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Creating Glue and Spark Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

date_to_clean = '2022-03-28'

def getS3Objects(s3,bucket):
    print("")
    objects = []
    my_bucket = s3.Bucket(bucket)
    for object_summary in my_bucket.objects.filter(Prefix=""):
        objects.append(object_summary.key)
    return objects

def getS3Subfolders(objects, basepath = ""):
    folders = set()
    if len(basepath) > 0:
        objects = [s.replace(basepath,"") for s in objects if basepath in s.lower()]
    for object in objects:
        if len(object) > 0 :
            if "/" in object:
                component = object.split("/")[0]
                folders |= {component}
    return list(folders)
    
def getS3FilesInFolder(objects, folder):
    # print("")
    files = []
    # my_bucket = s3.Bucket(bucket)
    # for object_summary in my_bucket.objects.filter(Prefix="%s/" % folder):
    prefix = "%s/" % folder
    filter = [x for x in objects if x.startswith(prefix)]
    for object in filter:
        # file = object.split("/")[1]
        
        file = object[len(folder)+1:]
        if len(file)> 0:
            files.append(file)
    return files

import boto3
s3 = boto3.resource('s3')
objects = getS3Objects(s3,'dev-dashy-normalise-scraping')
s3Folder = getS3Subfolders(objects)


s3_client = boto3.client('s3')
for folder in s3Folder:

    files = getS3FilesInFolder(objects, folder)
    path = 's3://dev-dashy-normalise-scraping/' + folder + '/'

    latest_files = []
    latest_norm_files = []
    for f in files:
        if date_to_clean in f:
            if 'normalise' not in f.lower():
                latest_files.append(f)
            elif 'normalise' in f.lower():
                latest_norm_files.append(f)
            else:
                continue
    
    if len(latest_files) > 1 and len(latest_norm_files) > 1:
        print(folder)
        print(latest_files)
        print(latest_norm_files)
        
        base_file_name = latest_files[0].split(date_to_clean)[0] + date_to_clean + '.csv'
        base_norm_file_name = latest_norm_files[0].split(date_to_clean)[0] + date_to_clean + '.csv'
        big_file = pd.concat([pd.read_csv(path+f, encoding_errors = 'ignore', dtype="str") for f in latest_files])
        big_norm_file = pd.concat([pd.read_csv(path+f, encoding_errors = 'ignore', dtype="str") for f in latest_norm_files])
        
        a_val = big_file['Current_time'].iloc[0]
        t =big_file[big_file.columns[:-1]].copy()
        print('len before:', len(t))
        t.drop_duplicates(keep='first', inplace=True)
        print('len after:', len(t))
        t['Current_time'] = a_val
        t.to_csv(path+base_file_name, index=False)
        
        t = big_norm_file[big_norm_file.columns[:-1]].copy()
        print('len before:', len(t))
        t.drop_duplicates(keep='first', inplace=True)
        print('len after:', len(t))
        t['Current_time'] = a_val
        t.to_csv(path+base_norm_file_name, index=False)
        
        print('Sleeping for delete')
        time.sleep(30)
        
        for _file in latest_files:
            response = s3_client.delete_object(Bucket='dev-dashy-normalise-scraping',Key=folder+ '/' + _file)
        for _file in latest_norm_files:
            response = s3_client.delete_object(Bucket='dev-dashy-normalise-scraping',Key=folder+ '/' + _file)
    
            
