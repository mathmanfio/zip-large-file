import boto3
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
import sys
import time
import math


import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.transforms import *

spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())

bucket = "abt-aa-brasil-jarvis-202006001-nonprod"

s3 = boto3.client('s3')
s3r = boto3.resource('s3')
bucket_obj = s3r.Bucket(bucket)
chunk_size=524288000

# #INPUT
date = "2021-04-01"
audit = "NPS".lower()


input_path = f"raw/{audit}"
key = f"{input_path}/{date}/"



def read_in_chunks(file_object, chunk_size):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

def transfer_chunk_from_ftp_to_s3(
    piece,
    s3,
    multipart_upload,
    bucket,
    path_csv,
    part_number,
    chunk_size, 
):
    start_time = time.time()

    part = s3.upload_part(
        Bucket=bucket,
        Key=path_csv,
        PartNumber=part_number,
        UploadId=multipart_upload["UploadId"],
        Body=piece,
    )
    end_time = time.time()
    total_seconds = end_time - start_time
    print(
        "speed is {} kb/s total seconds taken {}".format(
            math.ceil((int(chunk_size) / 1024) / total_seconds), total_seconds
        )
    )
    part_output = {"PartNumber": part_number, "ETag": part["ETag"]}
    return part_output
    


zip_file = [n.key for n in bucket_obj.objects.filter(Prefix=key) if ".zip" in n.key]

if len(zip_file) > 1:
    sys.exit(f"Existe mais de um zip na pasta {key}")

##Descompressão do arquivo .zip para o dir do arcname
#Leitura em objeto
obj = s3r.Object(
    bucket_name=bucket,
    key=zip_file[0]
)

buffer = io.BytesIO(obj.get()["Body"].read())
z = zipfile.ZipFile(buffer)
list_full = z.namelist()

#etapa para nps: remove files com "Totals"
list = [e for e in list_full if not "Totals" in e]


#pra cada item da lista de objetos dentro do zip:
for filerr in list:
    path_csv = f"dev/standard/{audit}/temp/{audit}_{'_'.join(filerr.rsplit('_')[4:]).split('.')[0].lower()}/{filerr}"
    
    multipart_upload = s3.create_multipart_upload(
            Bucket=bucket, Key=path_csv
        )
    parts = []
    
    y=z.open(filerr, force_zip64=True)
    i = 0
    for piece in read_in_chunks(y, chunk_size):
        print(f"Transferring chunk {i+1}...")
        part = transfer_chunk_from_ftp_to_s3(
            piece,
            s3,
            multipart_upload,
            bucket,
            path_csv,
            i + 1,
            chunk_size,
        )
        i= i+1
        parts.append(part)
        print("Chunk {} Transferred Successfully!".format(i))

    part_info = {"Parts": parts}
    s3.complete_multipart_upload(
    Bucket=bucket,
    Key=path_csv,
    UploadId=multipart_upload["UploadId"],
    MultipartUpload=part_info,
    )
    print(f"All chunks Transferred to S3 bucket! File Transfer successful for file {filerr}!")
    
    y.close()
    
    
    
    
    
    




    
    




    
#     try:
#         expected = spark.read.csv(f's3://{bucket}/{path_csv}', sep=";", header="true")
#         expected.write.csv(f"s3://{bucket}/dev/standard/{audit}/{audit}_{'_'.join(filerr.rsplit('_')[4:]).split('.')[0].lower()}", compression='gzip', mode = "append", header = "true")
        
#     except Exception as e:
#         raise e
#         continue

# del_temp = f"dev/standard/{audit}/temp/"    
# bucket_obj.objects.filter(Prefix=del_temp).delete()
    
    

# prefixes = ['.csv', '.CSV']
# csv_files = [n.key for n in bucket_obj.objects.filter(Prefix=zip_file[:-4] + "/") if not any(prefix in n for prefix in prefixes)]

# print(csv_files)
# try:
#     expected = pd.read_csv(f"s3://{bucket}/{arcname}", header=true)
# except:
#     print(f"falha na leitura do arquivo {filerr}")
#     continue
# print(expected)
# # expected.to_csv(f's3://{bucket}/{key}/{y}.csv.gz', compression='gzip', index=False)    
    
    
    
    
    
   
