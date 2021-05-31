import boto3
import zipfile
import io
import sys
import time
import math
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.transforms import *


# Abrindo sessões
spark = SparkSession.builder.getOrCreate()

# Input das variáveis
bucket = "abt-aa-brasil-jarvis-202006001-nonprod"
date = "2021-04-03"
chunk_size = 524288000
auditoria = "GPS"

audit = auditoria.lower()
input_raw = f"raw/{audit}"
key = f"{input_raw}/{date[0:8]+'01'}/"

s3 = boto3.client('s3')
s3r = boto3.resource('s3')
bucket_obj = s3r.Bucket(bucket)


def read_in_chunks(file_object, chunk_size):
    """
    Lazy function (generator) to read object piece by piece
    :param file_object: object that you want to read piece by piece, must be type Bytes
    :param chunk_size: int with the length in bytes to read
    :return: yield of object read
    """
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def transfer_chunk_to_s3(piece, s3, multipart_upload, bucket, path_s3, part_number, chunk_size):
    """
    :param piece: piece of the object in bytes to transfer
    :param s3: boto3 s3 client
    :param multipart_upload: multipart upload object created by create_multipart_upload from s3 client
    :param bucket: bucket from s3 where it will be saved
    :param path_s3: path (key) from s3 where it will be saved
    :param part_number: iterator that defines which part belongs
    :param chunk_size: int with the length in bytes to read
    :return: json with PartNumber and its respective ETag
    """
    start_time = time.time()

    part = s3.upload_part(
        Bucket=bucket,
        Key=path_s3,
        PartNumber=part_number,
        UploadId=multipart_upload["UploadId"],
        Body=piece
    )
    end_time = time.time()
    total_seconds = end_time - start_time
    print(f"total seconds taken: {total_seconds}\n")
    part_output = {"PartNumber": part_number, "ETag": part["ETag"]}
    return part_output


# INICIO
zip_file = [n.key for n in bucket_obj.objects.filter(Prefix=key) if ".zip" in n.key]

# CHECK PARA VER SE EXISTE APENAS UM ZIP NA PASTA
if len(zip_file) > 1:
    sys.exit(f"Zip already exists in object {key}")

## DESCOMPRESSÃO DO ARQUIVO
# LEITURA DO ZIP EM OBJETO
obj = s3r.Object(
    bucket_name=bucket,
    key=zip_file[0]
)
# TRANSFORMANDO PARA BYTE E PEGANDO A LISTA DE ARQUIVOS DENTRO DO ZIP
buffer = io.BytesIO(obj.get()["Body"].read())
z = zipfile.ZipFile(buffer)
list_full = z.namelist()

# DA LISTA DE ARQUIVOS DO ZIP, REMOVE OS QUE TEM "Totals" EM SEU NOME'
list_not_total = [e for e in list_full if not "Totals" in e]

# PARA CADA ITEM DENTRO DA LISTA SEM TOTAL
for filerr in list_not_total:
    # CAMINHO ONDE SERÁ SALVO O CSV
    path_s3 = f"outros/temp/{audit}/{audit}_{filerr}/{filerr}"
    # CRIANDO OBJETO DE MULTIPART UPLOAD
    multipart_upload = s3.create_multipart_upload(
        Bucket=bucket, Key=path_s3
    )
    parts = []
    # ABRINDO A FILE "filerr" DE DENTRO DO ZIP
    y = z.open(filerr, force_zip64=True)
    i = 0
    # ETAPA PARA LEITURA EM CHUNKS, LOOP CONTENDO UM PEDAÇO DO CHUNK
    for piece in read_in_chunks(y, chunk_size):
        print(f"\nTransferring chunk {i + 1} from file {filerr}...")
        # TRANSFERINDO O CHUNK LIDO
        part = transfer_chunk_to_s3(
            piece,
            s3,
            multipart_upload,
            bucket,
            path_s3,
            i + 1,
            chunk_size
        )
        i = i + 1
        # PARTE MAIS IMPORTANTE, ONDE GUARDA LISTA DOS UPLOADS FEITOS EM ETAPAS
        parts.append(part)
        print(f"Chunk {i} Transferred Successfully!")

    part_info = {"Parts": parts}
    # ETAPA PRA JUNTAR TODAS AS PARTES QUE FORAM COMPLETADAS
    s3.complete_multipart_upload(
        Bucket=bucket,
        Key=path_s3,
        UploadId=multipart_upload["UploadId"],
        MultipartUpload=part_info
    )
    print(f"All chunks Transferred to S3 bucket! File Transfer successful for file {filerr}!")

    y.close()

    try:
        expected = spark.read.csv(f's3://{bucket}/{path_s3}', sep=";", header="true")
        print(f"Converting {filerr} to GZIP")
        # MUDANDO CAMINHO DE SALVAMENTO DA FACT POIS O NOME NAO VEM DO JEITO CERTO
        lista = ['PBS_FFF_BRA_GPS_M_8_202104.CSV', 'PBS_FFF_CATEG_BRA_GPS_M_8_202104.CSV']
        if any(prefix in filerr for prefix in lista):
            expected.write.csv(
                f"s3://{bucket}/dev/standard/{audit}/{audit}_fact_{'_'.join(filerr.rsplit('_')[2:-3]).split('.')[0].lower()}",
                compression='gzip', mode="overwrite", header="true")
        else:
            expected.write.csv(
                f"s3://{bucket}/dev/standard/{audit}/{audit}_{'_'.join(filerr.rsplit('_')[3:]).split('.')[0].lower()}",
                compression='gzip', mode="overwrite", header="true")
        print(f"File {filerr} converted to GZIP!")

    except Exception as e:
        print(f"Error: {e}")
        continue
print(f"Deleting temporary folder")

del_temp = f"outros/temp/{audit}/"
bucket_obj.objects.filter(Prefix=del_temp).delete()

print(f"Process completed!")

