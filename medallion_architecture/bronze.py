import pandas as pd  
from sqlalchemy import create_engine
from minio import Minio
from minio.error import S3Error
from io import BytesIO

engine = create_engine("mysql+pymysql://root:@localhost:3308/dummy_data")

minio_config = {
    'endpoint': 'localhost:9000',
    'access_key': 'gusti',
    'secret_key': 'delpiera06',
    'bucket': 'bronze',
    'secure': False
}

client = Minio(
    minio_config['endpoint'],
    access_key=minio_config['access_key'],
    secret_key=minio_config['secret_key'],
    secure=minio_config['secure']
)

if not client.bucket_exists(minio_config['bucket']):
    client.make_bucket(minio_config['bucket'])

tables = {'mata_kuliah', 'dosen', 'mahasiswa', 'kelas', 'frs', 'frs_kelas'}

for table in tables:
    print(f"Processing table: {table}")
    df = pd.read_sql(f"SELECT * FROM {table}", con=engine)
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    object_name = f"{table}.parquet"
    client.put_object(
        minio_config['bucket'],
        object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )

    print(f"'{table}' table successfully uploaded to '{minio_config['bucket']}/{object_name}'")
