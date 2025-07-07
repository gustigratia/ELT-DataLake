from minio import Minio
import pandas as pd
from io import BytesIO
import tempfile
import os


client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)
tables = {'mata_kuliah', 'dosen', 'mahasiswa', 'kelas', 'frs', 'frs_kelas'}


source_bucket = "bronze"
target_bucket = "silver"

for table in tables:
    print(f"Cleaning... {table}")
    source_object_name = f"{table}.parquet"
    target_object_name = f"{table}_silver.parquet"

    response = client.get_object(source_bucket, source_object_name)
    buffer = BytesIO(response.read())

    df = pd.read_parquet(buffer)

    df = df.fillna("Tidak Ada Data")

    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False, engine="pyarrow")
    out_buffer.seek(0)

    if not client.bucket_exists(target_bucket):
        client.make_bucket(target_bucket)

    client.put_object(
        target_bucket,
        target_object_name,
        data=out_buffer,
        length=out_buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )


    print(f"Successfully uploaded to '{target_bucket}/{target_object_name}'")