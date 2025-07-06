from minio import Minio
import pandas as pd
from io import BytesIO

def load_parquet(bucket, object_name):
    resp = client.get_object(bucket, object_name)
    buffer = BytesIO(resp.read())
    return pd.read_parquet(buffer)
    print(f"Load success {object_name}")

def upload_parquet(df, bucket, object_name):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client.put_object(bucket, object_name, data=buffer, length=buffer.getbuffer().nbytes, content_type="application/octet-stream")
    print(f"Uploaded: {object_name}")

def get_quarter(month):
    if month <= 3:
        return 1
    elif month <= 6:
        return 2
    elif month <= 9:
        return 3
    else:
        return 4

client = Minio(
    "localhost:9000",
    access_key="gusti",
    secret_key="delpiera06",
    secure=False
)

bucket_silver = "silver"
bucket_gold = "gold"

if not client.bucket_exists(bucket_gold):
    client.make_bucket(bucket_gold)

df_mahasiswa = load_parquet(bucket_silver, "mahasiswa_silver.parquet")
df_dosen = load_parquet(bucket_silver, "dosen_silver.parquet")
df_kelas = load_parquet(bucket_silver, "kelas_silver.parquet")
df_mk = load_parquet(bucket_silver, "mata_kuliah_silver.parquet")
df_frs = load_parquet(bucket_silver, "frs_silver.parquet")
df_frs_kelas = load_parquet(bucket_silver, "frs_kelas_silver.parquet")

# Dim_mhs
df_dm = df_mahasiswa.merge(df_dosen, left_on='id_dosen_wali', right_on='ID_dosen', how='left')
df_dm = df_dm[["nrp", "nama_mahasiswa", "ipk", "ips", "nama_dosen"]].copy()
df_dm.rename(columns={"nama_dosen": "dosen_wali"}, inplace=True)
df_dm.insert(0, "SK_Mahasiswa", range(1, len(df_dm)+1))
upload_parquet(df_dm, bucket_gold, "dim_mahasiswa.parquet")

# Dim_kelas
df_dk = df_kelas.merge(df_dosen, on="ID_dosen", how="inner").merge(df_mk, on="ID_MK", how="left")
df_dk = df_dk[["ID_kelas", "kode_mk", "nama_MK", "sks", "nama_dosen", "kelas"]].copy()
df_dk.rename(columns={"nama_dosen": "dosen_pengampu"}, inplace=True)
df_dk.insert(0, "SK_Kelas", range(1, len(df_dk)+1))
upload_parquet(df_dk, bucket_gold, "dim_kelas.parquet")

# Dim_waktu
start_date = "2015-01-01"
end_date = "2028-09-08"
date_range = pd.date_range(start=start_date, end=end_date)
df_dw = pd.DataFrame({"date": date_range})
df_dw["year"] = df_dw["date"].dt.year
df_dw["month"] = df_dw["date"].dt.month
df_dw["quarter"] = df_dw["month"].apply(get_quarter)
df_dw["day"] = df_dw["date"].dt.day
df_dw.insert(0, "SK_Waktu", range(1, len(df_dw)+1))
upload_parquet(df_dw, bucket_gold, "dim_waktu.parquet")

# Fact_frs
df_fakta = df_frs.merge(df_frs_kelas, on="ID_FRS", how="inner")
df_fakta = df_fakta.merge(df_dm[["SK_Mahasiswa", "nrp"]], on="nrp", how="inner")
df_fakta = df_fakta.merge(df_dk[["SK_Kelas", "ID_kelas"]], on="ID_kelas", how="inner")
df_fakta["tanggal_persetujuan"] = pd.to_datetime(df_fakta["tanggal_persetujuan"], errors='coerce')
df_fakta = df_fakta.merge(df_dk[["sks", "ID_kelas"]], on="ID_kelas", how="inner")
df_fakta = df_fakta.merge(
    df_dw[["SK_Waktu", "date"]],
    left_on="tanggal_persetujuan",
    right_on="date",
    how="left"
)

df_fakta_final = df_fakta[[
    "SK_Mahasiswa",
    "SK_Kelas",
    "SK_Waktu",
    "sks"
]].copy()
upload_parquet(df_fakta_final, bucket_gold, "frs_fact.parquet")




