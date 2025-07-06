import argparse
from minio import Minio
from io import BytesIO
import pandas as pd

parser = argparse.ArgumentParser(description="View tables in MinIO (Choose layer and table)")
parser.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True, help="Layer to view")
parser.add_argument("--table", required=True, help="Table name to view (or 'all' to view all)")

args = parser.parse_args()
bucket_name = args.layer
selected_table = args.table.lower()

tables_dict = {
    "bronze": ['mata_kuliah', 'dosen', 'mahasiswa', 'kelas', 'frs', 'frs_kelas'],
    "silver": ['mata_kuliah', 'dosen', 'mahasiswa', 'kelas', 'frs', 'frs_kelas'],
    "gold":   ['dim_mahasiswa', 'dim_kelas', 'dim_waktu', 'frs_fact']
}

tables = tables_dict[bucket_name]

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

if selected_table == "all":
    tables_to_view = tables
elif selected_table in tables:
    tables_to_view = [selected_table]
else:
    print(f"Table '{selected_table}' not found in '{bucket_name}'.")
    exit(1)

for table in tables_to_view:
    if bucket_name == "silver":
        object_name = f"{table}_silver.parquet"
    else:
        object_name = f"{table}.parquet"

    try:
        print(f"\nViewing table: {object_name} (from {bucket_name})")
        response = client.get_object(bucket_name, object_name)
        buffer = BytesIO(response.read())
        df = pd.read_parquet(buffer, engine="pyarrow")
        print(df.head(), "\n")
    except Exception as e:
        print(f"Can't read {object_name} from bucket {bucket_name}: {e}\n")
