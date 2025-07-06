import duckdb
import argparse

parser = argparse.ArgumentParser(description="SQL Query to Parquet file to MinIO via DuckDB")
parser.add_argument("--query", type=str, required=True, help="Choose query to run")
args = parser.parse_args()

duckdb.sql("INSTALL httpfs; LOAD httpfs;")
duckdb.sql("""
    SET s3_region='us-east-1';
    SET s3_access_key_id='gusti';
    SET s3_secret_access_key='delpiera06';
    SET s3_endpoint='localhost:9000';
    SET s3_use_ssl='false';
    SET s3_url_style='path';
""")

queries = {
    "1": """
        SELECT * FROM read_parquet('s3://gold/dim_mahasiswa.parquet');
    """,
    "2": """
        SELECT 
            f.nama_mahasiswa,
            w.quarter
        FROM 
            read_parquet('s3://gold/dim_mahasiswa.parquet') f
        JOIN 
            read_parquet('s3://gold/frs_fact.parquet') m
        ON 
            f.SK_Mahasiswa = m.SK_Mahasiswa 
        JOIN 
            read_parquet('s3://gold/dim_waktu.parquet') w
        ON
            m.SK_Waktu = w.SK_Waktu;
    """,
    "3": """
        SELECT * FROM read_parquet('s3://gold/dim_mahasiswa.parquet') where ipk > 3.0 order by ipk desc;
        """,
    "4": """
        select * from read_parquet('s3://gold/dim_waktu.parquet') order by SK_Waktu asc;
        """,
    "5": """
        select * from read_parquet('s3://gold/dim_kelas.parquet') where sks = 3;
        """
}

selected_query = args.query
duckdb.sql(queries[selected_query]).show()