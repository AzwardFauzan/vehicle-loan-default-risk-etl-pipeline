# Group 1 - CODA-018-RMT
# - Marshal Renjiro Rifath
# - Yohanes Bos
# - Azward Nurfauzan
# - Herlina

# Proyek ini menganalisis faktor-faktor yang memengaruhi gagal bayar pinjaman kendaraan menggunakan pipeline ETL berbasis PySpark dan Apache Airflow.
# Data hasil transformasi disimpan di PostgreSQL NeonDB dengan star schema dan divisualisasikan menggunakan Tableau untuk memberikan insight mengenai
# risiko nasabah dan performa pinjaman.

from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession

# ==========================
# Spark Session
# ==========================
spark = (
    SparkSession.builder
    .appName("Load to NeonDB")
    .getOrCreate()
)

# ==========================
# Database Configuration
# ==========================
DB_CONFIG = {
    "host": "ep-round-butterfly-apdw2yjr-pooler.c-7.us-east-1.aws.neon.tech",
    "database": "tugas_akhir",
    "user": "neondb_owner",
    "password": "npg_o6fiStThDF2Y",
    "port": "5432"
}

ENGINE_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    "?sslmode=require"
)

engine = create_engine(ENGINE_URL)

# ==========================
# Hapus isi tabel
# ==========================

with engine.begin() as conn:
    conn.execute(text("""
        TRUNCATE TABLE
            fact_loan_performance,
            dim_client,
            dim_location,
            dim_loan,
            dim_risk
        RESTART IDENTITY CASCADE;
    """))

print("Data lama berhasil dihapus.")

# ==========================
# Daftar tabel
# ==========================
TABLES = [
    "dim_client",
    "dim_location",
    "dim_loan",
    "dim_risk",
    "fact_loan_performance"
]

# ==========================
# Load setiap tabel
# ==========================
for table in TABLES:

    print(f"\nLoading {table}...")

    spark_df = spark.read.parquet(
        f"/opt/airflow/data/staging/{table}"
    )

    pdf = spark_df.toPandas()

    pdf.to_sql(
        name=table,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi"
    )

    print(f"{table}: {len(pdf)} rows inserted.")

spark.stop()