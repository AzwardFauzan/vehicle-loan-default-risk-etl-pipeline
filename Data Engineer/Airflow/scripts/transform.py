# Group 1 - CODA-018-RMT
# - Marshal Renjiro Rifath
# - Yohanes Bos
# - Azward Nurfauzan
# - Herlina

# Proyek ini menganalisis faktor-faktor yang memengaruhi gagal bayar pinjaman kendaraan menggunakan pipeline ETL berbasis PySpark dan Apache Airflow.
# Data hasil transformasi disimpan di PostgreSQL NeonDB dengan star schema dan divisualisasikan menggunakan Tableau untuk memberikan insight mengenai
# risiko nasabah dan performa pinjaman.

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =====================================
# Spark Session
# =====================================
spark = (
    SparkSession.builder
    .appName("Transform")
    .getOrCreate()
)

# =====================================
# Read Dataset
# =====================================
INPUT_FILE = "/opt/airflow/data/Train_Dataset.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(INPUT_FILE)
    .cache()
)

# Materialize cache
df.count()

print("[transform] Dataset berhasil dibaca.")

# =====================================
# Casting Data Type
# =====================================
type_mapping = {
    # Integer
    "ID": "int",
    "Client_Family_Members": "int",
    "Child_Count": "int",
    "Employed_Days": "int",
    "ID_Days": "int",
    "Application_Process_Hour": "int",

    # Double
    "Client_Income": "double",
    "Credit_Amount": "double",
    "Loan_Annuity": "double",
    "Own_House_Age": "double",
    "Population_Region_Relative": "double",
    "Cleint_City_Rating": "double",
    "Credit_Bureau": "double",
    "Score_Source_1": "double",
    "Score_Source_2": "double",
    "Score_Source_3": "double",
    "Social_Circle_Default": "double",
    "Phone_Change": "double",

    # Boolean
    "Car_Owned": "boolean",
    "House_Own": "boolean",
    "Active_Loan": "boolean",
    "Default": "boolean"
}

for column, dtype in type_mapping.items():
    if column in df.columns:
        df = df.withColumn(column, F.col(column).cast(dtype))

# =====================================
# Feature Engineering
# =====================================
if "Age_Days" in df.columns:
    df = df.withColumn(
        "age",
        (F.abs(F.col("Age_Days")) / 365.25).cast("int")
    )

print("[transform] Casting selesai.")

# =====================================
# Dimension Client
# =====================================
dim_client = (
    df.select(
        F.col("ID").alias("client_id"),
        F.col("age"),
        F.col("Client_Education").alias("education"),
        F.col("Client_Marital_Status").alias("marital_status"),
        F.col("Client_Income_Type").alias("income_type"),
        F.col("Client_Occupation").alias("occupation"),
        F.col("Client_Housing_Type").alias("housing_type"),
        F.col("Client_Family_Members").alias("family_members"),
        F.col("Child_Count").alias("child_count"),
        F.col("Type_Organization").alias("organization"),
        F.col("House_Own").alias("house_own"),
        F.col("Own_House_Age").alias("own_house_age"),
        F.col("Car_Owned").alias("car_owned"),
        F.col("Client_Income").alias("client_income"),
        F.col("Employed_Days").alias("employed_days"),
        F.col("ID_Days").alias("id_days")
    )
    .dropDuplicates(["client_id"])
)

# =====================================
# Dimension Loan
# =====================================
dim_loan = (
    df.select(
        F.col("ID").alias("loan_id"),
        F.col("Loan_Contract_Type").alias("contract_type"),
        F.col("Credit_Amount").alias("credit_amount"),
        F.col("Loan_Annuity").alias("loan_annuity"),
        F.col("Active_Loan").alias("active_loan"),
        F.col("Accompany_Client").alias("accompany_client"),
        F.col("Application_Process_Hour").alias("application_process_hour")
    )
    .dropDuplicates(["loan_id"])
)

# =====================================
# Dimension Risk
# =====================================
dim_risk = (
    df.select(
        F.col("ID").alias("risk_id"),
        F.col("Credit_Bureau").alias("credit_bureau"),
        F.col("Score_Source_1").alias("score_source_1"),
        F.col("Score_Source_2").alias("score_source_2"),
        F.col("Score_Source_3").alias("score_source_3"),
        F.col("Social_Circle_Default").alias("social_circle_default"),
        F.col("Phone_Change").alias("phone_change")
    )
    .dropDuplicates(["risk_id"])
)

# =====================================
# Dimension Location
# =====================================
location_temp = (
    df.select(
        F.col("Cleint_City_Rating").alias("city_rating"),
        F.col("Population_Region_Relative").alias("population_relative")
    )
    .dropDuplicates()
)

window = Window.orderBy("city_rating", "population_relative")

dim_location = (
    location_temp
    .withColumn("location_id", F.row_number().over(window))
    .select(
        "location_id",
        "city_rating",
        "population_relative"
    )
)

# =====================================
# Fact Loan Performance
# =====================================
fact_loan_performance = (
    df.select(
        F.col("ID").alias("fact_id"),
        F.col("ID").alias("client_id"),
        F.col("ID").alias("loan_id"),
        F.col("ID").alias("risk_id"),
        F.col("Cleint_City_Rating").alias("city_rating"),
        F.col("Population_Region_Relative").alias("population_relative"),
        F.col("Default").alias("default")
    )
    .join(
        dim_location,
        on=["city_rating", "population_relative"],
        how="left"
    )
    .select(
        "fact_id",
        "client_id",
        "location_id",
        "risk_id",
        "loan_id",
        "default"
    )
)

# =====================================
# Logging
# =====================================
print(f"dim_client             : {dim_client.count()} rows")
print(f"dim_location           : {dim_location.count()} rows")
print(f"dim_loan               : {dim_loan.count()} rows")
print(f"dim_risk               : {dim_risk.count()} rows")
print(f"fact_loan_performance  : {fact_loan_performance.count()} rows")

# =====================================
# Save to Parquet
# =====================================
OUTPUT_DIR = "/opt/airflow/data/staging"
os.makedirs(OUTPUT_DIR, exist_ok=True)

dim_client.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dim_client")
dim_location.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dim_location")
dim_loan.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dim_loan")
dim_risk.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/dim_risk")
fact_loan_performance.write.mode("overwrite").parquet(
    f"{OUTPUT_DIR}/fact_loan_performance"
)

print("[transform] Semua tabel berhasil disimpan ke staging.")

# =====================================
# Cleanup
# =====================================
df.unpersist()

spark.stop()

print("[transform] ETL Transform selesai.")