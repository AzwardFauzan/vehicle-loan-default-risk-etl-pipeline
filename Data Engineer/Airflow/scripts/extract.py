# Group 1 - CODA-018-RMT
# - Marshal Renjiro Rifath
# - Yohanes Bos
# - Azward Nurfauzan
# - Herlina

# Proyek ini menganalisis faktor-faktor yang memengaruhi gagal bayar pinjaman kendaraan menggunakan pipeline ETL berbasis PySpark dan Apache Airflow.
# Data hasil transformasi disimpan di PostgreSQL NeonDB dengan star schema dan divisualisasikan menggunakan Tableau untuk memberikan insight mengenai
# risiko nasabah dan performa pinjaman.

import os
import shutil
import kagglehub

DATASET_ROOT_DIR = "/opt/airflow/data"
os.makedirs(DATASET_ROOT_DIR, exist_ok=True)

destination = os.path.join(DATASET_ROOT_DIR, "Train_Dataset.csv")

if not os.path.exists(destination):
    print("[extract] Downloading dataset...")

    path = kagglehub.dataset_download(
        "meastanmay/nbfi-vehicle-loan-repayment-dataset"
    )

    source = os.path.join(path, "Train_Dataset.csv")

    shutil.copy2(source, destination)

    print("[extract] Dataset downloaded.")
else:
    print("[extract] Dataset already exists.")

print(f"[extract] Dataset location: {destination}")