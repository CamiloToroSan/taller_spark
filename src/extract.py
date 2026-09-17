from ucimlrepo import fetch_ucirepo
import pandas as pd


def extraer_dataset(spark):
    print("Extrayendo dataset Online Retail desde UCI (id=352)...")
    online_retail = fetch_ucirepo(id=352)
    df_pandas = online_retail.data.original.copy()

    print(f"Dataset extraído: {df_pandas.shape[0]} filas, {df_pandas.shape[1]} columnas")
    print("Columnas:", list(df_pandas.columns))

    print("\n--- Diagnóstico de calidad ---")
    print("Valores nulos por columna:")
    print(df_pandas.isnull().sum())
    print(f"\nFilas duplicadas: {df_pandas.duplicated().sum()}")

    ruta_csv = "data/raw/online_retail.csv"
    df_pandas.to_csv(ruta_csv, index=False)
    print(f"\nCSV guardado en {ruta_csv}")

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(ruta_csv)

    print(f"Dataset leído con Spark: {df.count()} filas, {len(df.columns)} columnas")
    return df