from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    round as _round,
    to_date,
    year,
    month
)


def crear_sesion():
    return SparkSession.builder \
        .appName("ETL_Online_Retail") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def limpiar_datos(df):
    print("Limpiando datos...")

    df = df.dropDuplicates()
    print(f"Duplicados eliminados. Filas restantes: {df.count()}")

    df = df.filter(col("CustomerID").isNotNull())
    print(f"Filas sin CustomerID eliminadas. Filas restantes: {df.count()}")

    df = df.fillna({"Description": "Sin descripción"})

    df = df.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))
    print(f"Filas con Quantity o UnitPrice inválidos eliminadas. Filas restantes: {df.count()}")

    df = df.withColumn("CustomerID", col("CustomerID").cast("integer"))
    df = df.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "M/d/yyyy H:mm"))

    return df


def crear_columnas_derivadas(df):
    print("Creando columnas derivadas...")

    df = df.withColumn(
        "IngresoTotal",
        _round(col("Quantity") * col("UnitPrice"), 2)
    )
    df = df.withColumn("Anio", year(col("InvoiceDate")))
    df = df.withColumn("Mes", month(col("InvoiceDate")))

    return df


def transformar(df):
    df = limpiar_datos(df)
    df = crear_columnas_derivadas(df)
    return df