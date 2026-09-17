from src.extract import extraer_dataset
from src.transform import crear_sesion, transformar
from src.analysis import generar_resultados

def main():
    spark = crear_sesion()

    #  CSV con spark.read.format("csv")
    df = extraer_dataset(spark)

    #  Guardar 
    df_original = df.cache()

    # limpiar y crear columnas derivadas
    df_transformado = transformar(df)

    # generar resultados y exportar a CSV
    generar_resultados(df_transformado, spark, df_original)

    spark.stop()
    print("Pipeline ETL finalizado correctamente.")

if __name__ == "__main__":
    main()