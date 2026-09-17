from pyspark.sql.functions import (
    col,
    sum as _sum,
    avg,
    min as _min,
    max as _max,
    desc,
    rank
)
from pyspark.sql.window import Window


def generar_resultados(df, spark, df_original):
    total_facturas = df.select("InvoiceNo").distinct().count()
    print(f"1. Total de facturas: {total_facturas}")

    clientes_unicos = df.select("CustomerID").distinct().count()
    print(f"2. Clientes únicos: {clientes_unicos}")

    ingreso_total = df.agg(_sum("IngresoTotal").alias("IngresoTotal")).collect()[0][0]
    print(f"3. Ingreso total: {ingreso_total}")

    producto_top = df.groupBy("StockCode", "Description") \
        .agg(_sum("Quantity").alias("TotalVendido")) \
        .orderBy(desc("TotalVendido")) \
        .limit(1)
    producto_top.show()
    producto_top.write.mode("overwrite").option("header", True).csv("output/p4_producto_mas_vendido")

    cliente_top = df.groupBy("CustomerID") \
        .agg(_sum("IngresoTotal").alias("TotalComprado")) \
        .orderBy(desc("TotalComprado")) \
        .limit(1)
    cliente_top.show()
    cliente_top.write.mode("overwrite").option("header", True).csv("output/p5_cliente_top")

    paises_top = df.filter(col("Country") != "United Kingdom") \
        .groupBy("Country") \
        .agg(_sum("IngresoTotal").alias("TotalComprado")) \
        .orderBy(desc("TotalComprado")) \
        .limit(5)
    paises_top.show()
    paises_top.write.mode("overwrite").option("header", True).csv("output/p6_top_paises")

    ticket_prom = df.groupBy("InvoiceNo") \
        .agg(_sum("IngresoTotal").alias("TotalFactura")) \
        .agg(avg("TotalFactura").alias("TicketPromedio"))
    ticket_prom.show()
    ticket_prom.write.mode("overwrite").option("header", True).csv("output/p7_ticket_promedio")

    productos_por_factura = df.groupBy("InvoiceNo") \
        .agg(_sum("Quantity").alias("TotalProductos")) \
        .agg(
            _min("TotalProductos").alias("MinProductos"),
            _max("TotalProductos").alias("MaxProductos"),
            avg("TotalProductos").alias("PromedioProductos")
        )
    productos_por_factura.show()
    productos_por_factura.write.mode("overwrite").option("header", True).csv("output/p8_productos_por_factura")

    ventas_mes = df.groupBy("Anio", "Mes") \
        .agg(_sum("IngresoTotal").alias("VentasMes")) \
        .orderBy(desc("VentasMes")) \
        .limit(1)
    ventas_mes.show()
    ventas_mes.write.mode("overwrite").option("header", True).csv("output/p9_mes_mas_ventas")

    total_facturas_distintas = df_original.select("InvoiceNo").distinct().count()
    facturas_con_devolucion = df_original.filter(col("Quantity") < 0) \
        .select("InvoiceNo").distinct().count()
    porcentaje = (facturas_con_devolucion / total_facturas_distintas) * 100 if total_facturas_distintas > 0 else 0
    print(f"10. Porcentaje de facturas con devoluciones: {porcentaje:.2f}%")

    resultado_devoluciones = spark.createDataFrame([
        (total_facturas_distintas, facturas_con_devolucion, round(porcentaje, 2))
    ], ["TotalFacturas", "FacturasConDevolucion", "PorcentajeDevoluciones"])
    resultado_devoluciones.write.mode("overwrite").option("header", True).csv("output/p10_devoluciones")

    ventana = Window.orderBy(desc("TotalComprado"))
    ranking_clientes = df.groupBy("CustomerID") \
        .agg(_sum("IngresoTotal").alias("TotalComprado")) \
        .withColumn("Rank", rank().over(ventana))
    ranking_clientes.show(10)
    ranking_clientes.write.mode("overwrite").option("header", True).csv("output/ranking_clientes")

    datos_paises = spark.createDataFrame([
        ("United Kingdom", "Europa"),
        ("Germany", "Europa"),
        ("France", "Europa"),
        ("Australia", "Oceanía"),
        ("Netherlands", "Europa"),
        ("Spain", "Europa"),
        ("Switzerland", "Europa"),
        ("Belgium", "Europa"),
        ("Sweden", "Europa"),
        ("Japan", "Asia"),
        ("EIRE", "Europa"),
        ("Norway", "Europa"),
        ("Portugal", "Europa"),
        ("Italy", "Europa"),
        ("Finland", "Europa"),
        ("Austria", "Europa"),
        ("Denmark", "Europa"),
        ("Poland", "Europa"),
        ("Iceland", "Europa"),
        ("Channel Islands", "Europa")
    ], ["Country", "Continente"])

    df_con_continente = df.join(datos_paises, on="Country", how="left")
    ventas_continente = df_con_continente.groupBy("Continente") \
        .agg(_sum("IngresoTotal").alias("VentasPorContinente")) \
        .orderBy(desc("VentasPorContinente"))
    ventas_continente.show()
    ventas_continente.write.mode("overwrite").option("header", True).csv("output/ventas_por_continente")