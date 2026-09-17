"""
etl.py â€” Taller 1: ETL con PySpark (Online Retail Dataset)

Cubre todas las operaciones requeridas:
  1.  Lectura de datos           spark.read.csv()
  2.  SelecciÃ³n de columnas      select()
  3.  Filtrado de datos          filter() / where()
  4.  Ordenamiento               orderBy()
  5.  Agregaciones               sum(), avg(), min(), max(), count()
  6.  AgrupaciÃ³n de datos        groupBy() + agg()
  7.  CreaciÃ³n de columnas       withColumn()
  8.  Uniones (joins)            join()
  9.  Funciones de ventana       rank(), row_number()
  10. ExportaciÃ³n                write.csv()

Responde las 10 preguntas del taller y exporta un CSV por resultado.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# â”€â”€ Rutas â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
BASE_DIR   = os.path.dirname(__file__)
INPUT_CSV  = os.path.join(BASE_DIR, "data", "online_retail.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def save(df, name: str) -> None:
    """Guarda un DataFrame como CSV en la carpeta output/."""
    path = os.path.join(OUTPUT_DIR, name)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    print(f"   â†’ Exportado: output/{name}/")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # â”€â”€ 1. Crear sesiÃ³n Spark â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    spark = (
        SparkSession.builder
        .appName("Taller1_OnlineRetail")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("\n" + "=" * 60)
    print("  Taller 1 â€” ETL con PySpark (Online Retail Dataset)")
    print("=" * 60)

    # â”€â”€ 2. LECTURA DE DATOS (spark.read.csv) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n[EXTRACT] Leyendo CSV...")
    raw_df = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", False)
        .load(INPUT_CSV)
    )
    print(f"   Schema original:")
    raw_df.printSchema()
    print(f"   Filas totales (raw): {raw_df.count():,}")

    # â”€â”€ 3. TRANSFORMACIONES INICIALES â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # SelecciÃ³n de columnas relevantes (select)
    df = raw_df.select(
        "InvoiceNo",
        "StockCode",
        "Description",
        F.col("Quantity").cast("int"),
        F.to_timestamp(F.col("InvoiceDate"), "M/d/yyyy H:mm").alias("InvoiceDate"),
        F.col("UnitPrice").cast("double"),
        "CustomerID",
        "Country"
    )

    # Columna derivada: Revenue = Quantity * UnitPrice (withColumn)
    df = df.withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))

    # Columna derivada: Mes y AÃ±o de la factura
    df = df.withColumn("InvoiceMonth", F.month("InvoiceDate"))
    df = df.withColumn("InvoiceYear",  F.year("InvoiceDate"))

    # Filtro: sÃ³lo transacciones vÃ¡lidas (Quantity > 0, UnitPrice > 0)
    df_clean = df.filter((F.col("Quantity") > 0) & (F.col("UnitPrice") > 0))

    # Separar devoluciones (InvoiceNo empieza con 'C')
    df_returns  = df.filter(F.col("InvoiceNo").startswith("C"))
    df_sales    = df_clean

    df_sales.cache()
    print(f"   Filas limpias (ventas): {df_sales.count():,}")
    print(f"   Filas devoluciones   : {df_returns.count():,}")

    print("\n[TRANSFORM] Respondiendo las 10 preguntas del taller...")

    # â”€â”€ PREGUNTA 1: NÃºmero total de facturas â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    total_invoices = df.select("InvoiceNo").distinct().count()
    print(f"\n[P1] Total de facturas distintas : {total_invoices:,}")
    df.select("InvoiceNo").distinct().orderBy("InvoiceNo") \
      .write.mode("overwrite").option("header", True) \
      .csv(os.path.join(OUTPUT_DIR, "p1_total_invoices"))
    # Guardamos tambiÃ©n el resumen como fila Ãºnica
    spark.createDataFrame([(total_invoices,)], ["total_facturas"]) \
         .coalesce(1).write.mode("overwrite").option("header", True) \
         .csv(os.path.join(OUTPUT_DIR, "p1_resumen_total_invoices"))
    print("   â†’ Exportado: output/p1_total_invoices/")

    # â”€â”€ PREGUNTA 2: NÃºmero de clientes Ãºnicos â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    total_customers = df_sales.filter(F.col("CustomerID").isNotNull()) \
                               .select("CustomerID").distinct().count()
    print(f"\n[P2] Clientes Ãºnicos             : {total_customers:,}")
    spark.createDataFrame([(total_customers,)], ["clientes_unicos"]) \
         .coalesce(1).write.mode("overwrite").option("header", True) \
         .csv(os.path.join(OUTPUT_DIR, "p2_unique_customers"))
    print("   â†’ Exportado: output/p2_unique_customers/")

    # â”€â”€ PREGUNTA 3: Ingreso total (Quantity * UnitPrice) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ingreso_total = df_sales.agg(
        F.round(F.sum("Revenue"), 2).alias("ingreso_total")
    )
    ingreso_total.show()
    save(ingreso_total, "p3_ingreso_total")

    # â”€â”€ PREGUNTA 4: Producto mÃ¡s vendido en cantidad â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # groupBy + agg (sum)
    producto_mas_vendido = (
        df_sales
        .groupBy("StockCode", "Description")
        .agg(F.sum("Quantity").alias("total_cantidad"))
        .orderBy(F.desc("total_cantidad"))
    )
    print(f"\n[P4] Producto mÃ¡s vendido (top 10):")
    producto_mas_vendido.show(10, truncate=False)
    save(producto_mas_vendido, "p4_producto_mas_vendido")

    # â”€â”€ PREGUNTA 5: Cliente con mayor volumen de compra en dinero â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    cliente_top = (
        df_sales
        .filter(F.col("CustomerID").isNotNull())
        .groupBy("CustomerID")
        .agg(F.round(F.sum("Revenue"), 2).alias("total_compras"))
        .orderBy(F.desc("total_compras"))
    )
    print(f"\n[P5] Top 10 clientes por volumen de compra:")
    cliente_top.show(10)
    save(cliente_top, "p5_cliente_mayor_compra")

    # â”€â”€ PREGUNTA 6: Top 5 paÃ­ses fuera de Reino Unido â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    top_paises = (
        df_sales
        .filter(F.col("Country") != "United Kingdom")
        .groupBy("Country")
        .agg(F.round(F.sum("Revenue"), 2).alias("total_revenue"))
        .orderBy(F.desc("total_revenue"))
        .limit(5)
    )
    print(f"\n[P6] Top 5 paÃ­ses fuera de UK:")
    top_paises.show()
    save(top_paises, "p6_top5_paises")

    # â”€â”€ PREGUNTA 7: Ticket promedio por factura â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ticket_por_factura = (
        df_sales
        .groupBy("InvoiceNo")
        .agg(F.round(F.sum("Revenue"), 2).alias("total_factura"))
    )
    ticket_promedio = ticket_por_factura.agg(
        F.round(F.avg("total_factura"), 2).alias("ticket_promedio"),
        F.round(F.min("total_factura"), 2).alias("ticket_minimo"),
        F.round(F.max("total_factura"), 2).alias("ticket_maximo")
    )
    print(f"\n[P7] Ticket promedio por factura:")
    ticket_promedio.show()
    save(ticket_promedio, "p7_ticket_promedio")

    # â”€â”€ PREGUNTA 8: Min, Max y Promedio de productos por factura â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    productos_por_factura = (
        df_sales
        .groupBy("InvoiceNo")
        .agg(F.count("StockCode").alias("num_productos"))
    )
    stats_productos = productos_por_factura.agg(
        F.min("num_productos").alias("min_productos"),
        F.max("num_productos").alias("max_productos"),
        F.round(F.avg("num_productos"), 2).alias("avg_productos")
    )
    print(f"\n[P8] Min/Max/Avg productos por factura:")
    stats_productos.show()
    save(stats_productos, "p8_stats_productos_por_factura")

    # â”€â”€ PREGUNTA 9: Mes con mÃ¡s ventas â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    ventas_por_mes = (
        df_sales
        .groupBy("InvoiceYear", "InvoiceMonth")
        .agg(F.round(F.sum("Revenue"), 2).alias("total_revenue"))
        .orderBy(F.desc("total_revenue"))
    )
    print(f"\n[P9] Ventas por mes (top 12):")
    ventas_por_mes.show(12)
    save(ventas_por_mes, "p9_ventas_por_mes")

    # â”€â”€ PREGUNTA 10: Porcentaje de facturas con devoluciones â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    total_all      = df.select("InvoiceNo").distinct().count()
    total_devol    = df_returns.select("InvoiceNo").distinct().count()
    pct_devol      = round(total_devol / total_all * 100, 2)
    print(f"\n[P10] Facturas con devoluciones : {total_devol:,} / {total_all:,} = {pct_devol}%")
    spark.createDataFrame(
        [(total_all, total_devol, pct_devol)],
        ["total_facturas", "facturas_con_devolucion", "porcentaje_devolucion"]
    ).coalesce(1).write.mode("overwrite").option("header", True) \
     .csv(os.path.join(OUTPUT_DIR, "p10_pct_devoluciones"))
    print("   â†’ Exportado: output/p10_pct_devoluciones/")

    # â”€â”€ BONUS: JOIN â€” Enriquecer ventas con resumen de cliente â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n[JOIN] UniÃ³n ventas + resumen de cliente...")
    resumen_cliente = (
        df_sales
        .filter(F.col("CustomerID").isNotNull())
        .groupBy("CustomerID")
        .agg(
            F.round(F.sum("Revenue"), 2).alias("gasto_total"),
            F.count("InvoiceNo").alias("num_transacciones"),
            F.round(F.avg("Revenue"), 2).alias("ticket_promedio_cliente")
        )
    )
    # join entre df_sales (transacciones) y resumen_cliente
    df_enriquecido = df_sales \
        .filter(F.col("CustomerID").isNotNull()) \
        .join(resumen_cliente, on="CustomerID", how="left")
    save(df_enriquecido.select(
        "CustomerID", "InvoiceNo", "StockCode", "Description",
        "Quantity", "UnitPrice", "Revenue",
        "gasto_total", "num_transacciones", "ticket_promedio_cliente"
    ).orderBy("CustomerID"), "bonus_ventas_enriquecidas")

    # â”€â”€ BONUS: Funciones de ventana â€” Ranking de clientes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n[WINDOW] Ranking de clientes por gasto total...")
    window_rank = Window.orderBy(F.desc("gasto_total"))
    ranking_clientes = resumen_cliente \
        .withColumn("rank",       F.rank().over(window_rank)) \
        .withColumn("row_number", F.row_number().over(window_rank)) \
        .orderBy("rank")
    ranking_clientes.show(15)
    save(ranking_clientes, "bonus_ranking_clientes")

    # â”€â”€ 4. Resumen final â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n" + "=" * 60)
    print("  RESUMEN DE RESULTADOS")
    print("=" * 60)
    print(f"  P1  Â· Total facturas          : {total_invoices:,}")
    print(f"  P2  Â· Clientes Ãºnicos          : {total_customers:,}")
    ingreso_val = ingreso_total.collect()[0]["ingreso_total"]
    print(f"  P3  Â· Ingreso total            : Â£{ingreso_val:,.2f}")
    top_prod = producto_mas_vendido.first()
    print(f"  P4  Â· Producto mÃ¡s vendido     : {top_prod['Description']} ({top_prod['total_cantidad']:,} uds)")
    top_cli  = cliente_top.first()
    print(f"  P5  Â· Cliente top              : ID {top_cli['CustomerID']}  (Â£{top_cli['total_compras']:,.2f})")
    print(f"  P6  Â· Top 5 paÃ­ses (ver CSV)")
    tp_val   = ticket_promedio.collect()[0]["ticket_promedio"]
    print(f"  P7  Â· Ticket promedio          : Â£{tp_val:,.2f}")
    sp       = stats_productos.collect()[0]
    print(f"  P8  Â· Productos/factura        : min={sp['min_productos']} max={sp['max_productos']} avg={sp['avg_productos']}")
    top_mes  = ventas_por_mes.first()
    print(f"  P9  Â· Mes con mÃ¡s ventas       : {int(top_mes['InvoiceYear'])}-{int(top_mes['InvoiceMonth']):02d}  (Â£{top_mes['total_revenue']:,.2f})")
    print(f"  P10 Â· % facturas devoluciones  : {pct_devol}%")
    print("=" * 60)
    print(f"\n  Todos los resultados exportados en: {OUTPUT_DIR}")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
