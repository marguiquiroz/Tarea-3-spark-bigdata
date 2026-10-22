import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

os.makedirs("resultados", exist_ok=True)

spark = SparkSession.builder.appName("EDA").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---------- Carga y limpieza ----------
df = (spark.read.option("header", True).option("inferSchema", True)
      .csv("Datos/dataset.csv"))
df = df.dropDuplicates()
df = df.fillna({"ciudad": "Sin dato", "descuento_pct": 0.0})
df = df.filter((F.col("cantidad") > 0) & (F.col("precio_unitario") > 0))

# ---------- Transformación ----------
df = (df.withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
        .withColumn("periodo", F.date_format("fecha", "yyyy-MM")))
df.cache()

# ---------- RDD ----------
print("=== RDD ===")
rdd = df.select("categoria", "total").rdd.map(lambda r: (r["categoria"], r["total"]))
por_cat_rdd = rdd.reduceByKey(lambda a, b: a + b).collect()
print("Ventas por categoría (RDD):", sorted(por_cat_rdd, key=lambda x: -x[1]))
print("Ventas mayores a 1.000.000:", rdd.filter(lambda kv: kv[1] > 1000000).count())

# ---------- EDA con DataFrames ----------
print("\n=== Estadísticas de total ===")
df.select("cantidad", "precio_unitario", "total").describe().show()

print("=== Ventas por ciudad ===")
por_ciudad = (df.groupBy("ciudad")
                .agg(F.count("*").alias("ventas"), F.sum("total").alias("ingresos"))
                .orderBy(F.desc("ingresos")))
por_ciudad.show()

print("=== Ventas por categoría ===")
por_categoria = (df.groupBy("categoria")
                   .agg(F.count("*").alias("ventas"), F.sum("total").alias("ingresos"))
                   .orderBy(F.desc("ingresos")))
por_categoria.show()

print("=== Ventas por canal ===")
df.groupBy("canal").agg(F.count("*").alias("ventas"), F.sum("total").alias("ingresos")).show()

print("=== Ingresos por mes ===")
por_mes = (df.groupBy("periodo").agg(F.sum("total").alias("ingresos")).orderBy("periodo"))
por_mes.show(24)

# ---------- Gráficas ----------
p = por_categoria.toPandas()
plt.figure(figsize=(8, 5))
plt.barh(p["categoria"], p["ingresos"] / 1e6, color="steelblue")
plt.gca().invert_yaxis()
plt.title("Ingresos por categoría")
plt.xlabel("Millones de COP")
plt.tight_layout()
plt.savefig("resultados/ingresos_por_categoria.png", dpi=150)
plt.close()

m = por_mes.toPandas()
plt.figure(figsize=(12, 5))
plt.plot(m["periodo"], m["ingresos"] / 1e6, marker="o", color="darkorange")
plt.xticks(rotation=90)
plt.title("Ingresos mensuales")
plt.ylabel("Millones de COP")
plt.tight_layout()
plt.savefig("resultados/ingresos_mensuales.png", dpi=150)
plt.close()

c = por_ciudad.toPandas()
plt.figure(figsize=(8, 5))
plt.bar(c["ciudad"], c["ingresos"] / 1e6, color="seagreen")
plt.title("Ingresos por ciudad")
plt.ylabel("Millones de COP")
plt.tight_layout()
plt.savefig("resultados/ingresos_por_ciudad.png", dpi=150)
plt.close()

print("\nGráficas guardadas en la carpeta resultados/")
spark.stop()