# Tarea 3 – Procesamiento de datos con Apache Spark

Curso: Big Data (código 202016911) – UNAD
Estudiante: Margareth Quiroz

## Problema
[Qué pregunta responde el análisis. Por ejemplo: qué ciudades y categorías generan más ingresos.]

## Dataset
Dataset simulado de ventas de una tienda, de práctica, con 50.250 registros y 9 columnas: id_venta, fecha, ciudad, categoria, canal, cantidad, precio_unitario, descuento_pct y total.

## Qué hace el script (batch_spark.py)
[Explicar con sus palabras: carga, limpieza, transformación, operaciones con RDD, análisis y gráficas.]

## Cómo ejecutarlo
1. Crear el entorno virtual: py -3.12 -m venv venv
2. Activarlo: .\venv\Scripts\Activate.ps1
3. Instalar librerías: pip install -r requirements.txt
4. Convertir el Excel a CSV: python convertir_a_csv.py
5. Ejecutar: python batch_spark.py