import pandas as pd

df = pd.read_excel("Datos/ventas_tienda.xlsx", sheet_name="datos")
df.to_csv("Datos/dataset.csv", index=False)
print(df.shape)