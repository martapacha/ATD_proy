# -*- coding: utf-8 -*-
"""
Created on Fri Jan 30 01:07:14 2026

@author: Usuario
"""

import pandas as pd
import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ruta_entrada = os.path.join(BASE_DIR, "aeropuertos.csv")

df = pd.read_csv(
    ruta_entrada,
    sep=";",
    encoding="utf-8-sig"
)

df.columns = df.columns.str.strip()


df["localizacion"] = (
    df["localizacion"]
    .astype(str)
    .str.extract(r"\(([^)]+)\)", expand=False)
    .fillna(df["localizacion"])
)


ruta_salida = os.path.join(BASE_DIR, "aeropuertos_limpios.csv")

df.to_csv(
    ruta_salida,
    sep=";",
    index=False,
    encoding="utf-8-sig"
)

print("CSV limpio generado correctamente")
print(df.head())
