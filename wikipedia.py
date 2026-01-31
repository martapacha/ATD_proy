# -*- coding: utf-8 -*-
"""
Created on Fri Jan 30 01:04:05 2026

@author: Usuario
"""

import requests
from bs4 import BeautifulSoup
import csv
import re
import pandas as pd


url = "https://es.wikipedia.org/wiki/Anexo:Aeropuertos_de_Espa%C3%B1a"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")


tabla = soup.find("table", class_="wikitable")
filas = tabla.find_all("tr")

datos = []


for fila in filas[1:]:
    columnas = fila.find_all("td")
    if len(columnas) >= 4:
        aeropuerto_publico = columnas[0].get_text(strip=True)
        localizacion = columnas[2].get_text(strip=True)
        icao = columnas[3].get_text(strip=True)

        
        aeropuerto_publico = re.sub(r"\[.*?\]", "", aeropuerto_publico).strip()
        localizacion = re.sub(r"\[.*?\]", "", localizacion).strip()
        icao = re.sub(r"\[.*?\]", "", icao).strip()

        
        if icao != "":
            datos.append([aeropuerto_publico, localizacion, icao])

print(f"Aeropuertos encontrados: {len(datos)}")


nombre_del_archivo = "aeropuertos.csv"

with open(
    nombre_del_archivo,
    "w",
    newline="",
    encoding="utf-8-sig"
) as f:
    writer = csv.writer(f, delimiter=";")
    writer.writerow(["aeropuerto_publico", "localizacion", "icao"])
    writer.writerows(datos)

print(f"¡Hecho! El archivo se ha guardado como '{nombre_del_archivo}' en tu carpeta actual.")


df_check = pd.read_csv(nombre_del_archivo, sep=";")
print("\nPrimeras filas del archivo generado:")
print(df_check.head())