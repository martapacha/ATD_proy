# -*- coding: utf-8 -*-

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
import random

RUTA_BASE = os.path.dirname(os.path.abspath(__file__))
INPUT_AEROPUERTOS = os.path.join(RUTA_BASE, 'aeropuertos_con_clima.csv')
OUTPUT_CLEAN = os.path.join(RUTA_BASE, 'altitudes_clasificadas.csv')

def scraping_altitud_web(icao):

    url = f"https://metar-taf.com/es/airport/{icao}"
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    ]
    headers = {'User-Agent': random.choice(user_agents)}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            celda_titulo = soup.find('td', string=lambda t: t and 'Elevación' in t)
            
            if celda_titulo:
                celda_valor = celda_titulo.find_next_sibling('td')
                texto_valor = celda_valor.get_text(strip=True)

                numero_limpio = ''.join(filter(str.isdigit, texto_valor))
                return int(numero_limpio)
    except Exception as e:
        print(f"Error scraping {icao}: {e}")
    
    return None

def clasificar_orografia(altitud_pies):
    if altitud_pies is None:
        return "Desconocido"
    
    if altitud_pies <= 600:
        return "Costa/Bajo"
    elif altitud_pies <= 1600:
        return "Interior"
    else:
        return "Montaña"

def ejecutar_pipeline_altitud():
    print("INICIANDO BÚSQUEDA DE ALTITUD")

    if not os.path.exists(INPUT_AEROPUERTOS):
        print(f"Error: No se encuentra {INPUT_AEROPUERTOS}")
        return

    try:
        df_aero = pd.read_csv(INPUT_AEROPUERTOS, sep=';', encoding='utf-8')
    except:
        df_aero = pd.read_csv(INPUT_AEROPUERTOS, sep=';', encoding='latin-1')

    correcciones = {'LEBL': '0076', 'LEBB': '1024E', 'GCLP': 'C649I', 'GCTS': 'C449C'}
    for icao, nuevo_id in correcciones.items():
        df_aero.loc[df_aero['icao'] == icao, 'ID_AEMET'] = nuevo_id

    df_objetivo = df_aero[df_aero['ID_AEMET'].notna()]
    
    print(f"Analizando orografía de {len(df_objetivo)} aeropuertos")

    lista_resultados = []

    for index, row in df_objetivo.iterrows():
        nombre = row['aeropuerto_publico']
        icao = str(row['icao']).strip()

        altitud = scraping_altitud_web(icao)

        if altitud is None:
            if icao == 'LERJ': altitud = 1154
            elif icao == 'LEMD': altitud = 1998 
            else: altitud = 0 

        categoria = clasificar_orografia(altitud)
        
        print(f"{icao}: {altitud} ft -> {categoria}")

        lista_resultados.append({
            'AEROPUERTO': nombre,
            'ICAO': icao,
            'ALTITUD_PIES': altitud,
            'CATEGORIA_ALTITUD': categoria
        })
        
        time.sleep(random.uniform(1.5, 3.0))

    if lista_resultados:
        df_final = pd.DataFrame(lista_resultados)
        df_final.to_csv(OUTPUT_CLEAN, index=False, sep=';', encoding='utf-8-sig')
        print(f"Archivo 'altitudes_clasificadas.csv' guardado correctamente.")
    else:
        print("No se obtuvieron datos.")

if __name__ == "__main__":
    ejecutar_pipeline_altitud()