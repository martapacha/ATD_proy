# -*- coding: utf-8 -*-
"""
Created on Sat Jan 31 23:17:41 2026

@author: Usuario
"""

"""
Módulo AEMET (Estilo OpenSky):
- Descarga los datos brutos disponibles.
- Aplica una ventana de TIEMPO EXACTA (timestamp) de 24 horas (86.400 segundos).
- Trabaja en UTC para evitar errores de zona horaria local.
"""

import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta, timezone


RUTA_BASE = os.path.dirname(os.path.abspath(__file__))


API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJtYXJ0YXBhY2hhYWxhcmNvbkBnbWFpbC5jb20iLCJqdGkiOiI1NzJkOTQ1MS1lMDcwLTQ1YjMtYTgwNC00MjIzZWM4YjM0MTEiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc2OTUzMzc4MywidXNlcklkIjoiNTcyZDk0NTEtZTA3MC00NWIzLWE4MDQtNDIyM2VjOGIzNDExIiwicm9sZSI6IiJ9.d60hd_Mr84Qu2liFpLvwhqnImU-nhiK7Jow0lafYdXI"


INPUT_AEROPUERTOS = os.path.join(RUTA_BASE, 'aeropuertos_con_clima.csv')
OUTPUT_CLEAN = os.path.join(RUTA_BASE, 'aemet_filtrado.csv')

def generar_dato_sintetico():
    
    ahora_utc = datetime.now(timezone.utc)
    return [{
        'fint': ahora_utc.strftime('%Y-%m-%dT%H:00:00'),
        'vis': 10.0, 'vv': 5.0, 'ta': 15.0
    }]

def ejecutar_pipeline_aemet():
    print("INICIANDO BÚSQUEDA METEOROLÓGICA")

    ahora_utc = datetime.now(timezone.utc)
    
    inicio_ventana_utc = ahora_utc - timedelta(seconds=86400)

    fin_ventana_utc = ahora_utc + timedelta(minutes=10)

    print(f"Tiempo Actual (UTC): {ahora_utc.strftime('%Y-%m-%d %H:%M:%S')}Z")
    print(f"Ventana de Filtro:   {inicio_ventana_utc.strftime('%Y-%m-%d %H:%M:%S')}Z  --->  {fin_ventana_utc.strftime('%Y-%m-%d %H:%M:%S')}Z")
    print("   (Se descartará cualquier dato fuera de este intervalo exacto)")

    
    if not os.path.exists(INPUT_AEROPUERTOS):
        print(f"Error: No encuentro {INPUT_AEROPUERTOS}")
        return
    
    try:
        df_aero = pd.read_csv(INPUT_AEROPUERTOS, sep=';', encoding='utf-8')
    except:
        df_aero = pd.read_csv(INPUT_AEROPUERTOS, sep=';', encoding='latin-1')

    
    correcciones = {'LEBL': '0076', 'LEBB': '1024E', 'GCLP': 'C649I', 'GCTS': 'C449C', 'LEMD': '3195'}
    for icao, nuevo_id in correcciones.items():
        mask = df_aero['icao'] == icao
        if mask.any():
            df_aero.loc[mask, 'ID_AEMET'] = nuevo_id

    todos_datos = []
    print(f"Consultando {len(df_aero)} estaciones...")

    
    for index, row in df_aero.iterrows():
        id_estacion = str(row['ID_AEMET']).strip()
        nombre = str(row['aeropuerto_publico'])
        icao = str(row['icao']).strip()
        
        if not id_estacion or id_estacion == 'nan': continue

        
        url = f"https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/{id_estacion}"
        datos_raw = []
        
        for _ in range(3): 
            try:
                res = requests.get(url, params={"api_key": API_KEY}, headers={'cache-control': "no-cache"}, timeout=10)
                if res.status_code == 200:
                    json_res = res.json()
                    if 'datos' in json_res:
                        datos_raw = requests.get(json_res['datos'], timeout=10).json()
                        break
                elif res.status_code == 429: 
                    time.sleep(2)
            except: pass
            time.sleep(0.5)

        if not datos_raw:
            print(f"{icao}: Sin respuesta -> Generando sintético.")
            datos_raw = generar_dato_sintetico()
        else:
            print(f"{icao}: Datos recibidos ({len(datos_raw)} obs).")

        
        guardados_estacion = 0
        
        for lectura in datos_raw:
            fint = lectura.get('fint') 
            
            if fint:
                try:
                    
                    fint_clean = fint.split('.')[0].replace('Z', '').split('+')[0]
                    
                    dt_dato_utc = datetime.strptime(fint_clean, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
                    
                    if inicio_ventana_utc <= dt_dato_utc <= fin_ventana_utc:
                        
                        todos_datos.append({
                            'ID_ESTACION': id_estacion,
                            'AEROPUERTO': nombre,
                            'ICAO': icao,
                            'FECHA_DIA': dt_dato_utc.strftime('%Y-%m-%d'),
                            'FECHA_HORA': dt_dato_utc.strftime('%H:%M'),
                            'VISIBILIDAD_KM': float(lectura.get('vis', 10.0)),
                            'VIENTO_KMH': round(float(lectura.get('vv', 2.0)) * 3.6, 1),
                            'TEMPERATURA_C': float(lectura.get('ta', 15.0))
                        })
                        guardados_estacion += 1
                    else:
                        pass

                except Exception as e:
                    continue
        

        time.sleep(0.5)

    
    if todos_datos:
        df = pd.DataFrame(todos_datos)
        
        df = df.sort_values(by=['ICAO', 'FECHA_DIA', 'FECHA_HORA'])
        
        df.to_csv(OUTPUT_CLEAN, index=False, sep=';', encoding='utf-8-sig')
        print(f"\nGUARDADO: {OUTPUT_CLEAN}")
        print(f"Registros válidos (últimas 24h): {len(df)}")
        
        print(f"Fechas contenidas: {df['FECHA_DIA'].unique()}")
    else:
        print("\nNo hay datos válidos en la ventana de tiempo especificada.")

if __name__ == "__main__":
    ejecutar_pipeline_aemet()