

import pandas as pd
import requests
import time
import os
from datetime import datetime, timezone


RUTA_BASE = os.path.dirname(os.path.abspath(__file__))


CLIENT_ID = 'vikitube1234-api-client'
CLIENT_SECRET = 'TkKDPghpM4PCMOkTEkjMcYpxhlhqrqG4'


INPUT_AEROPUERTOS = os.path.join(RUTA_BASE, 'aeropuertos_con_clima.csv')
OUTPUT_CLEAN = os.path.join(RUTA_BASE, 'vuelos_filtrado.csv')

def obtener_token_opensky():
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    payload = {'grant_type': 'client_credentials', 'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET}
    try:
        response = requests.post(url, data=payload, timeout=10)
        return response.json().get('access_token') if response.status_code == 200 else None
    except: return None

def es_vuelo_nacional(icao_origen):
    """
    Filtra origen español (LE, GC, GE).
    """
    if not icao_origen: return False
    icao_origen = icao_origen.upper()
    return icao_origen.startswith(('LE', 'GC', 'GE'))

def ejecutar_pipeline_vuelos():
    print("INICIANDO OBTENCIÓN VUELOS")

    if not os.path.exists(INPUT_AEROPUERTOS):
        print(f"Error: No encuentro {INPUT_AEROPUERTOS}")
        return

    try:
        df_aero = pd.read_csv(INPUT_AEROPUERTOS, sep=';', encoding='utf-8')
    except:
        df_aero = pd.read_csv(INPUT_AEROPUERTOS, sep=';', encoding='latin-1')

    
    correcciones = {'LEBL': '0076', 'LEBB': '1024E', 'GCLP': 'C649I', 'GCTS': 'C449C'}
    for icao, nuevo_id in correcciones.items():
        df_aero.loc[df_aero['icao'] == icao, 'ID_AEMET'] = nuevo_id

    
    df_objetivo = df_aero[df_aero['ID_AEMET'].notna()]

    print(f"Analizando {len(df_objetivo)} aeropuertos...")

    token = obtener_token_opensky()
    if not token: return print("Error de Token.")

    lista_vuelos_limpios = []
    ahora = int(time.time())
    ayer = ahora - (24 * 3600)

    for index, row in df_objetivo.iterrows():
        nombre = row['aeropuerto_publico']
        icao_destino = str(row['icao']).strip()
        
        url = "https://opensky-network.org/api/flights/arrival"
        params = {'airport': icao_destino, 'begin': ayer, 'end': ahora}
        headers = {'Authorization': f'Bearer {token}'}
        
        vuelos_raw = []
        try:
            res = requests.get(url, headers=headers, params=params, timeout=15)
            if res.status_code == 200:
                vuelos_raw = res.json()
            elif res.status_code == 429:
                print(f"Límite en {icao_destino}. Esperando...")
                time.sleep(5)
        except Exception: pass

        count_nacionales = 0
        if vuelos_raw:
            for v in vuelos_raw:
                origen = v.get('estDepartureAirport')
                
                
                if es_vuelo_nacional(origen):
                    ts_llegada = v.get('lastSeen')
                    
                    
                    dt_obj = datetime.fromtimestamp(ts_llegada, tz=timezone.utc)
                    
                    lista_vuelos_limpios.append({
                        'AEROPUERTO': nombre,
                        'ICAO': icao_destino,
                        'VUELO': v.get('callsign', 'S/N').strip(),
                        'ORIGEN': origen,
                        'FECHA_DIA': dt_obj.strftime('%Y-%m-%d'),
                        'FECHA_HORA': dt_obj.strftime('%H:%M')
                        
                    })
                    count_nacionales += 1
            
            print(f"{icao_destino}: {count_nacionales} vuelos nacionales guardados.")
        else:
            print(f"{icao_destino}: 0 vuelos.")

        time.sleep(1.0)

    
    if lista_vuelos_limpios:
        df_vuelos = pd.DataFrame(lista_vuelos_limpios)
        df_vuelos.to_csv(OUTPUT_CLEAN, index=False, sep=';', encoding='utf-8-sig')
        print(f"'vuelos_filtrado.csv' guardado con {len(df_vuelos)} registros.")
    else:
        print("No se encontraron vuelos.")

if __name__ == "__main__":
    ejecutar_pipeline_vuelos()