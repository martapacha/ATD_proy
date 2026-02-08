
import pandas as pd
import os
from datetime import datetime


RUTA_BASE = os.path.dirname(os.path.abspath(__file__))


FILE_VUELOS = os.path.join(RUTA_BASE, 'vuelos_filtrado.csv')
FILE_AEMET = os.path.join(RUTA_BASE, 'aemet_filtrado.csv')
FILE_ALTITUD = os.path.join(RUTA_BASE, 'altitudes_clasificadas.csv')

FILE_OUTPUT = os.path.join(RUTA_BASE, 'dataset_final_completo.csv')

def cargar_csvs():
    print("Cargando archivos CSV")
    
    if not os.path.exists(FILE_VUELOS) or not os.path.exists(FILE_AEMET):
        print("Error")
        return None, None, None

    def leer_seguro(path):
        try:
            return pd.read_csv(path, sep=';', encoding='utf-8')
        except:
            return pd.read_csv(path, sep=';', encoding='latin-1')

    df_v = leer_seguro(FILE_VUELOS)
    df_c = leer_seguro(FILE_AEMET)
    
    if os.path.exists(FILE_ALTITUD):
        df_a = leer_seguro(FILE_ALTITUD)
    else:
        print("No existe archivo de altitud.")
        df_a = pd.DataFrame(columns=['ICAO', 'ALTITUD_PIES', 'CATEGORIA_ALTITUD'])

    return df_v, df_c, df_a

def preparar_fechas(df, col_dia, col_hora):
    fechas_str = df[col_dia].astype(str) + ' ' + df[col_hora].astype(str)
    return pd.to_datetime(fechas_str, format='%Y-%m-%d %H:%M', errors='coerce')

def ejecutar_cruce_local():
    print("Unificando los datos...")

    df_vuelos, df_aemet, df_altitud = cargar_csvs()
    if df_vuelos is None: return

    dict_alt = {}
    for _, row in df_altitud.iterrows():
        dict_alt[str(row['ICAO']).strip()] = {
            'alt': row['ALTITUD_PIES'], 
            'cat': row['CATEGORIA_ALTITUD']
        }

    print("Procesando fechas...")
    df_vuelos['DT_OBJ'] = preparar_fechas(df_vuelos, 'FECHA_DIA', 'FECHA_HORA')
    df_aemet['DT_OBJ'] = preparar_fechas(df_aemet, 'FECHA_DIA', 'FECHA_HORA')

    lista_final = []
    aeropuertos = df_vuelos['ICAO'].unique()

    print(f"Cruzando datos de {len(aeropuertos)} aeropuertos...")

    for icao in aeropuertos:
        vuelos_local = df_vuelos[df_vuelos['ICAO'] == icao]
        clima_local = df_aemet[df_aemet['ICAO'] == icao]
        
        geo = dict_alt.get(icao, {'alt': 0, 'cat': 'Desconocido'})

        if clima_local.empty:
            print(f"  {icao}: No hay datos de clima para cruzar.")
            continue

        for _, vuelo in vuelos_local.iterrows():
            t_vuelo = vuelo['DT_OBJ']
            if pd.isna(t_vuelo): continue

          
            clima_local['diff_minutos'] = (clima_local['DT_OBJ'] - t_vuelo).abs().dt.total_seconds() / 60
            
            idx_mejor = clima_local['diff_minutos'].idxmin()
            mejor_clima = clima_local.loc[idx_mejor]
            
          
            lista_final.append({
                'AEROPUERTO': vuelo['AEROPUERTO'],
                'ICAO': vuelo['ICAO'],
                'VUELO': vuelo['VUELO'],
                'ORIGEN': vuelo['ORIGEN'],
                'FECHA_LLEGADA': vuelo['FECHA_DIA'],
                'HORA_LLEGADA': vuelo['FECHA_HORA'],
              
                'HORA_METEO': mejor_clima['FECHA_HORA'],
                'DIFERENCIA_MIN': round(mejor_clima['diff_minutos'], 1),
                'VISIBILIDAD_KM': mejor_clima['VISIBILIDAD_KM'],
                'VIENTO_KMH': mejor_clima['VIENTO_KMH'],
                'TEMPERATURA_C': mejor_clima['TEMPERATURA_C'],
              
                'ALTITUD_FT': geo['alt'],
                'CATEGORIA_OROGRAFICA': geo['cat']
            })


    if lista_final:
        df_final = pd.DataFrame(lista_final)
        df_final.to_csv(FILE_OUTPUT, index=False, sep=';', encoding='utf-8-sig')
        print(f"\n Fin")
        print(f" Archivo creado: {FILE_OUTPUT}")
        print(f" Vuelos totales: {len(df_final)}")

    else:
        print("\n No se han podido cruzar datos.")

if __name__ == "__main__":
    ejecutar_cruce_local()