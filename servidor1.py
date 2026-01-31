# -*- coding: utf-8 -*-

import socket
import json
import pandas as pd

def analizar_retrasos_y_altitud(df):
    df['RIESGO_RETRASO'] = 'BAJO'

    df.loc[(df['VISIBILIDAD_KM'] < 5) | (df['VIENTO_KMH'] > 30), 'RIESGO_RETRASO'] = 'MEDIO'
    df.loc[(df['VISIBILIDAD_KM'] < 2) | (df['VIENTO_KMH'] > 45), 'RIESGO_RETRASO'] = 'ALTO'

    print("\n--- ESTADÍSTICAS DE RIESGO POR UBICACIÓN ---")
    resumen = df.groupby('CATEGORIA_OROGRAFICA')['RIESGO_RETRASO'].value_counts(normalize=True).unstack() * 100
    print(resumen)
    
    return df

def iniciar_servidor():
    HOST = '127.0.0.1'
    PORT = 65432
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"Servidor esperando en el puerto {PORT}")
        
        conn, addr = s.accept()
        with conn:
            payload = b""
            while True:
                packet = conn.recv(1048576)
                if not packet: break
                payload += packet

            datos = json.loads(payload.decode('utf-8'))
            df = pd.DataFrame(datos)
            df_final = analizar_retrasos_y_altitud(df)
            df_final.to_csv("INFORME_FINAL_RIESGO_ALTITUD.csv", index=False, sep=";")
            print("\n Se ha generado 'INFORME_FINAL_RIESGO_ALTITUD.csv' con todo el análisis.")

if __name__ == "__main__":
    iniciar_servidor() 