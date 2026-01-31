# -*- coding: utf-8 -*-
"""
Created on Sat Jan 31 19:43:26 2026

@author: Usuario
"""

import socket
import json
import pandas as pd
import subprocess  
import os

def ejecutar_todo_el_proceso():
    
    scripts = [
        "wikipedia.py", 
        "limpiezaWikipedia.py", 
        "limpiezaWikipedia2.py", 
        "aemet.py", 
        "openSky.py", 
        "metar-taf.py", 
        "cruzar.py"
    ]
    
    print("INICIANDO PROCESO AUTOMÁTICO...")
    
    for script in scripts:
        if os.path.exists(script):
            print(f"Ejecutando: {script}...")
            
            subprocess.run(["python", script], check=True)
        else:
            print(f"No se encuentra el archivo {script}")

    print("Todos los CSV han sido creados y cruzados.")

def enviar_datos_finales():
    HOST = '127.0.0.1'
    PORT = 65432
    
    
    archivo_final = "dataset_final_completo.csv"
    
    if os.path.exists(archivo_final):
        df = pd.read_csv(archivo_final, sep=";")
        
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            payload = df.to_json(orient='records').encode('utf-8')
            s.sendall(payload)
            print("Datos enviados al Servidor con éxito.")

if __name__ == "__main__":
    
    ejecutar_todo_el_proceso()
    
    enviar_datos_finales()