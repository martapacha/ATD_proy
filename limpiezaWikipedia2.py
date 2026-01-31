# -*- coding: utf-8 -*-
"""
Created on Fri Jan 30 02:30:51 2026

@author: Usuario
"""

import pandas as pd


df = pd.read_csv("aeropuertos.csv", sep=";")


mapeo_aemet = {
    "LEMD": "2462",   
    "LEBL": "0076B",  
    "LEMG": "6155A",  
    "LEPA": "1393",
    "LEAL": "8025",   
    "LEVC": "8414A",  
    "LEZL": "5783",   
    "LEBB": "1024E",  
    "GCLP": "9270X",  
    "GCTS": "9390X"   
}


df['ID_AEMET'] = df['icao'].map(mapeo_aemet)


df.to_csv("aeropuertos_con_clima.csv", index=False, sep=";", encoding='utf-8-sig')

print("Se ha creado 'aeropuertos_con_clima.csv'")