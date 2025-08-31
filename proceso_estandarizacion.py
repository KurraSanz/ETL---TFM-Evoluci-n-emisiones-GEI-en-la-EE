'''
Script de estandarización
-----------------------------------------------------------------------

Este script forma parte del proceso ETL del Trabajo de Fin de Máster (TFM) y tiene como objetivo consolidar y estructurar la información curada proveniente de las fuentes EEA y Eurostat. 
Realiza las siguientes tareas principales:

    **Estandarización de datos:**
   - Homogeneiza el dataset de EEA (`UNFCCC_v28_3.csv`) para adecuarlo al formato de Eurostat, asegurando consistencia en nombres de columnas, valores categóricos y codificación de entidades geográficas.
   - Mueve todos los archivos curados de Eurostat a una carpeta común de ficheros estandarizados.

'''

import pandas as pd
import glob
import os
import shutil
from tqdm import tqdm


# Paths en local

CARPETA_CURADO_EUROSTAT = os.path.join("ficheros_curado", "eurostat")
CARPETA_CURADO_EEA = os.path.join("ficheros_curado", "eea")
CARPETA_ESTANDARIZADOS = "ficheros_estandarizados"


# Crear carpetas si no existen
for carpeta in [CARPETA_ESTANDARIZADOS]:
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)



# 1) TRANSFORMACIÓN DE LAS TABLAS DE EEA A LA ESTRUCTURA EUROSTAT (ESTANDARIZACIÓN)
archivos_curados_eea = glob.glob(os.path.join(CARPETA_CURADO_EEA, "*.csv"))
for archivo in tqdm(archivos_curados_eea, desc="Transformando archivos curados"):

    df = pd.read_csv(archivo, low_memory=False)
    
    # Homogeneizamos los ficheros
    if os.path.basename(archivo) == "UNFCCC_v28_3.csv":
        df.rename(columns={
        'Country_code': 'geo',
        'Country': 'Geopolitical entity (reporting)',
        'emissions': 'OBS_VALUE',
        'Year': 'TIME_PERIOD',
        'Pollutant_name': 'airpol'
        }, inplace=True)
        df['geo'] = df['geo'].replace('EUA', 'EU27_2020')
        df['Geopolitical entity (reporting)'] = df['Geopolitical entity (reporting)'].replace('EU-27', 'European Union - 27 countries (from 2020)')
        df['airpol'] = df['airpol'].replace('All greenhouse gases - (CO2 equivalent)','GHG')
        df = df[df['airpol'] == 'GHG']

    # Guardar CSV en carpeta intermedia
    nombre = os.path.basename(archivo)
    ruta_curado_int = os.path.join(CARPETA_ESTANDARIZADOS, nombre)
    df.to_csv(ruta_curado_int, index=False)

#2) Mover los ficheros de CARPETA_CURADO_EUROSTAT a CARPETA_ESTANDARIZADOS

archivos_curados_eurostat = glob.glob(os.path.join(CARPETA_CURADO_EUROSTAT, "*.csv"))
for archivo in tqdm(archivos_curados_eurostat, desc="Trasladando archivos curados"):

    df = pd.read_csv(archivo, low_memory=False)
    nombre = os.path.basename(archivo)
    ruta_destino = os.path.join(CARPETA_ESTANDARIZADOS, nombre)
    shutil.copy2(archivo, ruta_destino)
    print(f'Movido {nombre}')


print("Proceso finalizado.")