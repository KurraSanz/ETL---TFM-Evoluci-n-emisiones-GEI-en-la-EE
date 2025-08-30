'''
Script de curación de datos Eurostat

Descripción:
Este script forma parte del pipeline ETL del TFM y tiene como objetivo limpiar y estandarizar 
los archivos .csv provenientes de la EUROSTAT, previamente descargados y almacenados en la carpeta 
`ficheros_raw/eurostat`.

Acciones que realiza el script:
Acciones principales del script:
1. Lee todos los archivos CSV ubicados en la carpeta de entrada `ficheros_raw/eurostat`.
2. Elimina filas completamente vacías (`dropna(how='all')`).
3. Elimina filas con valores nulos en columnas clave.
4. Filtra los registros para quedarse solo con aquellos cuya frecuencia sea anual (`freq == 'A'`).
4. Realiza la conversión de tipos:
   - `obs_value` se convierte a `float64`.
   - `TIME_PERIOD` se convierte a `int64`.
   - La conversión utiliza coerción para evitar errores y convertir valores inválidos en NaN.
5. Eliminamos duplicados.
6. Guarda los archivos curados en la carpeta `ficheros_curado/eurostat` con el mismo nombre original.

'''

import pandas as pd
import glob
import os
from tqdm import tqdm

# Paths
CARPETA_RAW = os.path.join("ficheros_raw", "eurostat")
CARPETA_CURADO = os.path.join("ficheros_curado", "eurostat")

# Crear carpeta de salida si no existe

root_folder = "ficheros_curado"
secundary_folder = "eurostat"
folder_path = os.path.join(root_folder, secundary_folder)
os.makedirs(folder_path, exist_ok=True)
#os.makedirs(CARPETA_CURADO, exist_ok=True)

archivos_raw = glob.glob(os.path.join(CARPETA_RAW, "*.csv"))

for archivo in tqdm(archivos_raw, desc="Limpiando y guardando archivos raw"):
    try:
        df = pd.read_csv(archivo, low_memory=False)
        
        # 1) Eliminar filas completamente vacías
        df = df.dropna(how='all')

        # 2) Eliminar filas con valores nulos en columnas clave
        if 'TIME_PERIOD' in df.columns:
            df = df[df['TIME_PERIOD'].notna()]

        # 3) Quedarnos sólo con filas con frecuencia Anual
        df = df[df['freq'] == 'A']
        
        # 4) Convertir 'obs_value' a float64 y TIME_PERIOD a int64. Coercion convierte errores a NaN
        if 'obs_value' in df.columns:
            df['obs_value'] = pd.to_numeric(df['obs_value'], errors='coerce').astype('float64')
        if 'TIME_PERIOD' in df.columns:
            df['TIME_PERIOD'] = pd.to_numeric(df['TIME_PERIOD'], errors='coerce').astype('int64')
        
        # 5) Eliminar registros duplicados
        df = df.drop_duplicates()

        # Guardar CSV curado
        nombre = os.path.basename(archivo)
        ruta_curado = os.path.join(CARPETA_CURADO, nombre)
        df.to_csv(ruta_curado, index=False)
    except Exception as e:
        print(f"Error limpiando {archivo}: {e}")
