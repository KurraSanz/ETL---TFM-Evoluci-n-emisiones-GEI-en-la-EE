'''
Script de curación de datos EEA (Agencia Europea de Medio Ambiente)

Descripción:
Este script forma parte del pipeline ETL del TFM y tiene como objetivo limpiar y estandarizar 
los archivos .csv provenientes de la EEA, previamente descargados y almacenados en la carpeta 
`ficheros_raw/eea`.

Acciones que realiza el script:
1. Lee todos los archivos .csv en la carpeta de entrada (`ficheros_raw/eea`).
2. Aplica reglas específicas de limpieza:
   - Elimina filas completamente vacías.
   - Filtra filas con valores nulos en columnas clave como `year` o `Year`.
   - En el archivo especial `ETS_Database_v51_May23.csv`, elimina filas con el valor "Total" en la columna `year`.
3. Realiza la conversión de tipos:
   - Convierte columnas como `emissions`, `value`, `year` y `Year` a formatos numéricos (`float64`, `int64`).
4. Eliminar registros duplicados.
5. Guarda los archivos curados en la carpeta `ficheros_curado/eea`, manteniendo el nombre original.

'''


import pandas as pd
import glob
import os
from tqdm import tqdm


# Paths
CARPETA_RAW = os.path.join("ficheros_raw", "eea")
CARPETA_CURADO = os.path.join("ficheros_curado", "eea")

# Crear carpeta de salida si no existe

root_folder = "ficheros_curado"
secundary_folder = "eea"
folder_path = os.path.join(root_folder, secundary_folder)
os.makedirs(folder_path, exist_ok=True)


archivos_raw = glob.glob(os.path.join(CARPETA_RAW, "*.csv"))

for archivo in tqdm(archivos_raw, desc="Limpiando y guardando archivos raw"):
    try:
        if os.path.basename(archivo) == "ETS_Database_v51_May23.csv":
            df = pd.read_csv(archivo, sep='\t', low_memory=False)
            df = df[~df['year'].str.contains('Total', na=False)]
        else:
            df = pd.read_csv(archivo, low_memory=False)
        
        
        # 1) Eliminar filas completamente vacías o aquellas PK con valores null
        df = df.dropna(how='all')
        if 'Year' in df.columns:
            df = df[df['Year'].notna()]  # eliminar filas con null en 'Year'
        if 'year' in df.columns:
            df = df[df['year'].notna()]  # eliminar filas con null en 'year'
        # 2) Convertir 'emissions' a float64 y Year a int64. Coercion convierte errores a NaN
        if 'emissions' in df.columns:
            df['emissions'] = pd.to_numeric(df['emissions'], errors='coerce').astype('float64')
        if 'Year' in df.columns:
            df['Year'] = pd.to_numeric(df['Year'], errors='coerce').astype('int64')
        
        if 'value' in df.columns:
            df['value'] = pd.to_numeric(df['value'], errors='coerce').astype('float64')
        if 'year' in df.columns:
            df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('int64')

        # 3) Eliminar registros duplicados
        df = df.drop_duplicates()   
        # Guardar CSV curado
        nombre = os.path.basename(archivo)
        ruta_curado = os.path.join(CARPETA_CURADO, nombre)
        df.to_csv(ruta_curado, index=False)
    except Exception as e:
        print(f"Error limpiando {archivo}: {e}")
