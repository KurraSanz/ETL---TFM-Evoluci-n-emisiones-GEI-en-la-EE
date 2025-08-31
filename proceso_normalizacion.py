'''
Script de normalización y creación de tablas de hechos y dimensiones
-----------------------------------------------------------------------

Este script forma parte del proceso ETL del Trabajo de Fin de Máster (TFM) y tiene como objetivo consolidar y estructurar la información curada proveniente de las fuentes EEA y Eurostat. 
Realiza las siguientes tareas principales:


1. **Creación de tablas de hechos y dimensiones:**
   - Genera tablas de hechos (`fact_*.csv`) con las columnas relevantes según una estructura predefinida.
   - Extrae las distintas dimensiones clave (como tipo de emisiones, sectores económicos, localización geográfica, etc.) desde los datasets estandarizados.
   - Elimina duplicados y guarda las dimensiones como archivos separados (`dim_*.csv`).

2. **Organización en capas del modelo de datos:**
   - Las tablas generadas se almacenan en las carpetas `ficheros_fact` y `ficheros_dim`, según su rol en el modelo estrella de explotación analítica.


'''

import pandas as pd
import glob
import os
import shutil
from tqdm import tqdm


# Paths en local

CARPETA_ESTANDARIZADOS = "ficheros_estandarizados"
CARPETA_FACT = "ficheros_fact"
CARPETA_DIM = "ficheros_dim"


# Crear carpetas si no existen
for carpeta in [CARPETA_FACT, CARPETA_DIM]:
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)

# Definición de columnas de las tablas de hechos
columnas_hechos = ['STRUCTURE_NAME', 'freq', 'airpol', 'nace_r2', 
                   'Unit of measure', 'geo',
                   'Source sectors for greenhouse gas emissions (Common reporting format, UNFCCC)',
                    'Statistical information',
                    'Environment indicator',
                    'National accounts indicator (ESA 2010)',
                    'reporter',
                    'PARTNER',
                    'FLOW',
                    'indicators',
                    'TIME_PERIOD', 'OBS_VALUE', 'OBS_FLAG',
                    'Format_name', 'Sector_code',
                    'Sector_name','Notation',
                    'PublicationData',
                    'DataSource']

# Mapeo de dimensiones: clave → [columna_codigo, columna_descripcion]
dim_cols = {
    'airpol': ['airpol', 'Air pollutants and greenhouse gases'],
    'nace_r2': ['nace_r2', 'Statistical classification of economic activities in the European Community (NACE Rev. 2)'],
    'geo': ['geo', 'Geopolitical entity (reporting)'],
    'time_period': ['TIME_PERIOD', 'Time'],
    'ipcc': ['Sector_code','Sector_name']
}

# Diccionario personalizado de nombres para tablas de hechos
nombres_fact = {
    "env_ac_ainah_r2": "aea",
    "env_ac_aeint_r2": "aei",
    "env_ac_aibrid_r2": "aea_brid",
    "sdg_13_10": "ghe",
    "nrg_ind_eff": "eff",
    "sdg_07_10": "energy_cons",
    "nrg_ind_fecf": "share_energy_cons",
    "nrg_ind_ren": "share_ren",
    "sdg_13_40": "losses",
    "ds-059331$defaultview": "import_export",
    "nama_10_gdp": "gdp",
    "UNFCCC_V28_3": "ghg_unfccc"
}

# CREACIÓN TABLAS DE DIMENSIONES Y HECHOS
# Diccionario para ir guardando todas las tablas de dimensiones que vayamos extrayendo
dimensiones_acumuladas = {dim: [] for dim in dim_cols.keys()}

# Procesar cada CSV curado para separar hechos y dimensiones
archivos_curados = glob.glob(os.path.join(CARPETA_ESTANDARIZADOS, "*.csv"))

for archivo in tqdm(archivos_curados, desc="Procesando archivos curados"):
    df = pd.read_csv(archivo, low_memory=False)
    nombre_base = os.path.splitext(os.path.basename(archivo))[0]
    # 1)--- Hechos ---
    columnas_hechos_existentes = [c for c in columnas_hechos if c in df.columns]
    df_hechos = df[columnas_hechos_existentes].copy()
    nombre_fact = nombres_fact.get(nombre_base, f"fact_{nombre_base}")
    ruta_fact = os.path.join(CARPETA_FACT, f"fact_{nombre_fact}.csv")
    df_hechos.to_csv(ruta_fact, index=False)


    # 2)--- Dimensiones ---
    for dim, cols in dim_cols.items():
        cols_existentes = [c for c in cols if c in df.columns]
        if cols_existentes:
            df_dim = df[cols_existentes].drop_duplicates().reset_index(drop=True)
            dimensiones_acumuladas[dim].append(df_dim)


# Concatenar dimensiones de todos los archivos, eliminar duplicados y guardar
for dim, lista_df in dimensiones_acumuladas.items():
    if lista_df:
        df_dim_concat = pd.concat(lista_df, ignore_index=True).drop_duplicates().reset_index(drop=True)
        ruta_dim = os.path.join(CARPETA_DIM, f"dim_{dim}.csv")
        df_dim_concat.to_csv(ruta_dim, index=False)
        print(f"Dimensión '{dim}' creada con {len(df_dim_concat)} registros en {ruta_dim}")
    else:
        print(f"No se encontró información para la dimensión '{dim}' en los archivos.")



print("Proceso finalizado.")