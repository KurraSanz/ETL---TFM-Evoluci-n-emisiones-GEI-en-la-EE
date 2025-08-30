import requests
import os
import gzip
import shutil
from urllib.parse import urlparse

# Lista de URLs para descargar múltiples datasets. Ver excel "dataset"
dataset_urls = [
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/env_ac_ainah_r2/1.0?compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/sdg_13_10/1.0?compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/env_ac_aeint_r2/1.0?compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/nrg_ind_fecf/1.0?compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/sdg_07_10/1.0?compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/nrg_ind_ren/1.0?compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/sdg_13_40/1.0?compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name",
    "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/3.0/data/dataflow/ESTAT/ds-059331$defaultview/1.0?compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/nama_10_gdp/1.0?compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name",
    "https://www.eea.europa.eu/en/datahub/datahubitem-view/6f1efaf1-ae32-48cb-b962-0891f84b1f5f",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/nrg_ind_eff/1.0?compress=true&format=csvdata&formatVersion=2.0&lang=en&labels=name"
    
]

# Función para descargar y descomprimir el archivo GZ de un dataset
def download_and_extract(dataset_url, output_folder):
    # Usamos una sesión de requests para manejar cookies y redirecciones
    session = requests.Session()

    # Agregar un encabezado User-Agent para simular una solicitud desde un navegador
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    }

    # Realizamos la solicitud GET con la sesión y los encabezados
    response = session.get(dataset_url, headers=headers, allow_redirects=True)

    # Verificar si la respuesta es correcta
    if response.status_code == 200:
        # Obtener el nombre del archivo a partir de la URL
        parsed_url = urlparse(dataset_url)
        dataset_name = parsed_url.path.split('/')[-2]  # Extraemos el nombre del dataset de la URL

        # Crear la carpeta de salida si no existe
        os.makedirs(output_folder, exist_ok=True)

        # Nombre de archivo GZ y CSV utilizando el nombre del dataset
        gz_file_path = os.path.join(output_folder, f"{dataset_name}.csv.gz")
        csv_file_path = os.path.join(output_folder, f"{dataset_name}.csv")

        # Imprimir los encabezados completos de la respuesta para diagnóstico
        print(f"📦 Encabezados de la respuesta para {dataset_name}:")
        for key, value in response.headers.items():
            print(f"{key}: {value}")

        # Verificar si el contenido es un archivo GZ
        content_type = response.headers.get('Content-Type', '')
        print(f"🔍 Tipo de contenido recibido: {content_type}")

        if 'gzip' in content_type or 'csv' in content_type:
            # Guardar el archivo GZ
            with open(gz_file_path, "wb") as f:
                f.write(response.content)
            print(f"✅ Archivo GZ descargado y guardado en: {gz_file_path}")

            # Descomprimir el archivo GZ
            try:
                # Descomprimir el archivo GZ a un archivo CSV
                with gzip.open(gz_file_path, 'rb') as f_in:
                    with open(csv_file_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                print(f"✅ Archivo descomprimido y guardado como: {csv_file_path}")
            except Exception as e:
                print(f"⚠️ Error al descomprimir el archivo GZ: {e}")
        else:
            print("⚠️ El archivo descargado no es GZ válido. El contenido recibido no tiene el tipo esperado.")
            print("🔍 Contenido de la respuesta (primeros 500 caracteres):")
            print(response.text[:500])  # Mostrar los primeros 500 caracteres de la respuesta
    else:
        print(f"❌ Error al descargar los datos de {dataset_name}. Código de estado: {response.status_code}")
        print(f"🔍 Detalles completos del error: {response.text[:500]}")  # Mostrar los primeros 500 caracteres del mensaje de error


# Carpeta de salida
output_folder = "datos_eurostat"

# Descargar y descomprimir cada dataset
for dataset_url in dataset_urls:
    download_and_extract(dataset_url, output_folder)
