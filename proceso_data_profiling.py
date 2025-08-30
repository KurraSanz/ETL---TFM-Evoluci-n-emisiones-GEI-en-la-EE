"""
Script para análisis exploratorio y data profiling de datasets CSV

Este script procesa múltiples archivos CSV almacenados en carpetas locales especificadas,
generando informes detallados de profiling con la librería ydata_profiling, junto con resúmenes
estadísticos y gráficos para cada variable.

Funcionalidades principales:
- Lectura automática de todos los archivos CSV en las carpetas configuradas.
- Generación de informes HTML de profiling (tipos de variables, valores nulos, distribución, etc).
- Creación de resúmenes en Excel con estadísticas clave por columna:
  tipos, valores únicos, valores nulos, completitud, moda y frecuencia, métricas estadísticas para numéricas (media, mediana, desviación, máximo, mínimo).
- Producción de gráficos individuales para cada variable:
  histogramas para variables numéricas y gráficos de barras para variables categóricas.
- Organización automática de los informes, resúmenes y gráficos en carpetas específicas para facilitar su análisis.
- Ideal para la fase inicial de análisis y validación de datos en procesos ETL y proyectos de análisis de datos.

Este script forma parte del pipeline ETL para el TFM del máster y facilita la identificación
de problemas de calidad, patrones y características relevantes en los datos antes de su transformación y análisis.

"""

import pandas as pd
from ydata_profiling import ProfileReport
import matplotlib.pyplot as plt
import os

#Carpetas de entrada de csv 
carpetas_fuente = ["ficheros_raw/eurostat", "ficheros_raw/eea"]

for input_folder in carpetas_fuente:
    csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]
    print(f"Procesando carpeta: {input_folder} - Archivos encontrados: {csv_files}")

    for file in csv_files:
        input_file = os.path.join(input_folder, file)
        base_name = os.path.splitext(os.path.basename(input_file))[0]

        # ⬇️ Aquí empieza tu código actual para procesar cada archivo...

    #Proceso de ejecución
    for file in csv_files:

        input_file = os.path.join(input_folder,file)
        base_name=os.path.splitext(os.path.basename(input_file))[0]

        print(base_name)

        df = pd.read_csv(input_file)



        profile = ProfileReport(
            df,
            title="Informe de Data Profiling",
            explorative=False,
            minimal=True
        
        )

        # Carpeta de salida (creará una si no existe)
        output_folder = "data_profilling"
        os.makedirs(output_folder, exist_ok=True)
        out_file=f"informe_data_profiling_{base_name}.html"
        out_path=os.path.join(output_folder, out_file)

        profile.to_file(out_path)

        print("✅ Informe HTML generado.")

        # Crear resumen en CSV
        resumen = pd.DataFrame({
            "Tipo de dato": df.dtypes.astype(str),
            "Valores únicos": df.nunique(),
            "Nulos": df.isnull().sum(),
            "Completitud (%)": (1 - df.isnull().sum() / len(df)) * 100,
            "Número de registros": len(df),
            "Número de variables": len(df.columns)
        })


    ## Guardar como CSV

    # Total de registros (filas)
        total_registros = len(df)

    # Crear resumen por columna
        resumen_columnas = []

        for col in df.columns:
            tipo_dato = str(df[col].dtype)
            nulos = df[col].isnull().sum()
            completitud = round(100 * (1 - nulos / len(df[col])), 2)
            valores_unicos = df[col].nunique()
            registros = len(df[col])

        # Obtener valor más frecuente y su frecuencia
            if not df[col].dropna().empty:
                valor_mas_frec = df[col].mode().iloc[0]
                frec_mas_frec = df[col].value_counts().iloc[0]
            else:
                valor_mas_frec = None
                frec_mas_frec = 0

    # Inicializar las métricas estadísticas
            valor_maximo = None
            valor_minimo = None
            media = None
            mediana = None
            desviacion = None
            ceros = None

        # Si la columna es numérica, calcular las métricas estadísticas
            if pd.api.types.is_numeric_dtype(df[col]):
                valor_maximo = df[col].max()
                valor_minimo = df[col].min()
                media = df[col].mean()
                mediana = df[col].median()
                desviacion = df[col].std()
                ceros = (df[col] == 0).sum()

        # Añadir los resultados al resumen
            resumen_columnas.append({
                "Columna": col,
                "Tipo de dato": tipo_dato,
                "Valores únicos": valores_unicos,
                "Nulos": nulos,
                "Ceros": ceros,
                "Completitud (%)": completitud,
                "Valor más frecuente": valor_mas_frec,
                "Frecuencia del más frecuente (%)": frec_mas_frec/registros,
                "Valor máximo": valor_maximo,
                "Valor mínimo": valor_minimo,
                "Media": media,
                "Mediana": mediana,
                "Desviacion": desviacion
            })

    # Convertir a DataFrame y guardar como xls
        resumen_df = pd.DataFrame(resumen_columnas)
    #resumen_df.to_csv("analisis_por_columna.csv", index=False)

    # Carpeta de salida (creará una si no existe)
        output_folder2 = "data_resumen"
        os.makedirs(output_folder2, exist_ok=True)
        out_xls=f"resumen_data_profiling_{base_name}.xlsx"


        with pd.ExcelWriter(os.path.join(output_folder2, out_xls), engine="openpyxl") as writer:
                resumen.to_excel(writer, sheet_name="Resumen general",index=False)
                resumen_df.to_excel(writer, sheet_name="Análisis por columnas", index=False)



    ## Carpeta de salida (creará una si no existe)
    #output_folder = "eurostat_data_profilling"
    #os.makedirs(output_folder, exist_ok=True)

    ## Guardar como CSV

    #resumen.to_csv("resumen_data_profiling.csv", index_label="Columna")

        print("✅ Resumen CSV generados correctamente.")


    # Crear carpeta para los gráficos

        os.makedirs(f"graficos_{base_name}", exist_ok=True)

        for col in df.columns:
            plt.figure(figsize=(8, 5))

            if pd.api.types.is_numeric_dtype(df[col]):
            # Histograma para variables numéricas
                df[col].dropna().hist(bins=29, color='skyblue', edgecolor='black')
                plt.title(f"Histograma: {col}")
                plt.xlabel(col)
                plt.ylabel("Frecuencia")
                plt.tight_layout()
                plt.savefig(f"graficos_{base_name}/histograma_{col}.png")

        
            elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_categorical_dtype(df[col]):
            # Diagrama de barras para categóricas
                conteos = df[col].value_counts().head(5)  # Top 5 categorías
                conteos.plot(kind="bar", color='salmon', edgecolor='black')
                plt.title(f"Frecuencia: {col} (Top 5)")
                plt.xlabel(col)
                plt.ylabel("Frecuencia")
                plt.xticks(rotation=30, ha='right')
                plt.tight_layout()
                plt.savefig(f"graficos_{base_name}/frecuencia_{col}.png")

            plt.close()

        print("✅ Gráficos generados correctamente.")