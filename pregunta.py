"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""

import pandas as pd
import re


def ingest_data():

    #
    # Inserte su código aquí
    #

    # Leer el archivo
    df = pd.read_fwf(
        "clusters_report.txt",
        colspecs="infer",
        widths=[9, 16, 16, 80],
        header=None,
        names=[
            "cluster",
            "cantidad_de_palabras_clave",
            "porcentaje_de_palabras_clave",
            "principales_palabras_clave",
        ],
        converters={
            "porcentaje_de_palabras_clave": lambda x: x.rstrip(" %").replace(",", ".")
        },
    )

    # Eliminar las primeras tres filas
    df = df.drop(index={0, 1, 2}).ffill()

    # Convertir 'cluster' a tipo numérico
    df["cluster"] = pd.to_numeric(df["cluster"])

    # Función para eliminar comas duplicadas en las palabras clave
    def remove_duplicate_commas(text):
        return re.sub(r",+", ",", text)

    def remove_spaces(text):
        return re.sub(r"\s+", " ", text)

    def remove_dot(text):
        return re.sub(r"\s*\.$", "", text)

    # Agrupar por 'cluster' y combinar los valores de otras columnas en listas
    df_grouped = (
        df.groupby("cluster")
        .agg(
            {
                "cantidad_de_palabras_clave": "first",
                "porcentaje_de_palabras_clave": "first",
                "principales_palabras_clave": lambda x: "\n".join(x),
            }
        )
        .reset_index()
    )

    # Convertir 'porcentaje_de_palabras_clave' a tipo numérico
    df_grouped["porcentaje_de_palabras_clave"] = pd.to_numeric(
        df_grouped["porcentaje_de_palabras_clave"]
    )
    df_grouped["cluster"] = df_grouped["cluster"].astype(int)
    df_grouped["cantidad_de_palabras_clave"] = df_grouped[
        "cantidad_de_palabras_clave"
    ].astype(int)
    df_grouped["porcentaje_de_palabras_clave"] = df_grouped[
        "porcentaje_de_palabras_clave"
    ].astype(float)
    # Mostrar el DataFrame resultante

    # Aplicar la función a la columna 'principales_palabras_clave'
    df_grouped["principales_palabras_clave"] = (
        df_grouped["principales_palabras_clave"]
        .apply(remove_duplicate_commas)
        .apply(remove_spaces)
        .apply(remove_dot)
    )

    return df_grouped
