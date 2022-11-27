"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    df = pd.read_fwf(
    'clusters_report.txt',
    widths = [8, 12, 15, 100],
    header = None,
    names = ['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave', 'principales_palabras_clave'],
    skip_blank_lines = False
    )
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].map(lambda x: str(x).rstrip('%').replace(',','.'))
    df = df.drop(range(0,4))

    fila = ''
    inicio = 0
    for i, row in df.iterrows():
        if(isinstance(row['cluster'], str)):    #Revisar el inicio del cluster
            inicio = i
        
        if(isinstance(row['principales_palabras_clave'], str)): #Se construye el string para palabras clave
            fila += row['principales_palabras_clave']+' '

        else:                                                                         #Se appendea la fila con el formato requerido
            fila = ', '.join([' '.join(string.split()) for string in fila.split(',')])
            df.at[inicio, 'principales_palabras_clave'] = fila.rstrip(".")
            fila = ''

    df = df[df['cluster'].notna()].astype({
        'cluster':int,
        'cantidad_de_palabras_clave':int,
        'porcentaje_de_palabras_clave':float
    })

    df = df.reset_index(drop = True)

    return df
