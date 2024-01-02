def ejemplos():
    print('''col_a_list(students, "names")

    col_a_tupla(students, "names")

    cols_para_sql_athena(students.printSchema)

    shp_geohash(geohashes_list, spark=spark)
    shp_geohash(geohashes_list, ruta_guardado, nombre_shp, spark=spark)''')

def col_a_list(df, col):

    try:
        df_inter = df.select(f'{col}')
        df_inter = df_inter.dropDuplicates()
        n = df_inter.columns[0]
        row = df_inter.collect()
        s = str(row)
        s = s.replace('[', '').replace(']', '').replace(f'Row({n}=', '').replace(')', '').replace("'", "")
        ls = s.split(', ')
    except:
        print(f'Example:\nslh.col_a_list(Dataframe, "Colum_Name")')
        ls = None

    return ls

def col_a_tupla(df, col):

    try:
        df_inter = df.select(f'{col}')
        df_inter = df_inter.dropDuplicates()
        n = df_inter.columns[0]
        row = df_inter.collect()
        s = str(row)
        s = s.replace('[', '').replace(']', '').replace(f'Row({n}=', '').replace(')', '').replace("'", "")
        tp = tuple(s.split(', '))
    except:
        print(f'Example:\nslh.col_a_tupla(Dataframe, "Colum_Name")')
        tp = None

    return tp

def cols_para_sql_athena(esquema):

    print(f'Example:\nslh.cols_para_sql_athena(df.printSchema)\nImportant: do not use "()" at the end of DF funtion\n\n')

    estring = str(esquema)
    
    estring = estring.split('DataFrame[')[-1].split(']')[0]
    lineas = estring.split(', ')
    lineas = [linea.strip() for linea in lineas if linea.strip()]
    
    separacion = ',\n'.join(lineas)
    separacion = separacion.replace(':', '')
    
    try:
        separacion = separacion.replace('long', 'bigint')
    except:
        pass

    try:
        separacion = separacion.replace('bigintitud', 'longitud')
    except:
        pass

    return separacion


def shp_geohash(hashes, ruta_guardado=None, nombre='output', spark=None):
    import os
    import pandas as pd
    import geopandas as gpd

    def convertir_a_lista_de_listas(original):
        lista_resultante = [[elemento] for elemento in original]
        return lista_resultante

    def crear_archivo_prj(ruta_guardado, nombre_archivo, contenido_prj):
        ruta_completa = os.path.join(ruta_guardado, nombre_archivo + '.prj')
        with open(ruta_completa, 'w') as archivo_prj:
            archivo_prj.write(contenido_prj)

    contenido_prj = 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'

    if ruta_guardado is None:
        ruta_guardado = 'output/'
    elif not ruta_guardado.endswith('/'):
        ruta_guardado = ruta_guardado + '/'

    try:
        os.makedirs(ruta_guardado)
    except:
        pass

    crear_archivo_prj(ruta_guardado, nombre, contenido_prj)

    lista = convertir_a_lista_de_listas(hashes)
    columns = ["GeoHash"]

    dataframe = spark.createDataFrame(lista, schema=columns)
    dataframe.createOrReplaceTempView('df')

    query = '''
    select
        *,
        st_geomFromGeoHash(GeoHash) as geometry
    from df
    '''
    dfx = spark.sql(query)

    pdf = dfx.toPandas()
    gdf = gpd.GeoDataFrame(pdf, geometry='geometry')

    gdf.to_file(ruta_guardado + nombre + '.shp')

    print("ShapeFile creado con exito")
