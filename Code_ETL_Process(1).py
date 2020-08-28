# -*- coding: utf-8 -*-
"""
@author: Alex
"""

# Etapa de carga de base de datos
import pandas as pd

URL = pd.read_csv('https://raw.githubusercontent.com/diljeet1994/Python_Tutorials/master/Projects/Advanced%20ETL/crypto-markets.csv')
URL.head()

# Etapa de Extracción y Transformación
import numpy as np
import pandas as pd

Code_Assets = ['BTC','ETH','XRP','LTC']

# Convertir el precio 'open', 'close', 'high' and 'low' de las criptomonedas en valores de GBP,
# ya que el precio actual está en dólares, si la moneda pertenece a esta lista ['BTC', 'ETH', 'XRP', 'LTC'].
URL['open'] = URL[['open', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in Code_Assets else np.nan, axis=1)
URL['close'] = URL[['close', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in Code_Assets else np.nan, axis=1)
URL['high'] = URL[['high', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in Code_Assets else np.nan, axis=1)
URL['low'] = URL[['low', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in Code_Assets else np.nan, axis=1)

# Eliminación de filas con valores nulos por columna de activos
URL.dropna(inplace=True)

# Restablecer el índice del marco de datos
URL.reset_index(drop=True ,inplace=True)
URL.head()

# Etapa de Transformación
# Eliminamos las columnas que no deseamos visualizar
URL.drop(labels=['slug', 'ranknow', 'volume', 'market', 'close_ratio', 'spread'], inplace=True, axis=1)
URL.head()

# Etapa de Carga de datos
import sqlite3

# La función de conexión abre una conexión al archivo de base de datos SQLite
conn = sqlite3.connect('Tabla.db')
print(conn)
# Output: <sqlite3.Connection object at 0x0000015A87671730>

# Eliminar una columna o tabla que se llame Crypto, en caso de existir
try:
    conn.execute('DROP TABLE IF EXISTS `Crypto` ')
except Exception as e:
    raise(e)
finally:
    print('Tabla Eliminada')

# Creamos una nueva tabla llamada 'Crypto'
try:
    conn.execute('''
         CREATE TABLE Crypto
         (ID         INTEGER PRIMARY KEY,
         ASSET       TEXT    NOT NULL,
         NAME        TEXT    NOT NULL,
         Date        datetime,
         Open        Float DEFAULT 0,
         High        Float DEFAULT 0,
         Low         Float DEFAULT 0,
         Close       Float DEFAULT 0);''')
    print ("Tabla Creada Con Éxito");
except Exception as e:
    print(str(e))
    print('Error en la Creación de Tabla!!!!!')
finally:
    conn.close()
    
# Etapa de Carga de datos
# Para la insertar los datos, nuevamente necesitamos cambiar nuestros datos de Pandas Dataframe a Python
# con 'List of Lists' o 'List of Tuples', porque ese es el formato que el módulo sqlite entiende para la inserción de datos.

# Esto convertirá el marco de datos de pandas en una lista de la lista
crypto_list = URL.values.tolist()

# Se hace una nueva conexión para insertar desde la base de datos Crypto en SQL DB
conn = sqlite3.connect('Tabla.db')

# Generamos un cursor que nos permitirá consultar la base de datos SQL
cur = conn.cursor()

try:
    # Se utilizará el signo pregunta (?) para representar los nombres de cada columna dentro de VALUE ().
    cur.executemany("INSERT INTO Crypto(ASSET, NAME, Date, Open, High, Low, Close) VALUES (?,?,?,?,?,?,?)", crypto_list)
    conn.commit()
    print('Datos insertados con Éxito')
except Exception as e:
    print(str(e))
    print('Error en la Inserción de Datos')
finally:
    # Finalmente, cerramos nuestra conexión a la base de datos, incluso en caso de error.
    conn.close()
 
# Etapa de visualización para comprobar que el Proceso ETL se haya realizado con éxito
# Visualización de la base de datos SQL creada
conn = sqlite3.connect('Tabla.db')
rows = conn.cursor().execute('Select * from Crypto')

for row in rows:
    print(row)
conn.close()