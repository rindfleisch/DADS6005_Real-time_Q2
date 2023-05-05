# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 10:23:47 2023

@author: LEGION
"""

import psycopg2
import json
import pandas as pd
import sys


def getColumnDtypes(dataTypes):
    dataList = []
    for x in dataTypes:
        if(x == 'int64'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
            #dataList.append('int')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar')
    return dataList



source_file=sys.argv[1]
table_name=sys.argv[2]

df = pd.read_csv(source_file).reset_index(drop=False)

columnDataType = getColumnDtypes(df.dtypes)
columnName = list(df.columns.values)
columnName = [x.replace('.', '_') for x in columnName]


sql = f'CREATE TABLE IF NOT EXISTS {table_name} (' 
for i in range(len(columnDataType)):
    if i == 0:
        sql= sql + '\n' + columnName[i] + ' ' + columnDataType[i] + " primary key" +','
        #sql= sql + '\n' + columnName[i] + ' ' +"varchar" + " primary key" +','
    else:
        sql= sql + '\n' + columnName[i] + ' ' + columnDataType[i] + ','
        
#sql= sql + '\n' + 'timestamp' + ' ' + 'varchar' + ','
sql = sql[:-1] + ' );'

print(sql)


conn = psycopg2.connect(
    dbname="quiz02_dev",  # Replace with your database name
    user="root",  # Replace with your username
    password="secret",  # Replace with your password
    host="localhost",  # Replace with your host address
    port="5432"  # Replace with your port number
)


try:
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()

        
        
