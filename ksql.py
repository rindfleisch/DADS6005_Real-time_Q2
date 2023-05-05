# -*- coding: utf-8 -*-
"""
Created on Sat Apr 22 11:39:53 2023

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
            dataList.append('double')
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


sql = f'CREATE STREAM {table_name} (' 
for i in range(len(columnDataType)):
    if i == 0:
        sql= sql + '\n' + columnName[i] + ' ' + columnDataType[i] + " key" +','
    else:
        sql= sql + '\n' + columnName[i] + ' ' + columnDataType[i] + ','

sql= sql + '\n' + 'timestamp' + ' ' + 'varchar' + ','
sql = sql[:-1] + f' )  WITH (KAFKA_TOPIC=\'{table_name}\',VALUE_FORMAT=\'AVRO\');'

print(sql)