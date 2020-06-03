import sqlite3
import MySqldb

caminho_db_sqlite3 = ("D:\CNPJS\DB\CNPJ_filtrado.db")

#Conexão com banco sqlite
connSqlite = sqlite3.connect(caminho_db_sqlite3) #Conecta no banco sqlite3
cursorSqlite = connSqlite.cursor() #Usado para executar os scripts.
connSqlite.close #fecha conexão

#Conexão com banco mysql

