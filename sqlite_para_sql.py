import sqlite3
import mysql.connector
from mysql.connector import errorcode
import time

#caminho_db_sqlite3 = ("D:\CNPJS\DB\CNPJ_filtrado.db")
caminho_db_sqlite3 = ("D:\CNPJS\DB\empresas_estado.db")

#Conexão com banco sqlite
connSqlite = sqlite3.connect(caminho_db_sqlite3) #Conecta no banco sqlite3
cursorSqlite = connSqlite.cursor() #Usado para executar os scripts.
#connSqlite.close #fecha conexão



#Conexão com banco mysql



try:
	db_connection = mysql.connector.connect(host='127.0.0.1', user='root', password='', database='cnpj')
	print("Database connection made!")
except mysql.connector.Error as error:
	if error.errno == errorcode.ER_BAD_DB_ERROR:
		print("Database doesn't exist")
	elif error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
		print("User name or password is wrong")
	else:
		print(error)
else:
    print ('Conectado')
cursorMySql = db_connection.cursor() #Cursor do mysql

cursorSqlite.execute("SELECT * FROM emp_acre")
for linha in cursorSqlite.fetchall():
    print (linha)
    time.sleep(0.5)

connSqlite.close #fecha conexão


#sql = ("Insert Into empresas (cnpj,matriz_filial,razao_social,nome_fantasia,situacao,data_situacao,motivo_situacao,nm_cidade_exterior,cod_pais,nome_pais,cod_nat_juridica,data_inicio_ativ,cnae_fiscal,tipo_logradouro,logradouro,numero,complemento,bairro,cep,uf,cod_municipio,municipio,ddd_1,telefone_1,ddd_2,telefone_2,ddd_fax,num_fax,email,qualif_resp,capital_social,porte,opc_simples,data_opc_simples,data_exc_simples,opc_mei,sit_especial,data_sit_especial) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, )") #Insersão de dados

valores = ("")



#cursor.execute(sql)
#db_connection.close()


