import sqlite3
import pymysql

import time


#caminho_db_sqlite3 = ("D:\CNPJS\DB\CNPJ_filtrado.db")
caminho_db_sqlite3 = ("D:\CNPJS\DB\empresas_estado.db")

#Conexão com banco sqlite
connSqlite = sqlite3.connect(caminho_db_sqlite3) #Conecta no banco sqlite3
cursorSqlite = connSqlite.cursor() #Usado para executar os scripts.
#connSqlite.close #fecha conexão

def tratarDados(linha):
    dado = (str(linha))
    dado = dado.replace("(","")
    dado = dado.replace(')','')
    dado = dado.replace(',','')
    dado = dado.replace("'","")
    return dado

def pegarDados(nomeDado,nomeTabela):
    lista = []
    cursorSqlite.execute("SELECT "+ nomeDado +" FROM "+nomeTabela) 
    for linha in cursorSqlite.fetchall():
        dados = tratarDados(linha)
        lista.append(dados)
    return lista

#dados das tabelas
cnpj = pegarDados("cnpj","emp_acre")
matriz_filial = pegarDados("matriz_filial","emp_acre")
razao_social = pegarDados("razao_social","emp_acre")
nome_fantasia = pegarDados("nome_fantasia","emp_acre")
situacao = pegarDados("situacao","emp_acre")
data_situacao = pegarDados("data_situacao","emp_acre")
motivo_situacao = pegarDados("motivo_situacao","emp_acre")
nm_cidade_exterior = pegarDados("nm_cidade_exterior","emp_acre")
cod_pais = pegarDados("cod_pais","emp_acre")
nome_pais = pegarDados("nome_pais","emp_acre")
cod_nat_juridica = pegarDados("cod_nat_juridica","emp_acre")
data_inicio_ativ = pegarDados("data_inicio_ativ","emp_acre")
cnae_fiscal = pegarDados("cnae_fiscal","emp_acre")
tipo_logradouro = pegarDados("tipo_logradouro","emp_acre")
logradouro = pegarDados("logradouro","emp_acre")
numero = pegarDados("numero","emp_acre")
complemento = pegarDados("complemento","emp_acre")
bairro = pegarDados("bairro","emp_acre")
cep = pegarDados("cep","emp_acre")
uf = pegarDados("uf","emp_acre")
cod_municipio = pegarDados("cod_municipio","emp_acre")
municipio = pegarDados("municipio","emp_acre")
ddd_1 = pegarDados("ddd_1","emp_acre")
telefone_1 = pegarDados("telefone_1","emp_acre")
ddd_2 = pegarDados("ddd_2","emp_acre")
telefone_2 = pegarDados("telefone_2","emp_acre")
ddd_fax = pegarDados("ddd_fax","emp_acre")
num_fax = pegarDados("num_fax","emp_acre")
email = pegarDados("email","emp_acre")
qualif_resp = pegarDados("qualif_resp","emp_acre")
capital_social = pegarDados("capital_social","emp_acre")
porte = pegarDados("porte","emp_acre")
opc_simples = pegarDados("opc_simples","emp_acre")
data_opc_simples = pegarDados("data_opc_simples","emp_acre")
data_exc_simples = pegarDados("data_exc_simples","emp_acre")
opc_mei = pegarDados("opc_mei","emp_acre")
sit_especial = pegarDados("sit_especial","emp_acre")
data_sit_especial = pegarDados("data_sit_especial","emp_acre")  





##########################
#Inserindo dados na tabela
##########################

sql = "INSERT INTO empresas (cnpj,matriz_filial,razao_social,nome_fantasia,situacao,data_situacao,motivo_situacao,nm_cidade_exterior,cod_pais,nome_pais,cod_nat_juridica,data_inicio_ativ,cnae_fiscal,tipo_logradouro,logradouro,numero,complemento,bairro,cep,uf,cod_municipio,municipio,ddd_1,telefone_1,ddd_2,telefone_2,ddd_fax,num_fax,email,qualif_resp,capital_social,porte,opc_simples,data_opc_simples,data_exc_simples,opc_mei,sit_especial,data_sit_especial) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" #Insersão de dados

count = -1
print (len(cnpj))
while count != len(cnpj):
    valores = (cnpj[count],matriz_filial[count],razao_social[count],nome_fantasia[count],situacao[count],data_situacao[count],motivo_situacao[count],nm_cidade_exterior[count],cod_pais[count],nome_pais[count],cod_nat_juridica[count],data_inicio_ativ[count],cnae_fiscal[count],tipo_logradouro[count],logradouro[count],numero[count],complemento[count],bairro[count],cep[count],uf[count],cod_municipio[count],municipio[count],ddd_1[count],telefone_1[count],ddd_2[count],telefone_2[count],ddd_fax[count],num_fax[count],email[count],qualif_resp[count],capital_social[count],porte[count],opc_simples[count],data_opc_simples[count],data_exc_simples[count],opc_mei[count],sit_especial[count],data_sit_especial[count])
    #cursorMySql.execute(sql, valores)
    print ("Dado número",count + 1,"inserido!\n\n")
    count = count + 1


connSqlite.close #fecha conexão








#cursor.execute(sql)
#db_connection.close()


