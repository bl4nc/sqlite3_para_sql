import sqlite3
import pymysql
import time


caminho_db_sqlite3 = ("D:\CNPJS\DB\CNPJ_filtrado.db")
#caminho_db_sqlite3 = ("D:\CNPJS\DB\empresas_estado.db")

#Conexão com banco sqlite
connSqlite = sqlite3.connect(caminho_db_sqlite3) #Conecta no banco sqlite3
cursorSqlite = connSqlite.cursor() #Usado para executar os scripts.
#connSqlite.close #fecha conexão

#conexão com phpmyadmin

connPhpmyadmin = pymysql.connect(db='dados_empresas', user='root', passwd='')
cursorPhpmyadmin = connPhpmyadmin.cursor()

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
cnpj = pegarDados("cnpj","empresas")
matriz_filial = pegarDados("matriz_filial","empresas")
razao_social = pegarDados("razao_social","empresas")
nome_fantasia = pegarDados("nome_fantasia","empresas")
situacao = pegarDados("situacao","empresas")
data_situacao = pegarDados("data_situacao","empresas")
motivo_situacao = pegarDados("motivo_situacao","empresas")
nm_cidade_exterior = pegarDados("nm_cidade_exterior","empresas")
cod_pais = pegarDados("cod_pais","empresas")
nome_pais = pegarDados("nome_pais","empresas")
cod_nat_juridica = pegarDados("cod_nat_juridica","empresas")
data_inicio_ativ = pegarDados("data_inicio_ativ","empresas")
cnae_fiscal = pegarDados("cnae_fiscal","empresas")
tipo_logradouro = pegarDados("tipo_logradouro","empresas")
logradouro = pegarDados("logradouro","empresas")
numero = pegarDados("numero","empresas")
complemento = pegarDados("complemento","empresas")
bairro = pegarDados("bairro","empresas")
cep = pegarDados("cep","empresas")
uf = pegarDados("uf","empresas")
cod_municipio = pegarDados("cod_municipio","empresas")
municipio = pegarDados("municipio","empresas")
ddd_1 = pegarDados("ddd_1","empresas")
telefone_1 = pegarDados("telefone_1","empresas")
ddd_2 = pegarDados("ddd_2","empresas")
telefone_2 = pegarDados("telefone_2","empresas")
ddd_fax = pegarDados("ddd_fax","empresas")
num_fax = pegarDados("num_fax","empresas")
email = pegarDados("email","empresas")
qualif_resp = pegarDados("qualif_resp","empresas")
capital_social = pegarDados("capital_social","empresas")
porte = pegarDados("porte","empresas")
opc_simples = pegarDados("opc_simples","empresas")
data_opc_simples = pegarDados("data_opc_simples","empresas")
data_exc_simples = pegarDados("data_exc_simples","empresas")
opc_mei = pegarDados("opc_mei","empresas")
sit_especial = pegarDados("sit_especial","empresas")
data_sit_especial = pegarDados("data_sit_especial","empresas")  





##########################
#Inserindo dados na tabela
##########################

sql = "INSERT INTO empresas (cnpj,matriz_filial,razao_social,nome_fantasia,situacao,data_situacao,motivo_situacao,nm_cidade_exterior,cod_pais,nome_pais,cod_nat_juridica,data_inicio_ativ,cnae_fiscal,tipo_logradouro,logradouro,numero,complemento,bairro,cep,uf,cod_municipio,municipio,ddd_1,telefone_1,ddd_2,telefone_2,ddd_fax,num_fax,email,qualif_resp,capital_social,porte,opc_simples,data_opc_simples,data_exc_simples,opc_mei,sit_especial,data_sit_especial) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" #Insersão de dados

count = -1
print (len(cnpj))
while count != len(cnpj):
    valores = (cnpj[count],matriz_filial[count],razao_social[count],nome_fantasia[count],situacao[count],data_situacao[count],motivo_situacao[count],nm_cidade_exterior[count],cod_pais[count],nome_pais[count],cod_nat_juridica[count],data_inicio_ativ[count],cnae_fiscal[count],tipo_logradouro[count],logradouro[count],numero[count],complemento[count],bairro[count],cep[count],uf[count],cod_municipio[count],municipio[count],ddd_1[count],telefone_1[count],ddd_2[count],telefone_2[count],ddd_fax[count],num_fax[count],email[count],qualif_resp[count],capital_social[count],porte[count],opc_simples[count],data_opc_simples[count],data_exc_simples[count],opc_mei[count],sit_especial[count],data_sit_especial[count])
    cursorPhpmyadmin.execute(sql, valores)
    print ("Dado número",count + 1,"inserido!\n\n")
    count = count + 1
    time.sleep(0.1)

print ("Fazendo Commit...")    
connPhpmyadmin.commit()
print ("Fechando Conexão!")
connPhpmyadmin.close()
connSqlite.close()

