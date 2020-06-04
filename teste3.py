import sqlite3, pymysql, time

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
    dado = dado.replace("[","")
    dado = dado.replace("]","")
    return dado

def pegarDados(nomeDado,nomeTabela):
    lista = []
    cursorSqlite.execute("SELECT "+ nomeDado +" FROM "+nomeTabela) 
    for linha in cursorSqlite.fetchall():
        dados = tratarDados(linha)
        lista.append(dados)
    return lista

def pegarDadosByID(nomeDado,nomeTabela,count):
    num = str (count)
    cursorSqlite.execute("SELECT " +nomeDado+" FROM empresas where id = "+ num) 
    linha = cursorSqlite.fetchall()
    dados = tratarDados(linha)
    return dados        

count = 0
tamanho_Tabela = 43887581
while count < tamanho_Tabela:
    count = count + 1
    cnpj = pegarDadosByID("cnpj","empresas",count)
    matriz_filial = pegarDadosByID("matriz_filial","empresas",count)
    razao_social = pegarDadosByID("razao_social","empresas",count)
    nome_fantasia = pegarDadosByID("nome_fantasia","empresas",count)
    situacao = pegarDadosByID("situacao","empresas",count)
    data_situacao = pegarDadosByID("data_situacao","empresas",count)
    motivo_situacao = pegarDadosByID("motivo_situacao","empresas",count)
    nm_cidade_exterior = pegarDadosByID("nm_cidade_exterior","empresas",count)
    cod_pais = pegarDadosByID("cod_pais","empresas",count)
    nome_pais = pegarDadosByID("nome_pais","empresas",count)
    cod_nat_juridica = pegarDadosByID("cod_nat_juridica","empresas",count)
    data_inicio_ativ = pegarDadosByID("data_inicio_ativ","empresas",count)
    cnae_fiscal = pegarDadosByID("cnae_fiscal","empresas",count)
    tipo_logradouro = pegarDadosByID("tipo_logradouro","empresas",count)
    logradouro = pegarDadosByID("logradouro","empresas",count)
    numero = pegarDadosByID("numero","empresas",count)
    complemento = pegarDadosByID("complemento","empresas",count)
    bairro = pegarDadosByID("bairro","empresas",count)
    cep = pegarDadosByID("cep","empresas",count)
    uf = pegarDadosByID("uf","empresas",count)
    cod_municipio = pegarDadosByID("cod_municipio","empresas",count)
    municipio = pegarDadosByID("municipio","empresas",count)
    ddd_1 = pegarDadosByID("ddd_1","empresas",count)
    telefone_1 = pegarDadosByID("telefone_1","empresas",count)
    ddd_2 = pegarDadosByID("ddd_2","empresas",count)
    telefone_2 = pegarDadosByID("telefone_2","empresas",count)
    ddd_fax = pegarDadosByID("ddd_fax","empresas",count)
    num_fax = pegarDadosByID("num_fax","empresas",count)
    email = pegarDadosByID("email","empresas",count)
    qualif_resp = pegarDadosByID("qualif_resp","empresas",count)
    capital_social = pegarDadosByID("capital_social","empresas",count)
    porte = pegarDadosByID("porte","empresas",count)
    opc_simples = pegarDadosByID("opc_simples","empresas",count)
    data_opc_simples = pegarDadosByID("data_opc_simples","empresas",count)
    data_exc_simples = pegarDadosByID("data_exc_simples","empresas",count)
    opc_mei = pegarDadosByID("opc_mei","empresas",count)
    sit_especial = pegarDadosByID("sit_especial","empresas",count)
    data_sit_especial = pegarDadosByID("data_sit_especial","empresas",count)  

    count = count + 1

    ##########################
    #Inserindo dados na tabela
    ##########################

    sql = "INSERT INTO empresas (cnpj,matriz_filial,razao_social,nome_fantasia,situacao,data_situacao,motivo_situacao,nm_cidade_exterior,cod_pais,nome_pais,cod_nat_juridica,data_inicio_ativ,cnae_fiscal,tipo_logradouro,logradouro,numero,complemento,bairro,cep,uf,cod_municipio,municipio,ddd_1,telefone_1,ddd_2,telefone_2,ddd_fax,num_fax,email,qualif_resp,capital_social,porte,opc_simples,data_opc_simples,data_exc_simples,opc_mei,sit_especial,data_sit_especial) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" #Insersão de dados

    valores = (cnpj,matriz_filial,razao_social,nome_fantasia,situacao,data_situacao,motivo_situacao,nm_cidade_exterior,cod_pais,nome_pais,cod_nat_juridica,data_inicio_ativ,cnae_fiscal,tipo_logradouro,logradouro,numero,complemento,bairro,cep,uf,cod_municipio,municipio,ddd_1,telefone_1,ddd_2,telefone_2,ddd_fax,num_fax,email,qualif_resp,capital_social,porte,opc_simples,data_opc_simples,data_exc_simples,opc_mei,sit_especial,data_sit_especial)   

    cursorPhpmyadmin.execute(sql, valores)
    print ("Dado número",count + 1,"inserido!\n\n")
    count = count + 1
    time.sleep(0.1)
    print ("Fazendo Commit...")    
    connPhpmyadmin.commit()
   
print ("Fechando Conexão!")
connPhpmyadmin.close()
connSqlite.close()

