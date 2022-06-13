import pandas as pd
#from verificacoes import *
#from mongo import Conector_mongo
import numpy as np
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pandas as pd
from datetime import datetime
from google.cloud import storage
from sqlalchemy import true

# Classes e funções necessáriaspara o funcionamento do código:
class Conector_mongo():
    '''
    Essa classe tem por objetivo realizar operações entre o pyspark e o mongodb atlas.
        user = o nome do seu projeto do mongodb atlas
        password = sua senha do cluster criado no mongodb atlas
        db = a database que será utilizada
        
    '''
    def __init__(self, user, password, db):
        self.user = user
        self.password = password
        self.db = db
  
    def inserir_mongo(self, df, collection):
        '''
        Esse método tem por objetivo inserir todos os dados de uma dataframe spark no mongodb atlas
            df = a dataframe do spark que deseja realizar a inserção
            collection = o nome da collection que deseja inserir os dados
        '''
        self.collection=collection
        self.df = df
        mongo_ip = f"mongodb://{self.user}:{self.password}@ac-5uquupr-shard-00-00.bjjkitq.mongodb.net:27017,ac-5uquupr-shard-00-01.bjjkitq.mongodb.net:27017,ac-5uquupr-shard-00-02.bjjkitq.mongodb.net:27017/?ssl=true&replicaSet=atlas-dzh8bl-shard-0&authSource=admin&retryWrites=true/{self.db}."
        self.df.write.format('com.mongodb.spark.sql.DefaultSource')\
            .option('spark.mongodb.output.database', self.db)\
            .option('spark.mongodb.output.collection', self.collection)\
            .option('uri', mongo_ip + self.collection)\
            .mode('Overwrite')\
            .option('maxBatchSize', "80000000").save()
    
    def ler_mongo(self, spark_session, collection):
        '''
        Esse método tem por objetivo ler os dados de uma collection do mongodb atlas, retornando uma dataframe
        do pyspark
            spark_session = o nome da sua SparkSession
            collection = O nome da collection que deseja extrair os dados
        '''
        self.collection = collection
        self.spark_session = spark_session
        mongo_ip = f"mongodb://{self.user}:{self.password}@ac-5uquupr-shard-00-00.bjjkitq.mongodb.net:27017,ac-5uquupr-shard-00-01.bjjkitq.mongodb.net:27017,ac-5uquupr-shard-00-02.bjjkitq.mongodb.net:27017/?ssl=true&replicaSet=atlas-dzh8bl-shard-0&authSource=admin&retryWrites=true/{self.db}."
        self.df = ( self.spark_session.read.format('com.mongodb.spark.sql.DefaultSource')
                   .option('spark.mongodb.input.database', self.db)
                   .option('spark.mongodb.input.collection', self.collection)
                   .option('uri', mongo_ip + self.collection).load()) 
        return self.df

class Arquivo:
    def __init__(self, nome, pasta, bucket_name, dfs, tipo):
        self.nome = nome
        self.pasta = pasta
        self.bucket_name = bucket_name
        self.dfs = dfs
        self.tipo = tipo
  
    def envia_arquivo(self):
        '''
        Essa função tem como objetivo enviar e organizar arquivos no bucket do csv
        nome = nome do arquivo que vai ser enviado ex: 'dados.csv'
        pasta = nome da pasta do arquivo que será enviado, no formato 'nome_pasta/'
        bucket = nome do bucket em que o arquivo será enviado ex:'nome-bucket'
        dfs = dataframe do spark que será convertida e enviada
        tipo = tipo do arquivo que será enviado ex: csv
        '''
        # Conectando com o cloud storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)

        # Enviando o arquivo para o bucket na pasta desejada:
        if self.tipo == 'json':
            self.dfs.coalesce(1).write.json(f'gs://{self.bucket_name}/deletar2/{self.nome}')
        else:
          self.dfs.coalesce(1).write.option("header", True).option("encoding", "UTF-8").save(f'gs://{self.bucket_name}/deletar2/{self.nome}', format=self.tipo)

        # Renomeando do arquivo para o nome desejado - Pegando o blob antigo:
        blobs = storage_client.list_blobs(self.bucket_name)
        for blob in blobs:
            blob_name = str(blob)
            list_blob = blob_name.split(',')
            blob_path = list_blob[1]
            if f"deletar2/{self.nome}/part" in blob_path:
                blob_antigo = blob
                blob_antigo_nome = blob_path

        # Movendo o arquivo para a pasta desejada:
        blob_copy = bucket.copy_blob(blob_antigo, bucket, f'{self.pasta}/{self.nome}')

        blob_antigo_nome = blob_antigo_nome.split(" ")
        blob_antigo_nome = blob_antigo_nome[1]

        # Deletando a pasta com os arquivos anteriores:
        bucket.delete_blob(blob_antigo_nome)
        
class Conector_mysql:
    def __init__(self, host, user, password, db):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
    
    def envia_mysql(self, dfs, table):
        self.dfs = dfs
        self.table = table
        self.dfs.write.format("jdbc")\
                .option('url', f'jdbc:mysql://{self.host}/{self.db}')\
                .option('driver', 'com.mysql.cj.jdbc.Driver')\
                .option("numPartitions", "10") \
                .option("user",self.user)\
                .option("password", self.password)\
                .option("dbtable", self.db + "." + self.table)\
                .mode("append").save()
    
    def ler_mysql(self, table, spark_conection):
        self.table = table
        self.spark_conection = spark_conection
        self.df = self.spark_conection.read.format("jdbc")\
            .option('url', f'jdbc:mysql://{self.host}/{self.db}')\
            .option('driver', 'com.mysql.cj.jdbc.Driver')\
            .option("user",self.user)\
            .option("password", self.password)\
            .option("dbtable", self.db + "." + self.table).load()
        return self.df
    
# Criando a SparkSession:
conf =( pyspark.SparkConf()
               .set("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
               .set("spark.jars", 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar')
               .setMaster("local")
               .setAppName("job-3")
               .setAll([('spark.driver.memory', '40g'),('spark.executor.memory','50g')]))
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')

# Conectando com o mySQL
conexao_mysql = Conector_mysql("34.72.50.43", "root", ">}Dzh.=}YhZ#(G>s", "original")

# Puxando os arquivos do mYSQL:
dfs_arrecadacao = conexao_mysql.ler_mysql('arrecadacao', spark)
dfs_barragens = conexao_mysql.ler_mysql('barragens', spark)
dfs_autuacao = conexao_mysql.ler_mysql('autuacao', spark)
dfs_beneficiada = conexao_mysql.ler_mysql('beneficiada', spark)
dfs_distribuicao = conexao_mysql.ler_mysql('distribuicao', spark)
dfs_municipio = conexao_mysql.ler_mysql('municipio', spark)
dfs_pib = conexao_mysql.ler_mysql('pib', spark)
dfs_dados_populacao = conexao_mysql.ler_mysql('dados_populacao', spark)

# Transformando DataFrame Arrecadação Spark em DataFrame Pandas
dfp_arrecadacao = dfs_arrecadacao.toPandas()

# Tratamento dos dados
dfp_arrecadacao.drop_duplicates()
dfp_arrecadacao.replace(to_replace='ARGILA P/CER. VERMELH', value='ARGILA P/CER. VERMELHA', inplace=True)
dfp_arrecadacao.replace(to_replace='-', value=np.nan, inplace=True)
dfp_arrecadacao.fillna(np.nan)
dfp_arrecadacao.replace(to_replace='None', value=np.nan, inplace=True)
dfp_arrecadacao.drop(['CPF_CNPJ', 'index'], axis=1, inplace=True)
dfp_arrecadacao['QuantidadeComercializada'] = dfp_arrecadacao['QuantidadeComercializada'].str.replace(',', '.')
dfp_arrecadacao['QuantidadeComercializada'] = dfp_arrecadacao['QuantidadeComercializada'].str.replace('"', '')
dfp_arrecadacao['QuantidadeComercializada'].replace('""', 0)
dfp_arrecadacao['ValorRecolhido'] = dfp_arrecadacao['ValorRecolhido'].str.replace(',', '.')
dfp_arrecadacao['QuantidadeComercializada'] = dfp_arrecadacao['QuantidadeComercializada'].astype(float)
dfp_arrecadacao['ValorRecolhido'] = dfp_arrecadacao['ValorRecolhido'].astype(float)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='m3', value='Metros Cubicos', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='t', value='Toneladas', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='l', value='Litros', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='g', value='Gramas', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='m2', value='Metros Quadrados', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='ct', value='Quilates', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='kg', value='Quilograma', inplace=True)
dfp_arrecadacao.replace(to_replace='OURO', value='MINÉRIO DE OURO', inplace=True)
dfp_arrecadacao.replace(to_replace='FERRO', value='MINÉRIO DE FERRO', inplace=True)
dfp_arrecadacao.replace(to_replace='COBRE', value='MINÉRIO DE COBRE', inplace=True)

# Transformando DataFrame PIB Spark em DataFrame Pandas
dfp_pib = dfs_pib.toPandas()

# Tratamentos:
dfp_pib.drop(['index'], axis=1, inplace=True)
for i in range(len(dfp_pib)):
    if dfp_pib.loc[i, "impostos_liquidos"] < 0:
        dfp_pib.loc[i, "impostos_liquidos"] = (-1) * dfp_pib.loc[i, "impostos_liquidos"]

# Transformando Dataframe Barragens Spark em Dataframe Pandas
dfp_barragens = dfs_barragens.toPandas()

# Tratamento
dfp_barragens.drop(['ID', 'index', 'N pessoas afetadas a jusante em caso de rompimento da barragem', 'CPF_CNPJ', 'Latitude',
       'Longitude', 'Posicionamento',
       'Dano Potencial Associado - DPA', 'Classe', 'Necessita de PAEBM',
       'Inserido na PNSB', 'Status da DCE Atual',
       'Barragem possui estrutura interna selante',
       'Quantidade Diques Internos', 'Quantidade Diques Selantes',
       'A barragem de mineração possui Back Up Dam',
       'Esta Back Up Dam está operando pós rompimento da barragem',
       'Nome da Back Up Dam', 'UF (Back Up Dam)', 'Município (Back Up Dam)',
       'Situação operacional da Back Up Dam', 'Desde (Back Up Dam)',
       'Vida útil prevista da Back Up Dam (Anos)',
       'Previsão de término de construção da Back Up Dam',
       'A BUD está em Área do Processo ANM ou da Área de Servidão',
       'Processos associados (Back Up Dam)', 'Posicionamento (Back Up Dam)',
       'Latitude (Back Up Dam)', 'Longitude (Back Up Dam)',
       'Altura Máxima do projeto da Back Up Dam (m)',
       'Comprimento da Crista do projeto da Back Up Dam (m)',
       'Volume do projeto da Back Up Dam (m³)',
       'Descarga Máxima do vertedouro da Dack Up Dam (m³/seg)',
       'Possui documentação de segurança com ART',
       'Existe manual de operação da Back Up Dam',
       'A Back up Dam passou por auditoria de terceira parte',
       'BUD garante redução da área da mancha de inundação à jusante',
       'Tipo de Back Up Dam quanto ao material de construção',
       'Tipo de fundação da Back Up Dam', 'Vazão de projeto da Back Up Dam',
       'Método construtivo da Back Up Dam',
       'Tipo de auscultação da Back Up Dam', 'Desde',
       'Está dentro da Área do Processo ANM ou da Área de Servidão',
       'Barragem de mineração é alimentado por usina', 'Usinas',
       'Processo de beneficiamento', 'Produtos químicos utilizados',
       'A Barragem armazena rejeitos/residuos que contenham Cianeto',
       'Teor (%) do minério principal inserido no rejeito',
       'Outras substâncias minerais presentes no reservatório',
       'Altura máxima do projeto licenciado (m)', 'Altura máxima atual (m)',
       'Comprimento da crista do projeto (m)',
       'Comprimento atual da crista (m)',
       'Descarga máxima do vertedouro (m³/seg)', 'Área do reservatório (m²)',
       'Tipo de barragem quanto ao material de construção', 'Tipo de fundação',
       'Vazão de projeto', 'Método construtivo da barragem',
       'Tipo de alteamento', 'Tipo de auscultação',
       'A Barragem de Mineração possui Manta Impermeabilizante',
       'Data da última Vistoria de Inspeção Regular',
       'Confiabilidade das estruturas extravasora', 'Percolação',
       'Deformações e recalque', 'Deteriorização dos taludes / paramentos',
       'Documentação de projeto',
       'Profissionais na equipe de Segurança da Barragem',
       'Manuais para Inspeções de Segurança e Monitoramento',
       'PAE - Plano de Ação Emergencial',
       'Cópias físicas do PAEBM entregues as Prefeituras e Defesas Civis',
       'Relatórios da instrumentação e de Análise de Segurança',
       'Volume de projeto licenciado do Reservatório (m³)',
       'Volume atual do Reservatório (m³)',
       'Existência de população a jusante',
       'Data da Finalização da DCE', 'Motivo de Envio', 'RT/Declaração',
       'RT/Empreendimento','Impacto ambiental', 'Impacto sócio-econômico'], axis=1, inplace=True)

dfp_barragens.fillna(np.nan)
dfp_barragens['Vida útil prevista da Barragem (anos)'] = dfp_barragens['Vida útil prevista da Barragem (anos)'].str.replace(',', '.')
dfp_barragens['Nome'] = dfp_barragens['Nome'].str.replace(',', '.')
dfp_barragens['Nome'] = dfp_barragens['Nome'].str.replace('"', '')
dfp_barragens.replace(to_replace='-', value=np.nan, inplace=True)
dfp_barragens['Vida útil prevista da Barragem (anos)'] = dfp_barragens['Vida útil prevista da Barragem (anos)'].astype(float)

# Transformando DataFrame Dados População Spark em DataFrame Pandas
dfp_dados_populacao = dfs_dados_populacao.toPandas()

# Tratamento:
dfp_dados_populacao.drop(['index'], axis=1, inplace=True)


# Transformando o dataframe municipio spark em Dataframe Pandas
dfp_municipio = dfs_municipio.toPandas()

# Tratamentos
dfp_municipio.rename(columns={"COD. UF":"cod_uf", "COD. MUNIC":"cod_munic", "NOME DO MUNICÍPIO":"nome_municipio", "POPULAÇÃO ESTIMADA":"populacao_estimada"}, inplace=True)
dfp_municipio["codigo_municipio"] = dfp_municipio["cod_uf"]*100000 + dfp_municipio["cod_munic"]
dfp_municipio['populacao_estimada'] = dfp_municipio['populacao_estimada'].str.replace('.', '')

for i in range(len(dfp_municipio)):
    texto = str(dfp_municipio.loc[i, "populacao_estimada"])
    if "(" in texto:
        list_texto = texto.split('(')
        dfp_municipio.loc[i, "populacao_estimada"] = list_texto[0]
dfp_municipio['populacao_estimada'] = dfp_municipio['populacao_estimada'].astype(float)
# Fazendo as configurações necessárias:
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Transformando em DF spark:
dft_arrecadacao = spark.createDataFrame(dfp_arrecadacao)
dft_barragens = spark.createDataFrame(dfp_barragens)
dft_dados_populacao = spark.createDataFrame(dfp_dados_populacao)
dft_pib = spark.createDataFrame(dfp_pib)
dft_municipio = spark.createDataFrame(dfp_municipio)

# Fazendo a conexão com o mongo:
db_conexao_tratada = Conector_mongo('soulcode-mineracao', 'mongodb', 'tratados')

# Enviando os dados tratados para o mongo:
db_conexao_tratada.inserir_mongo(dft_arrecadacao, 'arrecadacao')
db_conexao_tratada.inserir_mongo(dft_barragens, 'barragens')
db_conexao_tratada.inserir_mongo(dft_dados_populacao, 'dados_populacao')
db_conexao_tratada.inserir_mongo(dft_pib, 'pib')
db_conexao_tratada.inserir_mongo(dft_municipio, 'municipio')

# Enviando os dados tratados para o bucket:
arrecadacao = Arquivo('arrecadacao.csv','tratados','soulcode-mineracao', dft_arrecadacao, 'csv')
barragens = Arquivo('barragens.csv', 'tratados', 'soulcode-mineracao', dft_barragens, 'csv')
dados_populacao = Arquivo('dados_populacao.csv', 'tratados', 'soulcode-mineracao', dft_dados_populacao, 'csv')
pib = Arquivo('pib.csv','tratados', 'soulcode-mineracao', dft_pib, 'csv')
municipio = Arquivo('municipio.csv', 'tratados', 'soulcode-mineracao', dft_municipio, 'csv')

arrecadacao.envia_arquivo()
barragens.envia_arquivo()
dados_populacao.envia_arquivo()
pib.envia_arquivo()
municipio.envia_arquivo()