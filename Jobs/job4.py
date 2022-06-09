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

# Função para fazer a verificação de texto
def verificacao_texto(data_frame, coluna, tamanho_texto: None, numeros: bool):
    '''
    Função para identificar problemas em colunas que contenham apenas letras e números
    '''
    problemas = []
    for i in range(len(data_frame)):
        texto = data_frame.loc[i, coluna]
        if numeros == False:
            if texto.isalpha() == False:
                problemas.append(data_frame.loc[i, coluna])
            elif tamanho_texto != None:
                if len(texto) != tamanho_texto:
                    problemas.append(data_frame.loc[i, coluna])
        elif numeros == True:
            if texto.isalnum() == False:
                problemas.append(data_frame.loc[i, coluna])
            elif tamanho_texto != None:
                if len(texto) != tamanho_texto:
                    problemas.append(data_frame.loc[i, coluna])
    # Imprimindo os problemas que deverão ser corrigidos:
    df_problemas = pd.DataFrame(problemas, columns=["corrigir"])
    # df_problemas = pd.DataFrame(df_problemas.corrigir.unique(), columns=["Corrigir:"])
    if len(df_problemas) > 0:
        print(df_problemas)
    else:
        print("Nenhum problema detectado nesta coluna")
    print(f"Verificação da coluna {coluna} concluída")


def verificacao_tipo(data_frame, coluna, tipo: type):
    '''
    Função para verificar problemas relacionado ao tipo de dados que a coluna deve possuir
    '''
    problemas = []
    for i in range(len(data_frame)):
        try:
            tipo(data_frame.loc[i, coluna])
        except Exception:
            problemas.append(data_frame.loc[i, coluna])
    df_problemas = pd.DataFrame(problemas, columns=["corrigir"])
    df_problemas = pd.DataFrame(df_problemas.corrigir.unique(), columns=["Corrigir: "])
    print("--------------------------------------------------------------------")
    print(f"Verificando a coluna {coluna}: ")
    if len(df_problemas) > 0:
        print(df_problemas)
    else:
        print("Nenhum problema detectado nessa coluna")
    print(f"Verificação da coluna {coluna} concluída")
    return df_problemas


def verificacao_valor_padrao(data_frame, coluna):
    '''
    Função para identificar os valores únicos presentes em uma coluna, útil para caso de colunas que possuem valores padronizados, como por exemplo 'SIM' e 'NÃO'
    '''
    unicos = data_frame[coluna].unique()
    print("---------------------------------------------------------------------")
    print(f"Verificando valores únicos da coluna {coluna}: ")
    print(unicos)
    print("Verificação concluída")


def verificacao_data(data_frame, coluna, formato: str):
    '''
    Função para verificar se todos os valores de uma coluna correspondem a data do formato especificado
    '''
    problemas = []
    # Verificando se tratam-se de datas:
    for i in range(len(data_frame)):
        try:
            datetime.now() - datetime.strptime(data_frame.loc[i, coluna], formato)
        except Exception:
            if data_frame.loc[i, coluna] != 'NULO':
                problemas.append(data_frame.loc[i, coluna])
    df_problemas = pd.DataFrame(problemas, columns=["Corrigir:"])
    print("---------------------------------------------------------------------")
    print(f"Verificando a coluna {coluna}: ")
    if len(df_problemas) > 0:
        print(df_problemas)
    else:
        print("Não há nenhum problema para corrigir")
    print(f"Verificação da coluna {coluna} concluída! ")

#Colocando a classe do mongo
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
          self.dfs.coalesce(1).write.option("header", True).option("encoding", "latin1").save(f'gs://{self.bucket_name}/deletar2/{self.nome}', format=self.tipo)

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
        
# Criando a SparkSession:
conf =( pyspark.SparkConf()
               .set("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
               .set("spark.jars", 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar')
               .setMaster("local")
               .setAppName("job-4")
               .setAll([('spark.driver.memory', '40g'),('spark.executor.memory','50g')]))
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')

# Conectando com o mongo atlas

db_conexao = Conector_mongo('soulcode-mineracao', 'mongodb', 'original')

# Puxando os arquivos do mongo_db

dfs_arrecadacao = db_conexao.ler_mongo(spark,'arrecadacao')
dfs_barragens = db_conexao.ler_mongo(spark, 'barragens')
dfs_autuacao = db_conexao.ler_mongo(spark, 'autuacao')
dfs_beneficiada = db_conexao.ler_mongo(spark, 'beneficiada')
dfs_distribuicao = db_conexao.ler_mongo(spark, 'distribuicao')
dfs_municipio = db_conexao.ler_mongo(spark, 'municipio')
dfs_pib = db_conexao.ler_mongo(spark, 'pib')
dfs_dados_populacao = db_conexao.ler_mongo(spark, 'dados_populacao')

# Transformando DataFrame Arrecadação Spark em DataFrame Pandas

dfp_arrecadacao = dfs_arrecadacao.toPandas()

# Analisando dados

dfp_arrecadacao.info()
dfp_arrecadacao.info()
dfp_arrecadacao.isna().sum()
dfp_arrecadacao.duplicated().sum()
dfp_arrecadacao.Ano.unique()
dfp_arrecadacao.Mês.unique()
verificacao_tipo(dfp_arrecadacao, 'Processo', int)
verificacao_texto(dfp_arrecadacao, 'Tipo_PF_PJ', None, False)
verificacao_valor_padrao(dfp_arrecadacao, 'Substância')
verificacao_valor_padrao(dfp_arrecadacao, 'UF')
verificacao_valor_padrao(dfp_arrecadacao, 'Município')
verificacao_valor_padrao(dfp_arrecadacao, 'QuantidadeComercializada')
verificacao_valor_padrao(dfp_arrecadacao, 'QuantidadeComercializada')
verificacao_valor_padrao(dfp_arrecadacao, 'UnidadeDeMedida')
verificacao_valor_padrao(dfp_arrecadacao, 'ValorRecolhido')

# Tratamento dos dados

dfp_arrecadacao.drop_duplicates()
dfp_arrecadacao.replace(to_replace='ARGILA P/CER. VERMELH', value='ARGILA P/CER. VERMELHA', inplace=True)
dfp_arrecadacao.replace(to_replace='-', value=np.nan, inplace=True)
dfp_arrecadacao.fillna(np.nan)
dfp_arrecadacao.replace(to_replace='None', value=np.nan, inplace=True)
dfp_arrecadacao.drop(['CPF_CNPJ', '_id'], axis=1, inplace=True)
dfp_arrecadacao['QuantidadeComercializada'] = dfp_arrecadacao['QuantidadeComercializada'].str.replace(',', '.')
dfp_arrecadacao['ValorRecolhido'] = dfp_arrecadacao['ValorRecolhido'].str.replace(',', '.')
dfp_arrecadacao['QuantidadeComercializada'] = dfp_arrecadacao['QuantidadeComercializada'].astype(float)
dfp_arrecadacao['ValorRecolhido'] = dfp_arrecadacao['ValorRecolhido'].astype(float)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='m3', value='Metros Cubicos', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='t', value='Toneladas', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='l', value='Litros', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='g', value='Gramas', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='m2', value='Metros Quadrados', inplace=True)
dfp_arrecadacao['UnidadeDeMedida'].replace(to_replace='ct', value='Quilates', inplace=True)
dfp_arrecadacao.replace(to_replace='OURO', value='MINÉRIO DE OURO', inplace=True)
dfp_arrecadacao.replace(to_replace='FERRO', value='MINÉRIO DE FERRO', inplace=True)
dfp_arrecadacao.replace(to_replace='COBRE', value='MINÉRIO DE COBRE', inplace=True)
# Conferindo resultados

print(dfp_arrecadacao)

# Transformando DataFrame PIB Spark em DataFrame Pandas

dfp_pib = dfs_pib.toPandas()

# Analisando dados

dfp_pib.duplicated().sum()
print(dfp_pib.isna().sum())
print(dfp_pib.dtypes)

#Tratamento necessário
dfp_pib.drop(['_id'], axis=1, inplace=True)

# Transformando Dataframe Barragens Spark em Dataframe Pandas

dfp_barragens = dfs_barragens.toPandas()

# Analisando dados

dfp_barragens.info()
print(dfp_barragens.columns)
dfp_barragens.drop(['ID', '_id', 'Nome', 'CPF_CNPJ', 'Latitude',
       'Longitude', 'Posicionamento',
       'Dano Potencial Associado - DPA', 'Classe', 'Necessita de PAEBM',
       'Inserido na PNSB', 'Status da DCE Atual',
       'A Barragem de Mineração possui outra estrutura de mineração interna selante de reservatório',
       'Quantidade Diques Internos', 'Quantidade Diques Selantes',
       'A barragem de mineração possui Back Up Dam',
       'Esta Back Up Dam está operando pós rompimento da barragem de mineração',
       'Nome da Back Up Dam', 'UF (Back Up Dam)', 'Município (Back Up Dam)',
       'Situação operacional da Back Up Dam', 'Desde (Back Up Dam)',
       'Vida útil prevista da Back Up Dam (Anos)',
       'Previsão de término de construção da Back Up Dam',
       'A Back Up Dam está dentro da Área do Processo ANM ou da Área de Servidão',
       'Processos associados (Back Up Dam)', 'Posicionamento (Back Up Dam)',
       'Latitude (Back Up Dam)', 'Longitude (Back Up Dam)',
       'Altura Máxima do projeto da Back Up Dam (m)',
       'Comprimento da Crista do projeto da Back Up Dam (m)',
       'Volume do projeto da Back Up Dam (m³)',
       'Descarga Máxima do vertedouro da Dack Up Dam (m³/seg)',
       'Existe documento que ateste a segurança estrutural e a capacidade para contenção de rejeitos da Back Up Dam com ART',
       'Existe manual de operação da Back Up Dam',
       'A Back up Dam passou por auditoria de terceira parte',
       'A Back Up Dam garante a redução da área da mancha de inundação à jusante',
       'Tipo de Back Up Dam quanto ao material de construção',
       'Tipo de fundação da Back Up Dam', 'Vazão de projeto da Back Up Dam',
       'Método construtivo da Back Up Dam',
       'Tipo de auscultação da Back Up Dam', 'Situação Operacional', 'Desde',
       'A Barragem de Mineração está dentro da Área do Processo ANM ou da Área de Servidão',
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
       'Estrutura organizacional e qualificação técnica dos profissionais na equipe de Segurança da Barragem',
       'Manuais de Procedimentos para Inspeções de Segurança e Monitoramento',
       'PAE - Plano de Ação Emergencial (quando exigido pelo órgão fiscalizador)',
       'As cópias físicas do PAEBM foram entregues para as Prefeituras e Defesas Civis municipais e estaduais',
       'Relatórios de inspeção e monitoramento da instrumentação e de Análise de Segurança',
       'Volume de projeto licenciado do Reservatório (m³)',
       'Volume atual do Reservatório (m³)',
       'Existência de população a jusante',
       'Data da Finalização da DCE', 'Motivo de Envio', 'RT/Declaração',
       'RT/Empreendimento','Impacto ambiental', 'Impacto sócio-econômico'], axis=1, inplace=True)
dfp_barragens.isna().sum()
print(dfp_barragens.dtypes)
verificacao_tipo(dfp_barragens, 'Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem',
                 float)
verificacao_valor_padrao(dfp_barragens, 'Empreendedor')
verificacao_valor_padrao(dfp_barragens, 'UF')
verificacao_valor_padrao(dfp_barragens, 'Município')
verificacao_valor_padrao(dfp_barragens, 'Categoria de Risco - CRI')
verificacao_valor_padrao(dfp_barragens, 'Nível de Emergência')
verificacao_valor_padrao(dfp_barragens, 'Tipo de Barragem de Mineração')
verificacao_valor_padrao(dfp_barragens, 'Vida útil prevista da Barragem (anos)')
verificacao_valor_padrao(dfp_barragens, 'Estrutura com o Objetivo de Contenção')
verificacao_valor_padrao(dfp_barragens, 'Minério principal presente no reservatório')
verificacao_valor_padrao(dfp_barragens, 'Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem')


# Tratamento do DataFrame

dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].replace(to_replace= 'o', value=np.nan, regex=True, inplace=True)
dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].replace(to_replace= 'Sim', value=np.nan, regex=True, inplace=True)
dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'] = dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].replace([None], np.nan)
dfp_barragens.fillna(np.nan)
dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'] = dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].str.replace('.', '')
dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'] = dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].str.replace(',', '.')
dfp_barragens['Vida útil prevista da Barragem (anos)'] = dfp_barragens['Vida útil prevista da Barragem (anos)'].str.replace(',', '.')
dfp_barragens.replace(to_replace='-', value=np.nan, inplace=True)
dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].replace(to_replace='1-100', value=100, inplace=True)
dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].replace(to_replace='101-500', value=500, inplace=True)
dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].replace(to_replace='1001-5000', value=5000, inplace=True)
dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].replace(to_replace='501-1000', value=1000, inplace=True)
dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].replace(to_replace='acima de 5001', value=5001, inplace=True)
dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'] = dfp_barragens['Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem'].astype(float)
dfp_barragens['Vida útil prevista da Barragem (anos)'] = dfp_barragens['Vida útil prevista da Barragem (anos)'].astype(float)

# Transformando DataFrame Dados População Spark em DataFrame Pandas

dfp_dados_populacao = dfs_dados_populacao.toPandas()

# Analisando dados
print(dfp_dados_populacao.dtypes)
verificacao_valor_padrao(dfp_dados_populacao, 'Ano')
verificacao_valor_padrao(dfp_dados_populacao, 'Esperança de Vida ao Nascer')
verificacao_valor_padrao(dfp_dados_populacao, 'Esperança de Vida ao Nascer - Homens')
verificacao_valor_padrao(dfp_dados_populacao, 'Esperança de Vida ao Nascer - Mulheres')
verificacao_valor_padrao(dfp_dados_populacao, 'Homens')
verificacao_valor_padrao(dfp_dados_populacao, 'Mulheres')
verificacao_valor_padrao(dfp_dados_populacao, 'Nascimentos')
verificacao_valor_padrao(dfp_dados_populacao, 'População total')
verificacao_valor_padrao(dfp_dados_populacao, 'Razão de Dependência')
verificacao_valor_padrao(dfp_dados_populacao, 'Razão de Dependência - Idosos 65 ou mais anos')
verificacao_valor_padrao(dfp_dados_populacao, 'Razão de Dependência - Jovens 0 a 14 anos')
verificacao_valor_padrao(dfp_dados_populacao, 'Taxa Bruta de Mortalidade')
verificacao_valor_padrao(dfp_dados_populacao, 'Taxa Bruta de Natalidade')
verificacao_valor_padrao(dfp_dados_populacao, 'Taxa de Crescimento Geométrico')
verificacao_valor_padrao(dfp_dados_populacao, 'Taxa de Fecundidade Total')
verificacao_valor_padrao(dfp_dados_populacao, 'Taxa de Mortalidade Infantil')
verificacao_valor_padrao(dfp_dados_populacao, 'Taxa de Mortalidade Infantil - Homens')
verificacao_valor_padrao(dfp_dados_populacao, 'Taxa de Mortalidade Infantil - Mulheres')
verificacao_valor_padrao(dfp_dados_populacao, 'Índice de Envelhecimento')
verificacao_valor_padrao(dfp_dados_populacao, 'Óbitos')
verificacao_valor_padrao(dfp_dados_populacao, 'uf')

# # Tratamento
dfp_dados_populacao.drop(['_id'], axis=1, inplace=True)

# Fazendo as configurações necessárias:
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Transformando em DF spark:
dft_arrecadacao = spark.createDataFrame(dfp_arrecadacao)
dft_barragens = spark.createDataFrame(dfp_barragens)
dft_dados_populacao = spark.createDataFrame(dfp_dados_populacao)
dft_pib = spark.createDataFrame(dfp_pib)

# Fazendo a conexão com o mongo:
db_conexao_tratada = Conector_mongo('soulcode-mineracao', 'mongodb', 'tratados')

'''
# Enviando os dados tratados para o mongo:
db_conexao_tratada.inserir_mongo(dft_arrecadacao, 'arrecadacao')
db_conexao_tratada.inserir_mongo(dft_barragens, 'barragens')
db_conexao_tratada.inserir_mongo(dft_dados_populacao, 'dados_populacao')
db_conexao_tratada.inserir_mongo(dft_pib, 'pib')
'''

# Enviando os dados tratados para o bucket:
arrecadacao = Arquivo('arrecadacao.csv','tratados','soulcode-mineracao', dft_arrecadacao, 'csv')
barragens = Arquivo('barragens.csv', 'tratados', 'soulcode-mineracao', dft_barragens, 'csv')
dados_populacao = Arquivo('dados_populacao.csv', 'tratados', 'soulcode-mineracao', dft_dados_populacao, 'csv')
pib = Arquivo('pib.csv','tratados', 'soulcode-mineracao', dft_pib, 'csv')

arrecadacao.envia_arquivo()
barragens.envia_arquivo()
dados_populacao.envia_arquivo()
pib.envia_arquivo()
