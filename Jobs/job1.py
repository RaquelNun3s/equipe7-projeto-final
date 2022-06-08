from pyspark.sql import SparkSession
from pyspark import SQLContext, SparkFiles
import pyspark.sql.functions as F
from google.cloud import storage
import pandas as pd
#from modules.arquivo import Arquivo

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
          self.dfs.coalesce(1).write.json(f'gs://{self.bucket_name}/deletar/{self.nome}')
        else:
          self.dfs.coalesce(1).write.option("header", True).option("encoding", "latin1").save(f'gs://{self.bucket_name}/deletar/{self.nome}', format=self.tipo)

        # Renomeando do arquivo para o nome desejado - Pegando o blob antigo:
        blobs = storage_client.list_blobs(self.bucket_name)
        for blob in blobs:
            blob_name = str(blob)
            list_blob = blob_name.split(',')
            blob_path = list_blob[1]
            if f"deletar/{self.nome}/part" in blob_path:
                blob_antigo = blob
                blob_antigo_nome = blob_path

        # Movendo o arquivo para a pasta desejada:
        blob_copy = bucket.copy_blob(blob_antigo, bucket, f'{self.pasta}/{self.nome}')

        blob_antigo_nome = blob_antigo_nome.split(" ")
        blob_antigo_nome = blob_antigo_nome[1]

        # Deletando a pasta com os arquivos anteriores:
        bucket.delete_blob(blob_antigo_nome)
# Conectando com a SparkSession
spark = ( SparkSession.builder
                        .master("local")
                        .appName("job-1")
                        .config("spark.ui.port", "4050")
                        .config("spark.jars", 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar')
                        .getOrCreate() 
        )

spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# Puxando os arquivos dos seus locais de origem e criando as:
dfs_arrecadacao = spark.read.csv(path='gs://mineracao2/arrecadacao.csv', inferSchema=True, header=True, sep=';', encoding='latin1')
dfs_barragens = spark.read.csv(path='gs://mineracao2/Barragens.csv', inferSchema=True, header=True, sep=';', encoding='latin1') 
dfs_autuacao = spark.read.csv(path='gs://mineracao2/autuacao.csv', inferSchema=True, header=True, sep=';', encoding='latin1')
dfs_beneficiada = spark.read.csv(path='gs://mineracao2/beneficiada.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
dfs_distribuicao = spark.read.csv(path='gs://mineracao2/distribuicao.csv', inferSchema=True, header=True, sep=';', encoding='latin1')
dfs_municipio = spark.read.csv(path='gs://mineracao2/municipio.csv', inferSchema=True, header=True, sep=';', encoding='latin1')
dfs_pib = spark.read.csv(path='gs://mineracao2/pib.csv', inferSchema=True, header=True, sep=',', encoding='latin1')


url = 'https://raw.githubusercontent.com/JoaoHenrique132/dados_populacao_IBGE/main/dados_populacao_IBGE_json/'
# spark.sparkContext.addFile(url + 'IBGE_AC.json')
# dfs_dados_populacao = spark.read.json(SparkFiles.get('IBGE_AC.json'))

# dfs_dados_populacao = spark.read.json("hdfs:///mydata/IBGE_AC.json", encoding='latin1', )
# dfs_dados_populacao = dfs_dados_populacao.withColumn('uf', F.lit('AC'))

df_dados_populacao = pd.read_json(url + 'IBGE_AC.json', lines=True)
dfs_dados_populacao = spark.createDataFrame(df_dados_populacao)
dfs_dados_populacao = dfs_dados_populacao.withColumn('uf', F.lit('AC'))

lista = ['IBGE_AL.json',
          'IBGE_AM.json',
          'IBGE_AP.json',
          'IBGE_BA.json',
          'IBGE_CE.json',
          'IBGE_DF.json',
          'IBGE_ES.json',
          'IBGE_GO.json',
          'IBGE_MA.json',
          'IBGE_MG.json',
          'IBGE_MS.json',
          'IBGE_MT.json',
          'IBGE_PA.json',
          'IBGE_PB.json',
          'IBGE_PE.json',
          'IBGE_PI.json',
          'IBGE_PR.json',
          'IBGE_RJ.json',
          'IBGE_RN.json',
          'IBGE_RO.json',
          'IBGE_RR.json',
          'IBGE_RS.json',
          'IBGE_SC.json',
          'IBGE_SE.json',
          'IBGE_SP.json',
          'IBGE_TO.json']

# for filename in lista:
#   spark.sparkContext.addFile('https://raw.githubusercontent.com/JoaoHenrique132/dados_populacao_IBGE/main/dados_populacao_IBGE_json/' + filename, recursive=True)
# for filename in lista:
#   df = spark.read.json(SparkFiles.get(filename))
#   df = df.withColumn('UF', F.lit(filename[5:7]))
#   dfs_dados_populacao = dfs_dados_populacao.union(df)
  
# for filename in lista:
#   df = spark.read.json("hdfs:///mydata/" + filename)
#   df = df.withColumn('UF', F.lit(filename[5:7]))
#   dfs_dados_populacao = dfs_dados_populacao.union(df)
  
for filename in lista:
  df = pd.read_json(url + filename, lines=True)
  dfs = spark.createDataFrame(df)
  dfs = dfs.withColumn('UF', F.lit(filename[5:7]))
  dfs_dados_populacao = dfs_dados_populacao.union(dfs)
  
# Criando os objetos Arquivos:
arrecadacao = Arquivo('arrecadacao.csv','original','soulcode-mineracao', dfs_arrecadacao, 'csv')
autuacao = Arquivo('autuacao.csv', 'original', 'soulcode-mineracao', dfs_autuacao, 'csv')
barragens = Arquivo('barragens.csv', 'original', 'soulcode-mineracao', dfs_barragens, 'csv')
beneficiada = Arquivo('beneficiada.csv','original', 'soulcode-mineracao', dfs_beneficiada, 'csv')
dados_populacao = Arquivo('dados_populacao.json', 'original', 'soulcode-mineracao', dfs_dados_populacao, 'json')
distribuicao = Arquivo('distribuicao.csv', 'original', 'soulcode-mineracao', dfs_distribuicao, 'csv')
municipio = Arquivo('municipio.csv','original', 'soulcode-mineracao', dfs_municipio, 'csv')
pib = Arquivo('pib.csv','original', 'soulcode-mineracao', dfs_pib, 'csv')



# Enviando todos os arquivos para o bucket:
arrecadacao.envia_arquivo()
autuacao.envia_arquivo()
barragens.envia_arquivo()
beneficiada.envia_arquivo()
try:
  dados_populacao.envia_arquivo()
except Exception:
  pass
distribuicao.envia_arquivo()
municipio.envia_arquivo()
pib.envia_arquivo()