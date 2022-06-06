from pyspark.sql import SparkSession
from pyspark import SparkFiles
import pyspark.sql.functions as F
from google.cloud import storage
from arquivo import Arquivo

# Conectando com a SparkSession
spark = ( SparkSession.builder
                        .master("local")
                        .appName("job-1")
                        .config("spark.ui.port", "4050")
                        .config("spark.jars", 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar')
                        .getOrCreate() 
        )

spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# Puxando os arquivos dos seus locais de origem e criando as dfs:
dfs_arrecadacao = spark.read.csv(path='gs://mineracao2/arrecadacao.csv', inferSchema=True, header=True, sep=';', encoding='latin1')
dfs_barragens = spark.read.csv(path='gs://mineracao2/Barragens.csv', inferSchema=True, header=True, sep=';', encoding='latin1') 
dfs_autuacao = spark.read.csv(path='gs://mineracao2/autuacao.csv', inferSchema=True, header=True, sep=';', encoding='latin1')
dfs_beneficiada = spark.read.csv(path='gs://mineracao2/beneficiada.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
dfs_distribuicao = spark.read.csv(path='gs://mineracao2/distribuicao.csv', inferSchema=True, header=True, sep=';', encoding='latin1')
dfs_municipio = spark.read.csv(path='gs://mineracao2/municipio.csv', inferSchema=True, header=True, sep=';', encoding='latin1')
dfs_pib = spark.read.csv(path='gs://mineracao2/pib.csv', inferSchema=True, header=True, sep=',', encoding='latin1')

spark.sparkContext.addFile('https://raw.githubusercontent.com/JoaoHenrique132/dados_populacao_IBGE/main/dados_populacao_IBGE_json/IBGE_AC.json')
dfs_dados_populacao = spark.read.json(SparkFiles.get('IBGE_AC.json'))
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

for filename in lista:
  spark.sparkContext.addFile('https://raw.githubusercontent.com/JoaoHenrique132/dados_populacao_IBGE/main/dados_populacao_IBGE_json/' + filename)
  
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
dados_populacao.envia_arquivo()
distribuicao.envia_arquivo()
municipio.envia_arquivo()
pib.envia_arquivo()