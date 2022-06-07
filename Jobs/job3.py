import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

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

# Criando e configurando a a SparkSession:
conf =( pyspark.SparkConf()
               .set("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
               .set("spark.jars", 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar')
               .setMaster("local")
               .setAppName("job-3"))
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')

# Puxando os arquivos do bucket do projeto para dataframes:
dfs_arrecadacao = spark.read.csv(path='gs://soulcode-mineracao/original/arrecadacao.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
dfs_barragens = spark.read.csv(path='gs://soulcode-mineracao/original/barragens.csv', inferSchema=True, header=True, sep=',', encoding='latin1') 
dfs_autuacao = spark.read.csv(path='gs://soulcode-mineracao/original/autuacao.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
dfs_beneficiada = spark.read.csv(path='gs://soulcode-mineracao/original/beneficiada.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
dfs_distribuicao = spark.read.csv(path='gs://soulcode-mineracao/original/distribuicao.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
dfs_municipio = spark.read.csv(path='gs://soulcode-mineracao/original/municipio.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
dfs_pib = spark.read.csv(path='gs://soulcode-mineracao/original/pib.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
dfs_dados_populacao = spark.read.json(path='gs://soulcode-mineracao/original/dados_populacao.json')

# Ajustando o nome das colunas da dfs_municipio:
dfs_municipio = dfs_municipio.withColumnRenamed('COD. UF', 'COD_UF')
dfs_municipio = dfs_municipio.withColumnRenamed('COD. MUNIC', 'COD_MUNIC')

# Instanciando o objetos:
db_conectada = Conector_mongo('soulcode-mineracao', 'mongodb', 'original')

# Chamando os métodos para enviar os dados para o mongo:
db_conectada.inserir_mongo(dfs_arrecadacao, 'arrecadacao')
db_conectada.inserir_mongo(dfs_autuacao, 'autuacao')
db_conectada.inserir_mongo(dfs_barragens, 'barragens')
db_conectada.inserir_mongo(dfs_beneficiada, 'beneficiada')
db_conectada.inserir_mongo(dfs_dados_populacao, 'dados_populacao')
db_conectada.inserir_mongo(dfs_distribuicao, 'distribuicao')
db_conectada.inserir_mongo(dfs_municipio, 'municipio')
db_conectada.inserir_mongo(dfs_pib, 'pib')