from pyspark.sql import SparkSession


class Conector_mysql:
    def __init__(self, host, user, password, db):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
    
    def envia_mysql(self, dfs, table, coluna_particao):
        self.dfs = dfs
        self.table = table
        self.coluna_particao = coluna_particao
        self.dfs.write.format("jdbc")\
                .option('url', f'jdbc:mysql://{self.host}/{self.db}')\
                .option('driver', 'com.mysql.cj.jdbc.Driver')\
                .option("numPartitions", "10") \
                .option("fetchsize", "0") \
                .option("partitionColumn", self.coluna_particao)\
                .option("lowerBound", 0)\
                .option("upperBound", 4)\
                .option("user",self.user)\
                .option("password", self.password)\
                .option("dbtable", self.db + "." + self.table)\
                .mode("append").save()
        
              
spark = ( SparkSession.builder.master("local")
         .appName('job-2')
         .config('spark.jars.packages', 'mysql:mysql-connector-java:5.1.44')
         .getOrCreate()
         )

# Lendo as Dataframes do bucket:
# dfs_arrecadacao = spark.read.csv(path='gs://soulcode-mineracao/original/arrecadacao.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
# dfs_barragens = spark.read.csv(path='gs://soulcode-mineracao/original/barragens.csv', inferSchema=True, header=True, sep=',', encoding='latin1') 
# dfs_autuacao = spark.read.csv(path='gs://soulcode-mineracao/original/autuacao.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
# dfs_beneficiada = spark.read.csv(path='gs://soulcode-mineracao/original/beneficiada.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
# dfs_distribuicao = spark.read.csv(path='gs://soulcode-mineracao/original/distribuicao.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
# dfs_municipio = spark.read.csv(path='gs://soulcode-mineracao/original/municipio.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
# dfs_pib = spark.read.csv(path='gs://soulcode-mineracao/original/pib.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
# dfs_dados_populacao = spark.read.json(path='gs://soulcode-mineracao/original/dados_populacao.json')

dfs_arrecadacao = spark.read.parquet('gs://soulcode-mineracao/original/arrecadacao.parquet')
# dfs_barragens = spark.read.parquet(path='gs://soulcode-mineracao/original/barragens.parquet') 
dfs_autuacao = spark.read.parquet('gs://soulcode-mineracao/original/autuacao.parquet')
# dfs_beneficiada = spark.read.parquet(path='gs://soulcode-mineracao/original/beneficiada.parquet')
dfs_distribuicao = spark.read.parquet('gs://soulcode-mineracao/original/distribuicao.parquet')
# dfs_municipio = spark.read.parquet(path='gs://soulcode-mineracao/original/municipio.parquet')
# dfs_pib = spark.read.parquet(path='gs://soulcode-mineracao/original/pib.parquet')
# dfs_dados_populacao = spark.read.parquet(path='gs://soulcode-mineracao/original/dados_populacao.parquet')

host = "34.72.50.43"
user = "root"
password = ">}Dzh.=}YhZ#(G>s" 
db = "original"



# Preparando as tabelas para inserção - renomeando colunas muito longas para a inserção:
# dfs_barragens = dfs_barragens.withColumnRenamed('A Barragem de Mineração possui outra estrutura de mineração interna selante de reservatório','Barragem possui estrutura interna selante')\
#     .withColumnRenamed('Esta Back Up Dam está operando pós rompimento da barragem de mineração','Esta Back Up Dam está operando pós rompimento da barragem')\
#     .withColumnRenamed('A Back Up Dam está dentro da Área do Processo ANM ou da Área de Servidão','A BUD está em Área do Processo ANM ou da Área de Servidão')\
#     .withColumnRenamed('Existe documento que ateste a segurança estrutural e a capacidade para contenção de rejeitos da Back Up Dam com ART','Possui documentação de segurança com ART')\
#     .withColumnRenamed('Estrutura organizacional e qualificação técnica dos profissionais na equipe de Segurança da Barragem','Profissionais na equipe de Segurança da Barragem')\
#     .withColumnRenamed('As cópias físicas do PAEBM foram entregues para as Prefeituras e Defesas Civis municipais e estaduais','Cópias físicas do PAEBM entregues as Prefeituras e Defesas Civis')\
#     .withColumnRenamed('A Back Up Dam garante a redução da área da mancha de inundação à jusante','BUD garante redução da área da mancha de inundação à jusante')\
#     .withColumnRenamed('A Barragem de Mineração está dentro da Área do Processo ANM ou da Área de Servidão','Está dentro da Área do Processo ANM ou da Área de Servidão')\
#     .withColumnRenamed('Manuais de Procedimentos para Inspeções de Segurança e Monitoramento','Manuais para Inspeções de Segurança e Monitoramento')\
#     .withColumnRenamed('PAE - Plano de Ação Emergencial (quando exigido pelo órgão fiscalizador)','PAE - Plano de Ação Emergencial')\
#     .withColumnRenamed('Relatórios de inspeção e monitoramento da instrumentação e de Análise de Segurança','Relatórios da instrumentação e de Análise de Segurança')\
#     .withColumnRenamed('Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem','N pessoas afetadas a jusante em caso de rompimento da barragem')

# dfs_beneficiada = dfs_beneficiada.withColumnRenamed('Quantidade Transferência para Transformação / Utilização / Consumo','Qtd Trans para Transformação / Utilização / Consumo')\
#     .withColumnRenamed('Unidade de Medida - Transferência para Transformação / Utilização / Consumo','Unidade - Transf Transformação / Utilização / Consumo')\
#     .withColumnRenamed('Valor Transferência para Transformação / Utilização / Consumo (R$)','Valor Transf Transformação / Utilização / Consumo (R$)')

print('Carregando as dataframes...')
# Enviando para a instância do mysql:
conector = Conector_mysql(host, user, password, db)
conector.envia_mysql(dfs_autuacao, 'autuacao', "NúmeroAuto")
# conector.envia_mysql(dfs_barragens, 'barragens')
# conector.envia_mysql(dfs_beneficiada, 'beneficiada')
# conector.envia_mysql(dfs_dados_populacao, 'dados_populacao')
# conector.envia_mysql(dfs_distribuicao, 'distribuicao')
# conector.envia_mysql(dfs_municipio, 'municipio')
# conector.envia_mysql(dfs_pib, 'pib')
#conector.envia_mysql(dfs_arrecadacao, 'arrecadacao')
