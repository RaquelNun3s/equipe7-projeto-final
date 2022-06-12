# Importando modulos
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession

# Criando a classe para conexão
class Interface_MySQL:
    
    def __init__(self, user, password, host, dbname):
        try:
            self.user = user
            self.password = password
            self.host = host
            self.dbname = dbname
        except Exception as e:
            print("Error: ", str(e))
        
    def create_engine(self):
        try:
            cnx = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.dbname}')
            return cnx
        except Exception as e:
            print("Error: ", str(e))
# Conectando com a sparkSession
spark = ( SparkSession.builder
                        .master("local")
                        .appName("job-1")
                        .config("spark.ui.port", "4050")
                        .config("spark.jars", 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar')
                        .getOrCreate() 
        )

spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# Instanciando a classe
conexao = Interface_MySQL("root", ">}Dzh.=}YhZ#(G>s", "34.72.50.43", "original")

# Estabelecendo conexão através do método create_engine
cnx = conexao.create_engine()

# Instanciando as dataframes através do bucket
try:
    print("Lendo as DFs pelo spark...")
    # Lendo as dataframes utilizando o spark
    dfs_arrecadacao = spark.read.csv(path='gs://soulcode-mineracao/original/arrecadacao.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
    dfs_autuacao = spark.read.csv(path='gs://soulcode-mineracao/original/autuacao.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
    dfs_barragens = spark.read.csv(path='gs://soulcode-mineracao/original/barragens.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
    dfs_beneficiada = spark.read.csv(path='gs://soulcode-mineracao/original/beneficiada.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
    dfs_dados_populacao = spark.read.json('gs://soulcode-mineracao/original/dados_populacao.json')
    dfs_distribuicao = spark.read.csv(path='gs://soulcode-mineracao/original/distribuicao.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
    dfs_municipio = spark.read.csv(path='gs://soulcode-mineracao/original/municipio.csv', inferSchema=True, header=True, sep=',', encoding='latin1')
    dfs_pib = spark.read.csv(path='gs://soulcode-mineracao/original/pib.csv', inferSchema=True, header=True, sep=',', encoding='latin1')

    # Convertendo as dataframes para pandas:
    print("Convertendo as DFs para Pandas...")
    df_arrecadacao = dfs_arrecadacao.toPandas()
    df_autuacao = dfs_autuacao.toPandas()
    df_barragens = dfs_barragens.toPandas()
    df_beneficiada = dfs_beneficiada.toPandas()
    df_dados_populacao = dfs_dados_populacao.toPandas()
    df_distribuicao = dfs_distribuicao.toPandas()
    df_municipio = dfs_municipio.toPandas()
    df_pib = dfs_pib.toPandas()
except Exception as e:
    print("Error: ". str(e))
            
# Carregando as dataframes no MySQL
try:
    print('Carregando as dataframes...')
    df_arrecadacao.to_sql('arrecadacao',con=cnx, if_exists='append')
    df_autuacao.to_sql('autuacao',con=cnx, if_exists='append')
    df_barragens.rename(columns= {
    'A Barragem de Mineração possui outra estrutura de mineração interna selante de reservatório':'Barragem possui estrutura interna selante',
    'Esta Back Up Dam está operando pós rompimento da barragem de mineração':'Esta Back Up Dam está operando pós rompimento da barragem',
    'A Back Up Dam está dentro da Área do Processo ANM ou da Área de Servidão':'A BUD está em Área do Processo ANM ou da Área de Servidão',
    'Existe documento que ateste a segurança estrutural e a capacidade para contenção de rejeitos da Back Up Dam com ART':'Possui documentação de segurança com ART',
    'Estrutura organizacional e qualificação técnica dos profissionais na equipe de Segurança da Barragem':'Profissionais na equipe de Segurança da Barragem',
    'As cópias físicas do PAEBM foram entregues para as Prefeituras e Defesas Civis municipais e estaduais':'Cópias físicas do PAEBM entregues as Prefeituras e Defesas Civis',
    'A Back Up Dam garante a redução da área da mancha de inundação à jusante':'BUD garante redução da área da mancha de inundação à jusante',
    'A Barragem de Mineração está dentro da Área do Processo ANM ou da Área de Servidão':'Está dentro da Área do Processo ANM ou da Área de Servidão',
    'Manuais de Procedimentos para Inspeções de Segurança e Monitoramento':'Manuais para Inspeções de Segurança e Monitoramento',
    'PAE - Plano de Ação Emergencial (quando exigido pelo órgão fiscalizador)':'PAE - Plano de Ação Emergencial',
    'Relatórios de inspeção e monitoramento da instrumentação e de Análise de Segurança':'Relatórios da instrumentação e de Análise de Segurança',
    'Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem':'N pessoas afetadas a jusante em caso de rompimento da barragem'
}, inplace = True)
    df_barragens.to_sql('barragens',con=cnx, if_exists='append')
    df_beneficiada.rename(columns= {
    'Quantidade Transferência para Transformação / Utilização / Consumo':'Qtd Trans para Transformação / Utilização / Consumo',
    'Unidade de Medida - Transferência para Transformação / Utilização / Consumo':'Unidade - Transf Transformação / Utilização / Consumo',
    'Valor Transferência para Transformação / Utilização / Consumo (R$)':'Valor Transf Transformação / Utilização / Consumo (R$)'
}, inplace = True)
    df_beneficiada.to_sql('beneficiada', con=cnx, if_exists='append')
    df_dados_populacao.to_sql('dados_populacao',con=cnx, if_exists='append')
    df_distribuicao.to_sql('distribuicao',con=cnx, if_exists='append')
    df_municipio.to_sql('municipio',con=cnx, if_exists='append')
    df_pib.to_sql('pib',con=cnx, if_exists='append')
    
except Exception as e:
    print("Error: ". str(e))