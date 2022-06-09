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
        elif self.tipo =='parquet':
          self.dfs.coalesce(1).write.parquet(f'gs://{self.bucket_name}/deletar/{self.nome}')
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
# arrecadacao = Arquivo('arrecadacao.csv','original','soulcode-mineracao', dfs_arrecadacao, 'csv')
# autuacao = Arquivo('autuacao.csv', 'original', 'soulcode-mineracao', dfs_autuacao, 'csv')
# barragens = Arquivo('barragens.csv', 'original', 'soulcode-mineracao', dfs_barragens, 'csv')
# beneficiada = Arquivo('beneficiada.csv','original', 'soulcode-mineracao', dfs_beneficiada, 'csv')
# dados_populacao = Arquivo('dados_populacao.json', 'original', 'soulcode-mineracao', dfs_dados_populacao, 'json')
# distribuicao = Arquivo('distribuicao.csv', 'original', 'soulcode-mineracao', dfs_distribuicao, 'csv')
# municipio = Arquivo('municipio.csv','original', 'soulcode-mineracao', dfs_municipio, 'csv')
# pib = Arquivo('pib.csv','original', 'soulcode-mineracao', dfs_pib, 'csv')

# barragem_cols = ['A Back Up Dam está dentro da Área do Processo ANM ou da Área de Servidão',
#  'A Back Up Dam garante a redução da área da mancha de inundação à jusante',
#  'A Back up Dam passou por auditoria de terceira parte',
#  'A Barragem armazena rejeitos/residuos que contenham Cianeto',
#  'A Barragem de Mineração está dentro da Área do Processo ANM ou da Área de Servidão',
#  'A Barragem de Mineração possui Manta Impermeabilizante',
#  'A Barragem de Mineração possui outra estrutura de mineração interna selante de reservatório',
#  'A barragem de mineração possui Back Up Dam',
#  'Altura Máxima do projeto da Back Up Dam (m)',
#  'Altura máxima atual (m)',
#  'Altura máxima do projeto licenciado (m)',
#  'As cópias físicas do PAEBM foram entregues para as Prefeituras e Defesas Civis municipais e estaduais',
#  'Barragem de mineração é alimentado por usina',
#  'CPF_CNPJ',
#  'Categoria de Risco - CRI',
#  'Classe',
#  'Comprimento atual da crista (m)',
#  'Comprimento da Crista do projeto da Back Up Dam (m)',
#  'Comprimento da crista do projeto (m)',
#  'Confiabilidade das estruturas extravasora',
#  'Dano Potencial Associado - DPA',
#  'Data da Finalização da DCE',
#  'Data da última Vistoria de Inspeção Regular',
#  'Deformações e recalque',
#  'Descarga Máxima do vertedouro da Dack Up Dam (m³/seg)',
#  'Descarga máxima do vertedouro (m³/seg)',
#  'Desde',
#  'Desde (Back Up Dam)',
#  'Deteriorização dos taludes / paramentos',
#  'Documentação de projeto',
#  'Empreendedor',
#  'Esta Back Up Dam está operando pós rompimento da barragem de mineração',
#  'Estrutura com o Objetivo de Contenção',
#  'Estrutura organizacional e qualificação técnica dos profissionais na equipe de Segurança da Barragem',
#  'Existe documento que ateste a segurança estrutural e a capacidade para contenção de rejeitos da Back Up Dam com ART',
#  'Existe manual de operação da Back Up Dam',
#  'Existência de população a jusante',
#  'ID',
#  'Impacto ambiental',
#  'Impacto sócio-econômico',
#  'Inserido na PNSB',
#  'Latitude',
#  'Latitude (Back Up Dam)',
#  'Longitude',
#  'Longitude (Back Up Dam)',
#  'Manuais de Procedimentos para Inspeções de Segurança e Monitoramento',
#  'Minério principal presente no reservatório',
#  'Motivo de Envio',
#  'Município',
#  'Município (Back Up Dam)',
#  'Método construtivo da Back Up Dam',
#  'Método construtivo da barragem',
#  'Necessita de PAEBM',
#  'Nome',
#  'Nome da Back Up Dam',
#  'Nível de Emergência',
#  'Número de pessoas possivelmente afetadas a jusante em caso de rompimento da barragem',
#  'Outras substâncias minerais presentes no reservatório',
#  'PAE - Plano de Ação Emergencial (quando exigido pelo órgão fiscalizador)',
#  'Percolação',
#  'Posicionamento',
#  'Posicionamento (Back Up Dam)',
#  'Previsão de término de construção da Back Up Dam',
#  'Processo de beneficiamento',
#  'Processos associados (Back Up Dam)',
#  'Produtos químicos utilizados',
#  'Quantidade Diques Internos',
#  'Quantidade Diques Selantes',
#  'RT/Declaração',
#  'RT/Empreendimento',
#  'Relatórios de inspeção e monitoramento da instrumentação e de Análise de Segurança',
#  'Situação Operacional',
#  'Situação operacional da Back Up Dam',
#  'Status da DCE Atual',
#  'Teor (%) do minério principal inserido no rejeito',
#  'Tipo de Back Up Dam quanto ao material de construção',
#  'Tipo de Barragem de Mineração',
#  'Tipo de alteamento',
#  'Tipo de auscultação',
#  'Tipo de auscultação da Back Up Dam',
#  'Tipo de barragem quanto ao material de construção',
#  'Tipo de fundação',
#  'Tipo de fundação da Back Up Dam',
#  'UF',
#  'UF (Back Up Dam)',
#  'Usinas',
#  'Vazão de projeto',
#  'Vazão de projeto da Back Up Dam',
#  'Vida útil prevista da Back Up Dam (Anos)',
#  'Vida útil prevista da Barragem (anos)',
#  'Volume atual do Reservatório (m³)',
#  'Volume de projeto licenciado do Reservatório (m³)',
#  'Volume do projeto da Back Up Dam (m³)',
#  'Área do reservatório (m²)']

barragem_cols = dfs_barragens.columns

for coluna in barragem_cols:
      coluna_nova = coluna.replace(" ", "_")
      coluna_nova = coluna_nova.replace("(", "")
      coluna_nova = coluna_nova.replace(")", "")
      coluna_nova = coluna_nova.replace("/", "ou")
      dfs_barragens = dfs_barragens.withColumnRenamed(coluna, coluna_nova)
      
beneficiada_cols = dfs_beneficiada.columns
for coluna in beneficiada_cols:
    coluna_nova = coluna.replace(" ", "_")
    coluna_nova = coluna_nova.replace("(", "")
    coluna_nova = coluna_nova.replace(")", "")
    coluna_nova = coluna_nova.replace("/", "ou")
    dfs_beneficiada = dfs_beneficiada.withColumnRenamed(coluna, coluna_nova)

municipio_cols = dfs_municipio.columns
for coluna in municipio_cols:
    coluna_nova = coluna.replace(" ", "_")
    coluna_nova = coluna_nova.replace(".", "")
    dfs_municipio = dfs_municipio.withColumnRenamed(coluna, coluna_nova)
    
arrecadacao = Arquivo('arrecadacao.parquet','original','soulcode-mineracao', dfs_arrecadacao, 'parquet')
autuacao = Arquivo('autuacao.parquet', 'original', 'soulcode-mineracao', dfs_autuacao, 'parquet')
barragens = Arquivo('barragens.parquet', 'original', 'soulcode-mineracao', dfs_barragens, 'parquet')
beneficiada = Arquivo('beneficiada.parquet','original', 'soulcode-mineracao', dfs_beneficiada, 'parquet')
dados_populacao = Arquivo('dados_populacao.parquet', 'original', 'soulcode-mineracao', dfs_dados_populacao, 'parquet')
distribuicao = Arquivo('distribuicao.parquet', 'original', 'soulcode-mineracao', dfs_distribuicao, 'parquet')
municipio = Arquivo('municipio.parquet','original', 'soulcode-mineracao', dfs_municipio, 'parquet')
pib = Arquivo('pib.parquet','original', 'soulcode-mineracao', dfs_pib, 'parquet')

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