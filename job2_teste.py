from pyspark.sql import SparkSession


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
        self.df = self.spark_conection.read.formtat("jdbc")\
            .option('url', f'jdbc:mysql://{self.host}/{self.db}')\
            .option('driver', 'com.mysql.cj.jdbc.Driver')\
            .option("user",self.user)\
            .option("password", self.password)\
            .option("dbtable", self.db + "." + self.table).load()
        return self.df
                    
spark = ( SparkSession.builder.master("local")
         .appName('job-2')
         .config('spark.jars.packages', 'mysql:mysql-connector-java:5.1.44')
         .getOrCreate()
         )

dfs_arrecadacao = spark.read.parquet('gs://soulcode-mineracao/original/arrecadacao.parquet')
dfs_barragens = spark.read.parquet('gs://soulcode-mineracao/original/barragens.parquet') 
dfs_autuacao = spark.read.parquet('gs://soulcode-mineracao/original/autuacao.parquet')
dfs_beneficiada = spark.read.parquet('gs://soulcode-mineracao/original/beneficiada.parquet')
dfs_distribuicao = spark.read.parquet('gs://soulcode-mineracao/original/distribuicao.parquet')
dfs_municipio = spark.read.parquet('gs://soulcode-mineracao/original/municipio.parquet')
dfs_pib = spark.read.parquet('gs://soulcode-mineracao/original/pib.parquet')
dfs_dados_populacao = spark.read.parquet('gs://soulcode-mineracao/original/dados_populacao.parquet')

# Preparando as tabelas para inserção - renomeando colunas muito longas para a inserção:
dfs_barragens = dfs_barragens.withColumnRenamed('A_Barragem_de_Mineração_possui_outra_estrutura_de_mineração_interna_selante_de_reservatório','Barragem_possui_estrutura_interna_selante')\
    .withColumnRenamed('Esta_Back_Up_Dam_está_operando_pós_rompimento_da_barragem_de_mineração','Esta_Back_Up_Dam_está_operando_pós_rompimento_da_barragem')\
    .withColumnRenamed('A_Back_Up_Dam_está_dentro_da_Área_do_Processo_ANM_ou_da_Área_de_Servidão','A_BUD_está_em_Área_do_Processo_ANM_ou_da_Área_de_Servidão')\
    .withColumnRenamed('Existe_documento_que_ateste_a_segurança_estrutural_e_a_capacidade_para_contenção_de_rejeitos_da_Back_Up_Dam_com_ART','Possui_documentação_de_segurança_com_ART')\
    .withColumnRenamed('Estrutura_organizacional_e_qualificação_técnica_dos_profissionais_na_equipe_de_Segurança_da_Barragem','Profissionais_na_equipe_de_Segurança_da_Barragem')\
    .withColumnRenamed('As_cópias_físicas_do_PAEBM_foram_entregues_para_as_Prefeituras_e_Defesas_Civis_municipais_e_estaduais','Cópias_físicas_do_PAEBM_entregues_as_Prefeituras_e_Defesas_Civis')\
    .withColumnRenamed('A_Back_Up_Dam_garante_a_redução_da_área_da_mancha_de_inundação_à_jusante','BUD_garante_redução_da_área_da_mancha_de_inundação_à_jusante')\
    .withColumnRenamed('A_Barragem_de_Mineração_está_dentro_da_Área_do_Processo_ANM_ou_da_Área_de_Servidão','Está_dentro_da_Área_do_Processo_ANM_ou_da_Área_de_Servidão')\
    .withColumnRenamed('Manuais_de_Procedimentos_para_Inspeções_de_Segurança_e_Monitoramento','Manuais_para_Inspeções_de_Segurança_e_Monitoramento')\
    .withColumnRenamed('PAE_-_Plano_de_Ação_Emergencial_quando_exigido_pelo_órgão_fiscalizador','PAE_Plano_de_Ação_Emergencial')\
    .withColumnRenamed('Relatórios_de_inspeção_e_monitoramento_da_instrumentação_e_de_Análise_de_Segurança','Relatórios_da_instrumentação_e_de_Análise_de_Segurança')\
    .withColumnRenamed('Número_de_pessoas_possivelmente_afetadas_a_jusante_em_caso_de_rompimento_da_barragem','N_pessoas_afetadas_a_jusante_em_caso_de_rompimento_da_barragem')

dfs_beneficiada = dfs_beneficiada.withColumnRenamed('Quantidade_Transferência_para_Transformação_ou_Utilização_ou_Consumo','Qtd_Trans_para_Transformação_ou_Utilização_ou_Consumo')\
    .withColumnRenamed('Unidade_de_Medida_-_Transferência_para_Transformação_ou_Utilização_ou_Consumo','Unidade_Transf_Transformação_ou_Utilização_ou_Consumo')\
    .withColumnRenamed('Valor_Transferência_para_Transformação_ou_Utilização_ou_Consumo_R$','Valor_Transf_Transformação_ou_Utilização_ou_Consumo_R$')

print('Carregando as dataframes...')
host = "34.72.50.43"
user = "root"
password = ">}Dzh.=}YhZ#(G>s" 
db = "original"
# Enviando para a instância do mysql:
conector = Conector_mysql(host, user, password, db)
#conector.envia_mysql(dfs_autuacao, 'autuacao')
# conector.envia_mysql(dfs_barragens, 'barragens')
# conector.envia_mysql(dfs_beneficiada, 'beneficiada')
# conector.envia_mysql(dfs_dados_populacao, 'dados_populacao')
# conector.envia_mysql(dfs_municipio, 'municipio')
# conector.envia_mysql(dfs_pib, 'pib')
conector.envia_mysql(dfs_distribuicao, 'distribuicao')
conector.envia_mysql(dfs_arrecadacao, 'arrecadacao')
