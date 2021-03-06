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

# Preparando as tabelas para inser????o - renomeando colunas muito longas para a inser????o:
dfs_barragens = dfs_barragens.withColumnRenamed('A_Barragem_de_Minera????o_possui_outra_estrutura_de_minera????o_interna_selante_de_reservat??rio','Barragem_possui_estrutura_interna_selante')\
    .withColumnRenamed('Esta_Back_Up_Dam_est??_operando_p??s_rompimento_da_barragem_de_minera????o','Esta_Back_Up_Dam_est??_operando_p??s_rompimento_da_barragem')\
    .withColumnRenamed('A_Back_Up_Dam_est??_dentro_da_??rea_do_Processo_ANM_ou_da_??rea_de_Servid??o','A_BUD_est??_em_??rea_do_Processo_ANM_ou_da_??rea_de_Servid??o')\
    .withColumnRenamed('Existe_documento_que_ateste_a_seguran??a_estrutural_e_a_capacidade_para_conten????o_de_rejeitos_da_Back_Up_Dam_com_ART','Possui_documenta????o_de_seguran??a_com_ART')\
    .withColumnRenamed('Estrutura_organizacional_e_qualifica????o_t??cnica_dos_profissionais_na_equipe_de_Seguran??a_da_Barragem','Profissionais_na_equipe_de_Seguran??a_da_Barragem')\
    .withColumnRenamed('As_c??pias_f??sicas_do_PAEBM_foram_entregues_para_as_Prefeituras_e_Defesas_Civis_municipais_e_estaduais','C??pias_f??sicas_do_PAEBM_entregues_as_Prefeituras_e_Defesas_Civis')\
    .withColumnRenamed('A_Back_Up_Dam_garante_a_redu????o_da_??rea_da_mancha_de_inunda????o_??_jusante','BUD_garante_redu????o_da_??rea_da_mancha_de_inunda????o_??_jusante')\
    .withColumnRenamed('A_Barragem_de_Minera????o_est??_dentro_da_??rea_do_Processo_ANM_ou_da_??rea_de_Servid??o','Est??_dentro_da_??rea_do_Processo_ANM_ou_da_??rea_de_Servid??o')\
    .withColumnRenamed('Manuais_de_Procedimentos_para_Inspe????es_de_Seguran??a_e_Monitoramento','Manuais_para_Inspe????es_de_Seguran??a_e_Monitoramento')\
    .withColumnRenamed('PAE_-_Plano_de_A????o_Emergencial_quando_exigido_pelo_??rg??o_fiscalizador','PAE_Plano_de_A????o_Emergencial')\
    .withColumnRenamed('Relat??rios_de_inspe????o_e_monitoramento_da_instrumenta????o_e_de_An??lise_de_Seguran??a','Relat??rios_da_instrumenta????o_e_de_An??lise_de_Seguran??a')\
    .withColumnRenamed('N??mero_de_pessoas_possivelmente_afetadas_a_jusante_em_caso_de_rompimento_da_barragem','N_pessoas_afetadas_a_jusante_em_caso_de_rompimento_da_barragem')

dfs_beneficiada = dfs_beneficiada.withColumnRenamed('Quantidade_Transfer??ncia_para_Transforma????o_ou_Utiliza????o_ou_Consumo','Qtd_Trans_para_Transforma????o_ou_Utiliza????o_ou_Consumo')\
    .withColumnRenamed('Unidade_de_Medida_-_Transfer??ncia_para_Transforma????o_ou_Utiliza????o_ou_Consumo','Unidade_Transf_Transforma????o_ou_Utiliza????o_ou_Consumo')\
    .withColumnRenamed('Valor_Transfer??ncia_para_Transforma????o_ou_Utiliza????o_ou_Consumo_R$','Valor_Transf_Transforma????o_ou_Utiliza????o_ou_Consumo_R$')

print('Carregando as dataframes...')
host = "34.72.50.43"
user = "root"
password = ">}Dzh.=}YhZ#(G>s" 
db = "original"
# Enviando para a inst??ncia do mysql:
conector = Conector_mysql(host, user, password, db)
#conector.envia_mysql(dfs_autuacao, 'autuacao')
# conector.envia_mysql(dfs_barragens, 'barragens')
# conector.envia_mysql(dfs_beneficiada, 'beneficiada')
# conector.envia_mysql(dfs_dados_populacao, 'dados_populacao')
# conector.envia_mysql(dfs_municipio, 'municipio')
# conector.envia_mysql(dfs_pib, 'pib')
conector.envia_mysql(dfs_distribuicao, 'distribuicao')
conector.envia_mysql(dfs_arrecadacao, 'arrecadacao')
