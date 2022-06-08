# Importando modulos
from time import time
import pandas as pd
from sqlalchemy import create_engine

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

# Instanciando a classe
conexao = Interface_MySQL("root", ">}Dzh.=}YhZ#(G>s", "34.72.50.43", "original")

# Estabelecendo conexão através do método create_engine
cnx = conexao.create_engine()

# Instanciando as dataframes através do bucket
try:
    df_arrecadacao = pd.read_csv('gs://soulcode-mineracao/original/arrecadacao.csv', sep=',', encoding='latin-1')
    df_autuacao = pd.read_csv('gs://soulcode-mineracao/original/autuacao.csv', sep=',', encoding='latin-1')
    df_barragens = pd.read_csv('gs://soulcode-mineracao/original/barragens.csv', sep=',', encoding='latin-1')
    df_beneficiada = pd.read_csv('gs://soulcode-mineracao/original/beneficiada.csv', sep=',', encoding='latin-1')
    df_dados_populacao = pd.read_json('gs://soulcode-mineracao/original/dados_populacao.json', lines=True)
    df_distribuicao = pd.read_csv('gs://soulcode-mineracao/original/distribuicao.csv', sep=',', encoding='latin-1')
    df_municipio = pd.read_csv('gs://soulcode-mineracao/original/municipio.csv', sep=',', encoding='latin-1')
    df_pib = pd.read_csv('gs://soulcode-mineracao/original/pib.csv', sep=',', encoding='latin-1')
except Exception as e:
    print("Error: ". str(e))
            
# Carregando as dataframes no MySQL
try:
    print('Carregando as dataframes...')
    inicio = time()
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
    df_dados_populacao.to_sql('dados_populacao',con=cnx, if_exists='append')
    df_distribuicao.to_sql('distribuicao',con=cnx, if_exists='append')
    df_municipio.to_sql('municipio',con=cnx, if_exists='append')
    df_pib.to_sql('pib',con=cnx, if_exists='append')
    fim = time.time()
    print(f"Carregamento concluido em {(float((fim - inicio)%60))} seg.")
except Exception as e:
    print("Error: ". str(e))