import pandas as pd
from datetime import datetime


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