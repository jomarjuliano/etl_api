import pandas as pd
import sys
import datetime as dt
import os
import pandas_gbq
from datetime import date, timedelta, datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


# Leitura do Excel com Sheetname
def read_excel_sheet(filename, sheetname):
    df = pd.read_excel(filename, index_col=None, header=0, sheet_name=sheetname)
    return df


# Leitura do Excel
def read_excel(filename):
    df = pd.read_excel(filename, index_col=None, header=0)
    return df


# Leitura do CSV
def read_csv(filename, encode, delimiter):
    df = pd.read_csv(filename, encoding=encode, delimiter=delimiter)
    return df


# Criação do dataframe GCP
def create_dataframe_gcp():
    columns = ['NOM_ORIGEM_DADOS', 'DAT_REFERENCIA', 'NOM_ANUNCIANTE', 'NOM_SITE', 'NOM_AD', 'NOM_DISCIPLINA',
               'NOM_CAMPANHA', 'NOM_CAMPANHA_LP', 'NOM_ORIGEM', 'NOM_MIDIA', 'NOM_INICIATIVA', 'TIP_CANAL',
               'TIP_COMPRA', 'TIP_ESTRATEGIA', 'TIP_FORMATO', 'NOM_CRIATIVO', 'NOM_SEGMENTACAO', 'NOM_PILAR',
               'TIP_DEVICE', 'QTD_IMPRESSOES', 'QTD_CLIQUES', 'QTD_VISITAS', 'QTD_NOVOS_USUARIOS', 'QTD_REJEICOES',
               'QTD_TEMPO_SESSAO', 'QTD_PAGEVIEWS', 'QTD_TRECHOS', 'QTD_TRANSACOES', 'VAL_VENDA',
               'VAL_INVESTIMENTO', 'VAL_INVEST_DESEMBOLSADO', 'QTD_BUSCAS', 'QTD_VIEWS_PLAY', 'QTD_VIEWS_25',
               'QTD_VIEWS_50', 'QTD_VIEWS_75', 'QTD_VIEWS_100', 'VAL_GA_VENDAS', 'QTD_GA_TRECHOS',
               'QTD_GA_TRANSACOES', 'NOM_GRUPO_ANUNCIO', 'NOM_PALAVRA_CHAVE', 'HOR_REFERENCIA', 'NOM_REGIAO',
               'QTD_ALCANCE', 'QTD_IMPRESSOES_VISIVEIS', 'QTD_IMPRESSOES_MENSURAVEIS', 'NOM_DOMINIO_VEICULADO',
               'NOM_URL']
    global df
    df = pd.DataFrame(columns=columns, dtype='str')
    return df


# Converte od tipos das colunas
def convert_columns_types(df):
    # Preenche com 0 os campos INT e FLOAT
    for col in df.columns:
        if (col[:3]) == 'QTD':
            df[col].fillna(0, inplace=True)
        elif (col[:3]) == 'VAL':
            df[col].fillna(0.0, inplace=True)

    # Converte os tipos das colunas
    for col in df.columns:
        if (col[:3]) == 'QTD':
            df[col] = df[col].astype(int)
        elif (col[:3]) == 'VAL':
            df[col] = df[col].astype(float)
        elif (col[:3]) == 'DAT':
            # df[col] = pd.to_datetime(df[col], format='%m-%d-%Y')
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d')
        elif (col[:3]) == 'HOR':
            df[col] = pd.to_datetime(df[col], format='%H:%M')
    df['DAT_INSERCAO'] = dt.datetime.now()

    # Converte os campos texto em UPPER
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.upper()

    return df


# Insere os dados do dataframe no GCP
def insert_gfc(dataframe, tabela, projeto):
    # pandas_gbq.to_gbq(dataframe, tabela, project_id=projeto, if_exists='replace',chunksize = 10000)
    pandas_gbq.to_gbq(dataframe, tabela, project_id=projeto, if_exists='append',)


# Captura a máxima data de referência
def max_data(table, empresa, projeto):
    query = 'SELECT MAX(DAT_REFERENCIA) AS DATA FROM ' + table + ' WHERE NOM_ORIGEM_DADOS = ' + '"' + empresa + '"'
    df = pandas_gbq.read_gbq(query, project_id=projeto)
    data = df.loc[0, 'DATA']
    if not str(data) == 'NaT':
        filtro = data + dt.timedelta(days=-7)
    else:
        filtro = datetime.strptime('1900-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
    return filtro.date()


# Captura a máxima data de referência na tabela da ANAC
def max_data_anac(projeto):
    query = 'SELECT MAX(DAT_REFERENCIA) AS DATA FROM `Canal_Digital.TB_ANAC` '
    df_max = pandas_gbq.read_gbq(query, project_id=projeto)
    data = df_max.loc[0, 'DATA']
    if not str(data) == 'NaT':
        filtro = data
    else:
        filtro = datetime.strptime('1900-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
    return filtro.date()


# Apaga os registros após o filtro
def del_rows(empresa, projeto, data_inicial, data_final, tabela, disciplina):
    if disciplina == '':
        query = 'DELETE FROM ' + tabela + ' WHERE DAT_REFERENCIA BETWEEN "' + str(data_inicial) \
                + '"' + ' AND ' + '"' + str(data_final) + '"' + ' AND NOM_ORIGEM_DADOS = ' + '"' + empresa + '"'
    else:
        query = 'DELETE FROM ' + tabela + ' WHERE DAT_REFERENCIA BETWEEN "' + str(data_inicial) \
                + '"' + ' AND ' + '"' + str(data_final) + '"' + ' AND NOM_ORIGEM_DADOS = ' + '"' + empresa + '"' \
                + ' AND NOM_DISCIPLINA = ' + '"' + disciplina + '"'

    # pandas_gbq.read_gbq(query, project_id=projeto)
    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()


# Verifica se o nome do arquivo Excel está no padrao aaaammdd_empresa
def verify_file_name(nome_arquivo, empresa):
    data_validacao = nome_arquivo[:8]
    try:
        dt.datetime.strptime(data_validacao, '%Y%m%d')
    except Exception:
        return False
    nome_validacao = data_validacao + '_' + empresa
    if nome_validacao == nome_arquivo:
        return True
    else:
        return False


# Captura o Pilar
def get_pilar(ad, advertiser, disciplina, nom_iniciativa):
    if ('META-SEARCH' in ad) or ('METASAERCH' in ad) or ('CORE' in ad) or ('CPA' in ad):
        pilar = 'COMPARADOR'
    elif ('GOL_INTERNACIONAL' in advertiser) \
            and not ('META-SEARCH|METASAERCH|CORE|CPA' in ad):
        pilar = 'VAREJO_INTERNACIONAL'
    elif disciplina == 'GOOGLE_LP_BRANDING':
        pilar = 'TERMOS_DE_MARCA'
    elif ('RECEITAS-AUXILIARES' in nom_iniciativa) \
            and not ('META-SEARCH|METASAERCH|CORE|CPA' in ad) \
            and not ('GOL_INTERNACIONAL' in advertiser):
        pilar = 'RECEITAS_AUXILIARES'
    elif ('TEST-AND-LEARN' in nom_iniciativa) \
            and not ('META-SEARCH|METASAERCH|CORE|CPA' in ad) \
            and not ('GOL_INTERNACIONAL' in advertiser):
        pilar = 'TEST_AND_lEARN'
    elif ('AQUISICAO_' in ad) \
            and not ('META-SEARCH|METASAERCH|CORE|CPA' in ad) \
            and not ('GOL_INTERNACIONAL' in advertiser) \
            and not ('RECEITAS-AUXILIARES' in nom_iniciativa) \
            and not ('TEST-AND-LEARN' in nom_iniciativa):
        pilar = 'AQUISICAO'
    else:
        pilar = 'VAREJO_NACIONAL'
    return pilar


# Verifica se a tabela existe
def check_table_exist(project_id, table_id):
    bigquery_client = bigquery.Client(project=project_id)
    try:
        table = bigquery_client.get_table(table_id)
        if table:
            return True
    except NotFound as error:
        return False


# Captura o anunciante
def get_anunciante(campanha):
    # NOM_CAMPANHA contém varejo-nac: NOM_ANUNCIANTE = GOL;
    # NOM_CAMPANHA contém varejo-inter: NOM_ANUNCIANTE = GOL_INTERNACIONAL;
    # NOM_CAMPANHA contém marca: NOM_ANUNCIANTE = GOL_MARCA;
    # NOM_CAMPANHA contém receitas-auxiliares: NOM_ANUNCIANTE = GOL_RA;
    anunciante = ''
    if 'VAREJO-NAC' in campanha:
        anunciante = 'GOL'
    elif 'VAREJO-INTER' in campanha:
        anunciante = 'GOL_INTERNACIONAL'
    elif 'MARCA' in campanha:
        anunciante = 'GOL_MARCA'
    elif 'RECEITAS-AUXILIARES' in campanha:
        anunciante = 'GOL_RA'
    return anunciante
