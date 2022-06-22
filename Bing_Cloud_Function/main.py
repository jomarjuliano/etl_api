import pandas as pd
import logging
import sys
import datetime as dt
import os
from datetime import datetime, date
from google.cloud import storage
from google.cloud import bigquery
import google.auth
import openpyxl
import warnings
import xlrd

warnings.filterwarnings("ignore")
warnings.simplefilter("always")

ORIGEM_DADOS = 'BING_LP'

credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/bigquery",
    ]
)

# Construct a BigQuery client object.
client_bq = bigquery.Client(credentials=credentials, project=project)


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


# Apaga os registros após o filtro
def del_rows(empresa, data_inicial, data_final, tabela, disciplina):
    if disciplina == '':
        query = 'DELETE FROM ' + tabela + ' WHERE DAT_REFERENCIA BETWEEN "' + str(data_inicial) \
                + '"' + ' AND ' + '"' + str(data_final) + '"' + ' AND NOM_ORIGEM_DADOS = ' + '"' + empresa + '"'
    else:
        query = 'DELETE FROM ' + tabela + ' WHERE DAT_REFERENCIA BETWEEN "' + str(data_inicial) \
                + '"' + ' AND ' + '"' + str(data_final) + '"' + ' AND NOM_ORIGEM_DADOS = ' + '"' + empresa + '"' \
                + ' AND NOM_DISCIPLINA IN ('"'BING_LP_BRAND'"', '"'BING_LP_NON-BRAND'"') '
                # + ' AND NOM_DISCIPLINA = ' + '"' + disciplina + '"'
    query_job = client_bq.query(query)
    query_job.result()


# Concatena os dataframes
def concat_df(df1, df2):
    df = pd.concat([df1, df2], axis=1)

    # Renomeia as colunas
    df = df.rename(columns={'URL Final': 'URL_Final'})

    # Copia os valores das colunas
    df['DAT_REFERENCIA'] = df['Date']
    df['VAL_INVESTIMENTO'] = df['Gastos']
    df['NOM_ORIGEM_DADOS'] = 'PARCEIRO'
    df['NOM_SITE'] = ORIGEM_DADOS
    df['NOM_CAMPANHA'] = df['Nome_Campanha']
    df['NOM_AD'] = df['Nome_Campanha'] + '_' + df['Grupo_Anuncio']
    df['NOM_DISCIPLINA'] = ORIGEM_DADOS
    df['QTD_CLIQUES'] = df['Cliques']
    df['QTD_IMPRESSOES'] = df['Visualizações']
    df['NOM_URL'] = df['URL_Final']

    # Quebra a string NOM_AD
    for index, row in df.iterrows():
        data_ad = row['NOM_AD'].split("_", 10)
        if 'branding' in row['NOM_CAMPANHA']:
            df.at[index, 'NOM_DISCIPLINA'] = 'BING_LP_BRAND'
        else:
            df.at[index, 'NOM_DISCIPLINA'] = 'BING_LP_NON-BRAND'
        df.at[index, 'TIP_DEVICE'] = row['Dispositivo'].upper()
        df.at[index, 'NOM_ANUNCIANTE'] = (data_ad[1]).upper()
        df.at[index, 'NOM_CAMPANHA'] = data_ad[1]
        df.at[index, 'NOM_INICIATIVA'] = data_ad[3]
        df.at[index, 'TIP_CANAL'] = data_ad[7]
        df.at[index, 'TIP_COMPRA'] = data_ad[6]
        df.at[index, 'TIP_ESTRATEGIA'] = data_ad[4]
        df.at[index, 'NOM_CRIATIVO'] = data_ad[5]
        df.at[index, 'NOM_SEGMENTACAO'] = data_ad[9]
        df.at[index, 'TIP_FORMATO'] = data_ad[8]
        df.at[index, 'NOM_MIDIA'] = data_ad[3]
        df.at[index, 'NOM_ORIGEM'] = data_ad[2]

    # Apaga as colunas
    df.drop(['Date', 'Nome_Campanha', 'Tipo_Anuncio', 'Grupo_Anuncio', 'Gastos', 'Dispositivo', 'Visualizações',
             'Cliques', 'URL_Final'], axis=1, inplace=True)
    return df


def main(self):
    global table_iniciativa
    # Variaveis de ambiente
    table = os.environ['table']
    funcao_dispositivo = os.environ['funcao_dispositivo']
    funcao_anunciante = os.environ['funcao_anunciante']
    table_iniciativa = os.environ['table_iniciativa']

    # Storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('arquivos_etl')

    # Log
    logging.info('Inicio da rotina')
    inicio = dt.datetime.now()
    
    # Percorre os arquivos que estão na pasta "Importados"
    blobs = bucket.list_blobs(prefix='importados/bing/', delimiter=None)
    
    # Percorre os arquivos no bucket
    for blob in blobs:

        # Verifica se existe algum arquivo
        if blob.name[16:] != '':

            # Valida se o nomme do arquivo é válido
            if verify_file_name(os.path.splitext(blob.name[16:])[0], 'bing'):

                # Leitura do Excel
                try:
                    # df_bing = pd.read_excel('gs://arquivos_etl/' + blob.name, dtype={'URL Final': str}, engine='openpyxl')
                    df_bing = pd.read_excel('gs://arquivos_etl/' + blob.name, engine='openpyxl')
                    # df_bing = pd.read_excel('gs://arquivos_etl/' + blob.name, skiprows=9, skipfooter=3,
                    #                         dtype={'URL Final': str})
                    df_bing = df_bing[9:]
                    df_bing.columns = ['Date', 'Nome_Campanha', 'Tipo_Anuncio', 'Grupo_Anuncio', 'Gastos',
                                       'Dispositivo', 'Visualizações', 'Cliques', 'URL Final']
                    # df_bing.columns = ['Date', 'Nome_Campanha', 'Tipo_Anuncio', 'Grupo_Anuncio', 'Gastos',
                    #                    'Dispositivo', 'Visualizações', 'Cliques']
                    df_bing = df_bing[:-3]
                except Exception as e:
                    logging.error('Erro ao carregar o Excel no dataframe - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/bing/' + blob.name[16:])
                    sys.exit(0)

                # Captura a minina/maxima data no dataframe do Excel
                try:
                    dt_min_excel = df_bing['Date'].min().date()
                    dt_max_excel = df_bing['Date'].max().date()
                except Exception as e:
                    logging.error('Erro ao capturar a data minim/maxima do Excel - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/bing/' + blob.name[16:])
                    sys.exit(0)

                # Apaga os registros após o filtro
                try:
                    del_rows('PARCEIRO', dt_min_excel, dt_max_excel, table, ORIGEM_DADOS)
                except Exception as e:
                    logging.error('Erro ao apagar os registros - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/bing/' + blob.name[16:])
                    sys.exit(0)

                # Cria o dataframe do GCP
                try:
                    df_bing_gfc = create_dataframe_gcp()
                except Exception as e:
                    logging.error('Erro ao criar o dataframe GCP - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/bing/' + blob.name[16:])
                    sys.exit(0)
            
                # Concatena os dataframes
                try:
                    df_concat = concat_df(df_bing_gfc, df_bing)
                except Exception as e:
                    logging.error('Erro ao concatenar os dataframes - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/bing/' + blob.name[16:])
                    sys.exit(0)

                # Converte os tipos das colunas
                try:
                    df_final = convert_columns_types(df_concat)
                except Exception as e:
                    logging.error('Erro ao converter os tipos das colunas - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/bing/' + blob.name[16:])
                    sys.exit(0)

                # Insere os dados no BigQuery
                try:
                    if not df_final.empty:
                        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                        job = client_bq.load_table_from_dataframe(df_final, table, job_config=job_config)
                        job.result()
                        logging.info('Registros inseridos com sucesso.')
                    else:
                        logging.info('Não existem registros para serem inseridos.')
                except Exception as e:
                    logging.error('Erro ao inserir os dados do dataframe no BigQuery - {}'.format(e))
                    sys.exit(0)

                # Move o arquivo para a pasta processados
                try:
                    bucket.rename_blob(blob, 'processados/bing/' + blob.name[16:])
                except Exception as e:
                    logging.error('Erro ao mover o arquivo para a pasta processados - {}'.format(e))
                    sys.exit(0)

                # Atualiza NOM_ANUNCIANTE e TIP_DEVICE
                try:
                    query = 'UPDATE ' + table + \
                            ' SET TIP_DEVICE = ' + funcao_dispositivo + '(TIP_DEVICE), NOM_ANUNCIANTE = ' + \
                            funcao_anunciante + '(NOM_ANUNCIANTE)' + \
                            ' WHERE DAT_REFERENCIA BETWEEN "' + str(dt_min_excel) + '"' + ' AND ' + '"' + str(
                        dt_max_excel) + '"' + \
                            ' AND NOM_ORIGEM_DADOS = ' + '"' + 'PARCEIRO' + '"' + ' AND NOM_DISCIPLINA = ' + '"' + ORIGEM_DADOS + '"'
                    query_job = client_bq.query(query)
                    query_job.result()
                    logging.info('Update realizado com sucesso.')
                except Exception as e:
                    logging.error('Erro ao realizar o update na base de dados - {}'.format(e))
                    sys.exit(0)

                # Atualiza NOM_INICIATIVA
                try:
                    query = 'UPDATE ' + table + ' MD ' \
                            'SET MD.NOM_INICIATIVA = UPPER(TI.INICIATIVA) ' \
                            'FROM ' + table_iniciativa + ' TI ' \
                            'WHERE MD.NOM_URL = UPPER(TI.URL) '
                    query_job = client_bq.query(query)
                    query_job.result()
                    logging.info('Update iniciativa realizado com sucesso.')
                except Exception as e:
                    logging.error('Erro ao realizar o update na base de dados - {}'.format(e))
                    sys.exit(0)

            else:
                logging.error('Arquivo com nomenclatura inválida - {}'.format(os.path.splitext(blob.name[16:])[0]))
                # bucket.rename_blob(blob, 'erros/bing/' + blob.name[16:])
        else:
            logging.info('Não existem arquivos na pasta.')

    # Grava as informações no arquivo de log
    fim = dt.datetime.now()
    tempo = fim - inicio
    logging.info('Tempo de execucao da rotina: ' + str(tempo))
    logging.info('Fim da rotina\n')
