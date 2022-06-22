# https://developers.facebook.com/docs/marketing-api/insights/parameters/v3.3

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business import adobjects
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adsinsights import AdsInsights
import pandas as pd
import logging
import configparser
import sys
import datetime as dt
import os
from datetime import datetime, date
import google
from google.cloud import bigquery
from google.oauth2 import service_account
import google.oauth2.credentials
from sendgrid.helpers.mail import *
from sendgrid import SendGridAPIClient
from google.auth import compute_engine


# Configuraçãoes API
app_id = 'xxx'
app_secret = 'yyy'
access_token = '123'

# Conexao com a API
FacebookAdsApi.init(app_id, app_secret, access_token)

# Empresa
ORIGEM_DADOS = 'FACEBOOK'

client_bq = bigquery.Client()


# Apaga os registros após o filtro
def del_rows(empresa, data_inicial, data_final, tabela, disciplina):
    if disciplina == '':
        query = 'DELETE FROM ' + tabela + ' WHERE DAT_REFERENCIA BETWEEN "' + str(data_inicial) \
                + '"' + ' AND ' + '"' + str(data_final) + '"' + ' AND NOM_ORIGEM_DADOS = ' + '"' + empresa + '"'
    else:
        query = 'DELETE FROM ' + tabela + ' WHERE DAT_REFERENCIA BETWEEN "' + str(data_inicial) \
                + '"' + ' AND ' + '"' + str(data_final) + '"' + ' AND NOM_ORIGEM_DADOS = ' + '"' + empresa + '"' \
                + ' AND NOM_DISCIPLINA = ' + '"' + disciplina + '"'
    query_job = client_bq.query(query)
    query_job.result()


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


# Send email on error
def send_email(error, from_name, to_name, origem_dados):
    message = Mail(
        from_email=from_name,
        to_emails=to_name,
        subject='Cliente : Gol - Erro na rotina ' + origem_dados,
        plain_text_content=PlainTextContent('\nCliente: Gol ' + '\n' +
                                            '\nRotina: ' + origem_dados + '\n' +
                                            '\nErro: ' + str(error) + '\n' +
                                            '\nData erro: ' + str(datetime.now())))
    try:
        sg = SendGridAPIClient(sendgrid_key)
        sg.send(message)
    except Exception as e:
        logging.error('Erro ao enviar o email - {}'.format(e))


# Criação do dataframe
def create_dataframe():
    columns = ['date', 'adname', 'impressions', 'spend', 'device_platform']
    df = pd.DataFrame(columns=columns, dtype='str')
    return df


# Grab insight info for all ads in the adaccount
def get_api(df_api, accounts):
    tempaccount = accounts

    # ads = tempaccount.get_insights(params={'date_preset': 'yesterday',
    ads = tempaccount.get_insights(params={'date_preset': 'last_7d',
                                           'level': 'ad',
                                           'time_increment': 1,
                                           'limit': '900000',
                                           'breakdowns': ['device_platform']},
                                   fields=[AdsInsights.Field.ad_name,
                                           AdsInsights.Field.impressions,
                                           AdsInsights.Field.date_start,
                                           AdsInsights.Field.spend])

    for ad in ads:
        date = ""
        adname = ""
        impressions = ""
        spend = ""
        device_platform = ""

        # Set values from insight data
        if 'ad_name' in ad:
            adname = ad[AdsInsights.Field.ad_name]
        if 'impressions' in ad:
            impressions = ad[AdsInsights.Field.impressions]
        if 'spend' in ad:
            spend = ad[AdsInsights.Field.spend]
        # if 'device_platform' in ad:
        #     device_platform = ad[AdsInsights.Field.device_platform]
        if 'device_platform' in ad:
            device_platform = ad['device_platform']
        if 'date_start' in ad:
            date = ad[AdsInsights.Field.date_start]

        new_row = {'date': date, "adname": adname, 'impressions': impressions, 'spend': spend,
                   'device_platform': device_platform}
        df_api = df_api.append(new_row, ignore_index=True)

    return df_api


def concat_df(df1, df2):
    df = pd.concat([df1, df2], axis=1)

    # Converte os tipos dos dados
    df['spend'] = df['spend'].astype(float)
    df['impressions'] = df['impressions'].astype(int)

    # Copia os valores das colunas
    df['DAT_REFERENCIA'] = df['date']
    df['VAL_INVESTIMENTO'] = df['spend']
    df['NOM_SITE'] = ORIGEM_DADOS
    df['NOM_AD'] = df['adname']
    df['QTD_IMPRESSOES'] = df['impressions']
    df['NOM_ORIGEM_DADOS'] = 'PARCEIRO'
    df['NOM_DISCIPLINA'] = ORIGEM_DADOS

    # Quebra a string Placement Name
    for index, row in df.iterrows():
        # if row['adname'].count('_') >= 11:
        data_ad = row['adname'].split("_", 11)
        df.at[index, 'TIP_DEVICE'] = row['device_platform'].upper()
        df.at[index, 'NOM_ANUNCIANTE'] = (data_ad[0] + '_' + data_ad[1]).upper()
        df.at[index, 'NOM_CAMPANHA'] = data_ad[0] + '_' + data_ad[1]
        df.at[index, 'NOM_INICIATIVA'] = data_ad[4]
        df.at[index, 'TIP_CANAL'] = data_ad[5]
        df.at[index, 'TIP_COMPRA'] = data_ad[6]
        df.at[index, 'TIP_ESTRATEGIA'] = data_ad[7]
        df.at[index, 'NOM_CRIATIVO'] = data_ad[9]
        df.at[index, 'NOM_SEGMENTACAO'] = data_ad[10]
        df.at[index, 'TIP_FORMATO'] = data_ad[8]
        df.at[index, 'NOM_MIDIA'] = data_ad[3]
        df.at[index, 'NOM_ORIGEM'] = data_ad[2]

    # Apaga as colunas
    df.drop(['date', 'adname', 'impressions', 'spend', 'device_platform'], axis=1, inplace=True)
    return df


def main(self):
    # Variaveis de ambiente
    campanha = os.environ['campanha']
    project_id = os.environ['project_id']
    table = os.environ['table']
    diretorio = os.environ['diretorio']
    funcao_dispositivo = os.environ['funcao_dispositivo']
    funcao_anunciante = os.environ['funcao_anunciante']
    from_name = os.environ['from_name']
    to_name = os.environ['to_name']
    global sendgrid_key
    sendgrid_key = os.environ['sendgrid_key']

    # Log
    logging.info('Inicio da rotina')
    inicio = dt.datetime.now()

    # Cria uma lista com as campanhas
    lst_campanhas = campanha.split(",")

    # Cria o dataframe vazio
    try:
        df_empty = create_dataframe()
    except Exception as e:
        logging.error('Erro ao criar o dataframe vazio - {}'.format(e))
        sys.exit(0)

    # Captura os dados da API
    try:
        for idx, campanha in enumerate(lst_campanhas):
            accounts = AdAccount(campanha)
            if idx == 0:
                df_facebook = get_api(df_empty, accounts)
            else:
                df_facebook = get_api(df_facebook, accounts)
    except Exception as e:
        send_email(e, from_name, to_name, ORIGEM_DADOS)
        logging.error('Erro ao capturar os dados da API - {}'.format(e))
        sys.exit(0)

    # Grava os dados da API e os parametros em um arquivo CSV no Storage
    try:
        # Dataframe original
        nome_arquivo = datetime.today().strftime('%Y%m%d') + '_facebook.csv'
        df_facebook.to_csv(diretorio + nome_arquivo, sep=',', index=False)
        # Parametros da API
        nome_arquivo_parametro = datetime.today().strftime('%Y%m%d') + '_facebook_parametros.csv'
        dict = {"campos": 'ad_name, impressions, spend ,date_start, device_platform',
                "date_preset": 'last_7d',
                "level": 'ad',
                "versao_api": '10.0',
                "campanha": campanha,
                "time_increment": '1',
                "data_hora_leitura": datetime.now()}
        df_param = pd.DataFrame(dict, index=[0])
        df_param.to_csv(diretorio + nome_arquivo_parametro, index=False)
    except Exception as e:
        logging.error('Erro ao gravar os dados da API em um CSV no storage - {}'.format(e))
        sys.exit(0)

    # Captura a minina/maxima data no dataframe
    try:
        df_facebook['date'] = pd.to_datetime(df_facebook['date'])
        dt_min_api = df_facebook['date'].min().date()
        dt_max_api = df_facebook['date'].max().date()
    except Exception as e:
        logging.error('Erro ao capturar a data minima/maxima do dataframe - {}'.format(e))
        sys.exit(0)

    # Valida se o dataframe não está vazio
    if not df_facebook.empty:

        # Apaga os registros após o filtro
        try:
            del_rows('PARCEIRO', dt_min_api, dt_max_api, table, ORIGEM_DADOS)
        except Exception as e:
            logging.error('Erro ao apagar os registros - {}'.format(e))
            sys.exit(0)

        # Cria o dataframe do GCP
        try:
            df_facebook_gfc = create_dataframe_gcp()
        except Exception as e:
            logging.error('Erro ao criar o dataframe GCP - {}'.format(e))
            sys.exit(0)

        # Concatena os dataframes
        try:
            df_concat = concat_df(df_facebook_gfc, df_facebook)
        except Exception as e:
            logging.error('Erro ao concatenar os dataframes - {}'.format(e))
            sys.exit(0)

        # Converte os tipos das colunas
        try:
            df_final = convert_columns_types(df_concat)
        except Exception as e:
            logging.error('Erro ao converter os tipos das colunas - {}'.format(e))
            sys.exit(0)

        # # Insere os dados no BigQuery
        # try:
        #     if not df_final.empty:
        #         insert_gfc(df_final, table, project_id)
        #     else:
        #         logging.info('Não existem registros para serem inseridos.')
        # except Exception as e:
        #     logging.error('Erro ao inserir os dados do dataframe no BigQuery - {}'.format(e))
        #     sys.exit(0)

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
            send_email(e, from_name, to_name, ORIGEM_DADOS)
            logging.error('Erro ao inserir os dados do dataframe no BigQuery - {}'.format(e))
            sys.exit(0)

        # Atualiza NOM_ANUNCIANTE e TIP_DEVICE
        try:
            query = 'UPDATE ' + table + \
                    ' SET TIP_DEVICE = ' + funcao_dispositivo + '(TIP_DEVICE), NOM_ANUNCIANTE = ' + \
                    funcao_anunciante + '(NOM_ANUNCIANTE)' + \
                    ' WHERE DAT_REFERENCIA BETWEEN "' + str(dt_min_api) + '"' + ' AND ' + '"' + str(dt_max_api) + '"' + \
                    ' AND NOM_ORIGEM_DADOS = ' + '"' + 'PARCEIRO' + '"' + ' AND NOM_DISCIPLINA = ' + '"' + ORIGEM_DADOS + '"'
            query_job = client_bq.query(query)
            query_job.result()
            logging.info('Update realizado com sucesso.')
        except Exception as e:
            send_email(e, from_name, to_name, ORIGEM_DADOS)
            logging.error('Erro ao realizar o update na base de dados - {}'.format(e))
            sys.exit(0)

    else:
        logging.info('Não existem registros para inserir.')

    # Grava as informações no arquivo de log
    fim = dt.datetime.now()
    tempo = fim - inicio
    logging.info('Tempo de execucao da rotina: ' + str(tempo))
    logging.info('Fim da rotina\n')
