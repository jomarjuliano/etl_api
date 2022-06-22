
import logging
import pandas as pd
import datetime as dt
import configparser
import sys
import os
from datetime import date, timedelta, datetime
import json
import requests
from utils import *

empresa = 'VERIZON'


def get_device(device):
    if device in ('DESKTOP', 'TABLET'):
        tipo = 'DESKTOP'
    elif device in ('SMARTPHONE', 'UNKNOWN'):
        tipo = 'MOBILE'
    else:
        tipo = 'CROSS_DEVICE'
    return tipo


# Gera o token
def get_token():
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Basic'
    }

    data = {
      'grant_type': 'refresh_token',
      'redirect_uri': 'oob',
      'refresh_token': '123XXX'
    }

    response_post = requests.post('https://api.login.yahoo.com/oauth2/get_token', headers=headers, data=data)
    data_json = response_post.json()
    return data_json['access_token']


# Cria o Relatório como dicionário
def create_report(token, data_inicial, data_final, diretorio, advertiser):

    advertiser_id = advertiser

    url = "https://api.gemini.yahoo.com/v3/rest/reports/custom"

    payload = json.dumps({"cube": "performance_stats", "fields":
        [{"field": "Day"},
         {"field": "Hour"},
         {"field": "Device Type"},
         {"field": "Spend"},
         {"field": "Ad Group Name"},
         {"field": "Video Views"},
         {"field": "Video 25% Complete"},
         {"field": "Video 50% Complete"},
         {"field": "Video 75% Complete"},
         {"field": "Video 100% Complete"}],
                          "filters": [{"field": "Advertiser ID", "operator": "=", "value": advertiser_id},
                                      {"field": "Day", "operator": "between", "from": data_inicial, "to": data_final}]})

    headers = {
        'Content-Type': "application/json",
        'Authorization': "Bearer " + token,
        'User-Agent': "PostmanRuntime/7.15.2",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Postman-Token': "YYYYYY",
        'Host': "api.gemini.yahoo.com",
        'Accept-Encoding': "gzip, deflate",
        'Content-Length': "615",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }

    # Cria o job
    response = requests.request("POST", url, data=payload, headers=headers)
    data_json = response.json()

    # Captura o relatório pelo jobid
    url_get = url + '/' + data_json["response"]['jobId']

    querystring = {"advertiserId": advertiser_id}

    headers = {
        'Authorization': "Bearer " + token,
        'cache-control': "no-cache",
        'Postman-Token': "YYYYYYYYYYY"
        }

    while True:
        response_post = requests.request("GET", url_get, headers=headers, params=querystring)
        data_json = response_post.json()
        try:
            if data_json["response"]["status"] == 'completed':
                customerreport = data_json["response"]['jobResponse']
                df = pd.read_csv(customerreport)
                df = df[df['Spend'] > 0]
                # Grava o dataframe original no bucket
                nome_arquivo = datetime.today().strftime('%Y%m%d') + '_verizon.csv'
                df.to_csv(diretorio + nome_arquivo, index=None, header=True)
                break
        except Exception as e:
            logging.error('Falha ao capturar os dados da API - {}'.format(e))
            sys.exit(0)

    # Grava os dados da API e os parametros em um arquivo CSV no Storage
    try:
        # Parametros da API
        nome_arquivo_parametro = datetime.today().strftime('%Y%m%d') + '_verizon_parametros.csv'
        dict = {"campos": 'Day, Hour , Device Type, Spend, Ad Group Name,Video Views, Video 25% Complete, '
                          'Video 50% Complete, Video 75% Complete, Video 100% Complete',
                "from": 'performance_stats',
                "data_inicial": data_inicial,
                "data_final": data_final,
                "versao_api": '1',
                "Advertiser ID": advertiser_id,
                "data_hora_leitura": datetime.now()}
        df_param = pd.DataFrame(dict, index=[0])
        df_param.to_csv(diretorio + nome_arquivo_parametro, index=False)
    except Exception as e:
        logging.error('Erro ao gravar os dados da API em um CSV no storage - {}'.format(e))
        sys.exit(0)

    return df


def concat_df(df1, df2):
    df = pd.concat([df1, df2], axis=1)

    # Filtra 'alp' no Ad Group Name
    df = df[df['Ad Group Name'].str.startswith("alp")]

    # Renomeia as colunas
    df = df.rename(columns={'Video 25% Complete': 'Video_25'})
    df = df.rename(columns={'Video 50% Complete': 'Video_50'})
    df = df.rename(columns={'Video 75% Complete': 'Video_75'})
    df = df.rename(columns={'Video 100% Complete': 'Video_100'})
    df = df.rename(columns={'Video Views': 'Video_Views'})
    df = df.rename(columns={'Ad Group Name': 'Ad_Group_Name'})
    df = df.rename(columns={'Device Type': 'Device_Type'})

    # Converte os tipos dos dados
    df['Video_Views'] = df['Video_Views'].astype(int)
    df['Video_25'] = df['Video_25'].astype(int)
    df['Video_50'] = df['Video_50'].astype(int)
    df['Video_75'] = df['Video_75'].astype(int)
    df['Video_100'] = df['Video_100'].astype(int)
    df['Spend'] = df['Spend'].astype(float)

    # Copia os valores das colunas
    df['DAT_REFERENCIA'] = df['Day']
    df['NOM_ORIGEM_DADOS'] = 'PARCEIRO'
    df['NOM_SITE'] = empresa
    df['QTD_VIEWS_PLAY'] = df['Video_Views']
    df['QTD_VIEWS_25'] = df['Video_25']
    df['QTD_VIEWS_50'] = df['Video_50']
    df['QTD_VIEWS_75'] = df['Video_75']
    df['QTD_VIEWS_100'] = df['Video_100']
    df['VAL_INVESTIMENTO'] = df['Spend']
    df['NOM_AD'] = df['Ad_Group_Name']
    df['NOM_DISCIPLINA'] = empresa

    # Filtra os ads nulos
    df = df[~df.Ad_Group_Name.isnull()]

    # Quebra a string Placement Name
    for index, row in df.iterrows():
        df.at[index, 'HOR_REFERENCIA'] = dt.datetime.strptime(str(row['Hour']).zfill(2) + ':00', "%H:%M")
        df.at[index, 'TIP_DEVICE'] = get_device(row['Device_Type'].upper())
        data_ad = row['Ad_Group_Name'].split("_", 11)
        anunciante = get_anunciante(data_ad[1].upper())
        df.at[index, 'NOM_ANUNCIANTE'] = anunciante
        df.at[index, 'NOM_CAMPANHA'] = data_ad[1]
        df.at[index, 'NOM_ORIGEM'] = data_ad[2]
        df.at[index, 'NOM_MIDIA'] = data_ad[3]
        df.at[index, 'NOM_INICIATIVA'] = data_ad[4]
        df.at[index, 'TIP_CANAL'] = data_ad[5]
        df.at[index, 'TIP_COMPRA'] = data_ad[6]
        df.at[index, 'TIP_ESTRATEGIA'] = data_ad[7]
        df.at[index, 'TIP_FORMATO'] = data_ad[8]
        df.at[index, 'NOM_CRIATIVO'] = data_ad[9]
        df.at[index, 'NOM_SEGMENTACAO'] = data_ad[10]
        df.at[index, 'NOM_PILAR'] = get_pilar(row['Ad_Group_Name'].upper(), anunciante.upper(), 'VERIZON',
                                              data_ad[4].upper())

    # Apaga as colunas
    df.drop(['Day', 'Hour', 'Spend', 'Ad_Group_Name', 'Video_25', 'Video_50', 'Video_75', 'Video_100',
             'Video_Views', 'Device_Type'], axis=1, inplace=True)

    return df


def main(self):

    # Variaveis de ambiente
    project_id = os.environ['project_id']
    table = os.environ['table']
    advertiser = os.environ['advertiser']
    diretorio = os.environ['diretorio']

    logging.info('Inicio da rotina')
    inicio = dt.datetime.now()

    # Gera o token
    try:
        token = get_token()
    except Exception as e:
        logging.error('Erro ao gerar o token - {}'.format(e))
        sys.exit(0)

    # Cria o relatório
    try:
        end_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
        start_date = datetime.strftime(datetime.now() - timedelta(8), '%Y-%m-%d')
        df_verizon = create_report(token, start_date, end_date, diretorio, advertiser)
    except Exception as e:
        logging.error('Erro ao criar o relatório - {}'.format(e))
        sys.exit(0)

    if not df_verizon.empty:

        # Captura a minina/maxima data no dataframe
        try:
            df_verizon['Day'] = pd.to_datetime(df_verizon['Day'])
            dt_min_api = df_verizon['Day'].min().date()
            dt_max_api = df_verizon['Day'].max().date()
        except Exception as e:
            logging.error('Erro ao capturar a data minima/maxima do Dataframe - {}'.format(e))
            sys.exit(0)

        # Apaga os registros após o filtro
        try:
            if check_table_exist(project_id, table):
                del_rows('PARCEIRO', project_id, dt_min_api, dt_max_api, table, empresa)
        except Exception as e:
            logging.error('Erro ao apagar os registros - {}'.format(e))
            sys.exit(0)

        # Cria o dataframe do GCP
        try:
            df_verizon_gfc = create_dataframe_gcp()
        except Exception as e:
            logging.error('Erro ao criar o dataframe GCP - {}'.format(e))
            sys.exit(0)

        # Concatena os dataframes
        try:
            df_concat = concat_df(df_verizon_gfc, df_verizon)
        except Exception as e:
            logging.error('Erro ao concatenar os dataframes - {}'.format(e))
            sys.exit(0)

        # Converte os tipos das colunas
        try:
            df_final = convert_columns_types(df_concat)
        except Exception as e:
            logging.error('Erro ao converter os tipos das colunas - {}'.format(e))
            sys.exit(0)

        # Insere os dados no BigQuery
        try:
            if not df_final.empty:
                insert_gfc(df_final, table, project_id)
            else:
                logging.info('Não existem registros para serem inseridos.')
        except Exception as e:
            logging.error('Erro ao inserir os dados do dataframe no BigQuery - {}'.format(e))
            sys.exit(0)

    else:
        logging.info('Relatório vazio.')

    # Grava as informações no arquivo de log
    fim = dt.datetime.now()
    tempo = fim - inicio
    logging.info('Tempo de execucao da rotina: ' + str(tempo))
    logging.info('Fim da rotina\n')
