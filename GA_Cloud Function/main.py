# https://github.com/googleapis/google-api-python-client/issues/632 (socket timeout)
# https://github.com/MarkEdmondson1234/google-analytics-cloud-functions/blob/master/cloud-storage-to-ga/main.py
# https://code.markedmondson.me/automatic-google-analytics-data-imports-cloud-storage/


import datetime as dt
import sys
import pandas as pd
import logging
from utils import *
import os
from datetime import datetime
import google.oauth2.credentials
from googleapiclient import discovery
import socket


DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
API_NAME = 'analytics'
API_VERSION = 'v4'

# Tratamento Socket
socket.setdefaulttimeout(600)  # set timeout to 10 minutes


def setup(access_token, refresh_token, token_uri, client_id, client_secret):
    credentials = google.oauth2.credentials.Credentials(access_token,
                                                        refresh_token=refresh_token,
                                                        token_uri=token_uri,
                                                        client_id=client_id,
                                                        client_secret=client_secret)
    return discovery.build(API_NAME, API_VERSION, credentials=credentials, discoveryServiceUrl=DISCOVERY_URI,
                           cache_discovery=False)


def get_report(analytics, view_id):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={'reportRequests': [{'viewId': view_id,
                                  'pageSize': 100000,
                                  'dateRanges': [{'startDate': '2019-11-01', 'endDate': '2019-11-07'}],
                                  'metrics': [{'expression': 'ga:itemRevenue'},
                                              {'expression': 'ga:avgSessionDuration'},
                                              {'expression': 'ga:pageviews'},
                                              {'expression': 'ga:bounces'},
                                              {'expression': 'ga:transactions'},
                                              {'expression': 'ga:sessions'},
                                              {'expression': 'ga:newUsers'},
                                              {'expression': 'ga:itemQuantity'},
                                              {'expression': 'ga:metric1'}],
                                  "dimensions": [{"name": "ga:date"},
                                                 {"name": "ga:campaign"},
                                                 {"name": "ga:sourceMedium"},
                                                 {"name": "ga:keyword"},
                                                 {"name": "ga:adGroup"},
                                                 {"name": "ga:adContent"},
                                                 {"name": "ga:deviceCategory"}],
                                  "samplingLevel": "LARGE",
                                  "orderBys": [{"fieldName": "ga:date", "sortOrder": "DESCENDING"}]
                                  }],
              "useResourceQuotas": 'true',
              }).execute()


def transform_df(response):
    # Initialize results, in list format because two dataframes might return
    result_list = []

    # Initialize empty data container for the two dateranges (if there are two that is)
    data_csv = []
    data_csv2 = []

    # Initialize header rows
    header_row = []

    for report in response.get('reports', []):

        # Get column headers, metric headers, and dimension headers.
        columnHeader = report.get('columnHeader', {})
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        dimensionHeaders = columnHeader.get('dimensions', [])

        # Combine all of those headers into the header_row, which is in a list format
        for dheader in dimensionHeaders:
            header_row.append(dheader)
        for mheader in metricHeaders:
            header_row.append(mheader['name'])

        # Get data from each of the rows, and append them into a list
        rows = report.get('data', {}).get('rows', [])
        for row in rows:
            row_temp = []
            dimensions = row.get('dimensions', [])
            metrics = row.get('metrics', [])
            for d in dimensions:
                row_temp.append(d)
            for m in metrics[0]['values']:
                row_temp.append(m)
            data_csv.append(row_temp)

            # In case of a second date range, do the same thing for the second request
            if len(metrics) == 2:
                row_temp2 = []
                for d in dimensions:
                    row_temp2.append(d)
                for m in metrics[1]['values']:
                    row_temp2.append(m)
                data_csv2.append(row_temp2)

    # Putting those list formats into pandas dataframe, and append them into the final result
    result_df = pd.DataFrame(data_csv, columns=header_row)

    result_list.append(result_df)
    if data_csv2 != []:
        result_list.append(pd.DataFrame(data_csv2, columns=header_row))

    df = pd.DataFrame(result_list[0])

    # Renomeia as colunas
    cols = []
    for column in df.columns:
        cols.append(column[3:])
    df.columns = cols

    # print(df)

    return df


def classify_search(campanha, midia, conteudo):
    lst_google_lp = ['branding', 'smartads', 'gdn', 'vídeo', 'display', 'youtube']
    lst_google_lp_inter = ['smartads', 'gdn', 'vídeo', 'display', 'youtube']
    lst_google_lp_branding = ['Non-Branding', 'smartads', 'gdn', 'vídeo', 'display', 'youtube', 'inter']
    # lst_smart_ads = ['youtube', 'gdn', 'dbm', 'discovery-ads']
    # lst_gdn = ['smart', 'search', 'not set']

    # GOOGLE_LP_BRANDING
    if ('branding' in campanha) and \
            ('google / cpc' in midia) and not \
            (any(ele in campanha for ele in lst_google_lp_branding)):
        veiculo = 'GOOGLE_LP_BRANDING'

    # GOOGLE_LP
    elif ('google / cpc' in midia) and not (any(ele in campanha for ele in lst_google_lp)):
        veiculo = 'GOOGLE_LP'

    elif ('google / cpc' in midia) and not (any(ele in campanha for ele in lst_google_lp_inter)):
        veiculo = 'GOOGLE_LP'

    # GOOGLE_ADS_GALLERY
    elif ('google / cpc' in midia) and ('gallery' in campanha):
        veiculo = 'GOOGLE_ADS_GALLERY'

    # SMARTADS_GOOGLE
    elif (('smart-ads' in campanha) or ('smart-ads' in conteudo)) and \
            (('google / cpc' in midia) or ('google-adwords / display' in midia) or
             ('google-ads-smart-ads / display' in midia)):
        veiculo = 'SMARTADS_GOOGLE'

    # GOOGLE_GDN
    elif (('gdn' in campanha) or ('gdn' in conteudo)) and \
            (('google / cpc' in midia) or ('google-adwords / display' in midia) or
             ('google-ads-gdn / display' in midia) or ('google-gdn / display' in midia)):
        veiculo = 'GOOGLE_GDN'

    # GOOGLE_DISCOVERY_ADS
    elif (('discovery-ads' in campanha) or ('discovery-ads' in conteudo)) \
            and (('google / cpc' in midia) or ('google-ads-discovery-ads / display' in midia) or
                 ('google-adwords / display' in midia)):
        veiculo = 'GOOGLE_DISCOVERY_ADS'

    # BING_LP
    elif 'bing / cpc' in midia:
        veiculo = 'BING_LP'

    # Criteo
    elif 'criteo / display' in midia:
        veiculo = 'CRITEO'

    # Facebook
    elif 'facebook / social' in midia:
        veiculo = 'FACEBOOK'

    # Kayak Comparador
    # validar se o tipo de compra vai impactar no futuro
    elif (('kayak / comparador' in midia) or ('kayak / display' in midia) or ('kayak / core' in midia)) and \
            (('metasearch' in conteudo) or ('meta-search' in conteudo) or ('cpa' in conteudo) or ('core' in conteudo)):
        veiculo = 'KAYAK_META_SEARCH'

    # Kayak Display
    elif (('kayak / comparador' in midia) or ('kayak / display' in midia)) and \
            (('metasearch' not in conteudo) and ('meta-search' not in conteudo) and ('cpa' not in conteudo)):
        veiculo = 'KAYAK_DISPLAY'

    # Oath Brigthroll
    elif ('oath / display' in midia) or ('oath-brightroll / programatico' in midia):
        veiculo = 'OATH_BRIGTHROLL'

    # Oath NativeAds / Verizon Gemini
    elif ('oath' in midia) or ('oath-gemini / native' in midia) or ('verizon-gemini / native' in midia):
        veiculo = 'VERIZON_GEMINI'

    # Verizon DSP
    elif ('verizon-dsp / programatico' in midia) or ('verizon / display' in midia) or \
            ('verizon-dsp / display' in midia):
        veiculo = 'VERIZON_DSP'

    # Skyscanner Comparador
    # elif (('referral' not in campanha) and ('metasearch' not in campanha) and ('meta-search' not in campanha)) and\
    #         ('skyscanner / comparador' in midia) and \
    #         (('meta-search' not in conteudo) or ('metasearch' not in conteudo)):
    #     veiculo = 'SKYSCANNER_META_SEARCH'

    elif (('skyscanner / comparador' in midia) and
          (('meta-search' in conteudo) or ('metasearch' in conteudo) or ('core' in conteudo))):
        veiculo = 'SKYSCANNER_META_SEARCH'

    # Skyscanner Display
    # elif (('skyscanner / comparador' in midia) or ('skyscanner / display' in midia)) and \
    #         (('meta-search' not in conteudo) or ('metasearch' not in conteudo) or ('referral' not in conteudo)):
    #     veiculo = 'SKYSCANNER_DISPLAY'

    elif (('skyscanner / comparador' in midia) or ('skyscanner / display' in midia)) and \
            (('meta-search' not in conteudo) and ('metasearch' not in conteudo) and ('core' not in conteudo)):
        veiculo = 'SKYSCANNER_DISPLAY'

    # Taboola
    elif ('taboola / native' in midia) or ('taboola / display' in midia):
        veiculo = 'TABOOLA'

    # Viajala Display
    elif (('viajala / comparador' in midia) or ('viajala / display' in midia)) and ('core' not in conteudo):
        veiculo = 'VIAJALA_DISPLAY'

    # Viajala Comparador
    elif (('viajala / comparador' in midia) or ('viajala / core' in midia)) and ('core' in conteudo):
        veiculo = 'VIAJALA_META_SEARCH'

    # Voopter Comparador
    elif (('voopter / comparador' in midia) or ('voopter / core' in midia)) and \
            (('meta' in conteudo) or ('cpa' in conteudo) or ('core' in conteudo)):
        veiculo = 'VOOPTER_META_SEARCH'

    # Voopter Display
    elif (('voopter / comparador' in midia) or ('voopter / publi-editorial' in midia) or
          ('voopter / display' in midia)) and \
            (('meta' not in conteudo) and ('cpa' not in conteudo) and ('core' not in conteudo)):
        veiculo = 'VOOPTER_DISPLAY'

    # Youtube
    # elif ('youtube / video' in midia) or ('google-dv360-youtube / programatico' in midia) or \
    #         ('google-dv360 / video' in midia):
    #     veiculo = 'YOUTUBE'

    # UOL
    elif 'uol / display' in midia:
        veiculo = 'UOL'

    # Abril - Viagem e Turismo
    elif 'abril / display' in midia:
        veiculo = 'ABRIL'

    # Webedia - Tudo Gostoso
    elif 'webedia / display' in midia:
        veiculo = 'WEBEDIA'

    # Terra
    elif 'terra / display' in midia:
        veiculo = 'TERRA'

    # RTBHouse
    elif ('rtbhouse / native' in midia) or ('rtbhouse / display' in midia) or ('rtbhouse / programatico' in midia):
        veiculo = 'RTBHOUSE'

    # Turismocity
    elif 'turismocity' in midia:
        veiculo = 'TURSIMO_CITY_META_SEARCH'

    # Seedtag
    elif 'seedtag / display' in midia:
        veiculo = 'SEEDTAG'

    # Outbrain
    elif ('outbrain / native' in midia) or ('outbrain / social' in midia):
        veiculo = 'OUTBRAIN'

    # XTMedia
    elif ('xtmedia / display' in midia) or ('xtmedia / video' in midia):
        veiculo = 'XTMEDIA'

    # DBM ou Google DV360
    elif (('google / programatico' in midia) or ('google-dv360 / programatico' in midia) or
          ('google-dv360 / native' in midia) or ('google-dv360 / display' in midia)):
        veiculo = 'GOOGLE_DV360'

    # Google GSP
    elif (('gsp' in campanha) or ('gsp' in conteudo)) and \
            (('google / cpc' in midia) or ('google-adwords / display' in midia) or
             ('google-ads-gsp / display' in midia)):
        veiculo = 'GOOGLE_GSP'

    # Outlook - Verizon
    elif ('outlook / display' in midia) or ('oath / outlook' in midia) or ('oath-outlook / programatico' in midia):
        veiculo = 'OUTLOOK_VERIZON'

    # Lancenet
    elif 'lancenet / display' in midia:
        veiculo = 'LANCENET'

    # Folha
    elif 'folha / display' in midia:
        veiculo = 'FOLHA'

    # Spotify
    elif ('spotify / audio' in midia) or ('spotify / programatico' in midia) or ('spotify / display' in midia):
        veiculo = 'SPOTIFY'

    # Youtube - DV360
    elif ('google-dv360 / video' in midia) or ('google-dv360-youtube / programatico' in midia):
        veiculo = 'YOUTUBE_DV360'

    # Jovem nerd
    elif 'jovem-nerd / content' in midia:
        veiculo = 'JOVEM_NERD'

    # Brazil Journal
    elif 'brazil-journal / display' in midia:
        veiculo = 'BRAZIL_JOURNAL'

    # Kiaora
    elif 'kiaora-digital / display' in midia:
        veiculo = 'KIAORA'

    # Outros
    else:
        veiculo = 'OUTROS'

    # Classifica Search e No Search
    lst_no_search = ['smart', 'gdn', 'discovery-ads']
    if (((midia == 'google / cpc') and ('search' in campanha)) or ((midia == 'bing / cpc') and ('search' in campanha))) \
            and not (any(ele in campanha for ele in lst_no_search)):
        grupo = 'GA_SEARCH'
    else:
        grupo = 'GA_NOM_SEARCH'

    return veiculo, grupo


# Device
def get_device(device):
    if device in ('DESKTOP', 'TABLET'):
        tipo = 'DESKTOP'
    elif device == 'MOBILE':
        tipo = 'MOBILE'
    else:
        tipo = 'CROSS_DEVICE'
    return tipo


# Concatena os dataframes
def concat_df(df1, df2):
    df = pd.concat([df1, df2], axis=1)

    # Filtra 'alp' na campaign
    df = df[df['campaign'].str.contains("alp")]

    # Converte os campos
    df['itemRevenue'] = df['itemRevenue'].astype(float)
    df['avgSessionDuration'] = df['avgSessionDuration'].astype(float)

    # Copia os valores das colunas
    df['DAT_REFERENCIA'] = df['date']
    df['QTD_VISITAS'] = df['sessions']
    df['QTD_NOVOS_USUARIOS'] = df['newUsers']
    df['QTD_REJEICOES'] = df['bounces']
    df['QTD_TEMPO_SESSAO'] = df['avgSessionDuration']
    df['QTD_PAGEVIEWS'] = df['pageviews']
    # df['QTD_TRECHOS'] = df['itemQuantity']
    df['QTD_TRANSACOES'] = df['transactions']
    # df['VAL_VENDA'] = df['itemRevenue']
    df['QTD_BUSCAS'] = df['metric1']
    df['NOM_ORIGEM_DADOS'] = 'GA'
    df['NOM_ANUNCIANTE'] = 'OUTROS'

    for index, row in df.iterrows():
        veiculo, grupo = classify_search(row['campaign'], row['sourceMedium'], row['adContent'])

        df.at[index, 'TIP_DEVICE'] = get_device(row['deviceCategory'].upper())
        df.at[index, 'NOM_PALAVRA_CHAVE'] = row['keyword']

        lst_campanha_grupo_anuncio = ['GOOGLE_LP', 'GOOGLE_LP_BRANDING', 'GOOGLE_ADS_GALLERY', 'SMARTADS_GOOGLE',
                                      'GOOGLE_GDN', 'GOOGLE_DISCOVERY_ADS', 'BING_LP', 'GOOGLE_GSP']

        # Cria o AD
        # if any(ele in veiculo for ele in lst_campanha_grupo_anuncio):
        if (any(ele in veiculo for ele in lst_campanha_grupo_anuncio)) and (row['adGroup'] != '(not set)'):
            ad = row['campaign'] + '_' + row['adGroup']
        else:
            ad = row['adContent']
        df.at[index, 'NOM_AD'] = ad

        # Disciplina
        df.at[index, 'NOM_DISCIPLINA'] = veiculo

        # Quebra do campo NOM_AD
        if grupo == 'GA_SEARCH':
            if ad.count('_') == 9:
                data_ad = ad.split("_", 9)
                anunciante = get_anunciante((data_ad[0] + '_' + data_ad[1]).upper())
                df.at[index, 'NOM_ANUNCIANTE'] = anunciante
                df.at[index, 'NOM_CAMPANHA'] = data_ad[0] + '_' + data_ad[1]
                df.at[index, 'NOM_INICIATIVA'] = data_ad[3]
                df.at[index, 'TIP_CANAL'] = data_ad[7]
                df.at[index, 'TIP_COMPRA'] = data_ad[6]
                df.at[index, 'TIP_ESTRATEGIA'] = data_ad[4]
                df.at[index, 'NOM_CRIATIVO'] = data_ad[9]
                df.at[index, 'NOM_SEGMENTACAO'] = data_ad[5]
                df.at[index, 'TIP_FORMATO'] = data_ad[8]
                df.at[index, 'NOM_MIDIA'] = data_ad[3]
                df.at[index, 'NOM_ORIGEM'] = data_ad[2]
                if (type(data_ad[0]) is str) and \
                        (type(ad) is str) and \
                        (type(data_ad[3]) is str) and \
                        (str(ad) != '') and \
                        (str(data_ad[0]) != '') and \
                        (str(data_ad[3]) != ''):
                    df.at[index, 'NOM_PILAR'] = get_pilar(ad.upper(), anunciante, veiculo, data_ad[3].upper())
        elif grupo == 'GA_NOM_SEARCH':
            if ad.count('_') == 10:
                data_ad = ad.split("_", 10)
                anunciante = get_anunciante(data_ad[1].upper())
                df.at[index, 'NOM_ANUNCIANTE'] = anunciante
                df.at[index, 'NOM_CAMPANHA'] = data_ad[1]
                df.at[index, 'NOM_INICIATIVA'] = data_ad[4]
                df.at[index, 'TIP_CANAL'] = data_ad[5]
                df.at[index, 'TIP_COMPRA'] = data_ad[6]
                df.at[index, 'TIP_ESTRATEGIA'] = data_ad[7]
                df.at[index, 'NOM_CRIATIVO'] = data_ad[9]
                df.at[index, 'NOM_SEGMENTACAO'] = data_ad[10]
                df.at[index, 'TIP_FORMATO'] = data_ad[8]
                df.at[index, 'NOM_MIDIA'] = data_ad[3]
                df.at[index, 'NOM_ORIGEM'] = data_ad[2]
                if (type(data_ad[0]) is str) and \
                        (type(ad) is str) and \
                        (type(data_ad[4]) is str) and \
                        (str(ad) != '') and \
                        (str(data_ad[0]) != '') and \
                        (str(data_ad[4]) != ''):
                    df.at[index, 'NOM_PILAR'] = get_pilar(ad.upper(), anunciante, veiculo, data_ad[4].upper())

    # Apaga as colunas
    df.drop(['date', 'campaign', 'keyword', 'adContent', 'deviceCategory', 'itemRevenue', 'avgSessionDuration',
             'pageviews', 'bounces', 'transactions', 'sessions', 'newUsers', 'itemQuantity', 'metric1', 'adGroup',
             'sourceMedium'], axis=1, inplace=True)

    return df


def main(self):

    # Variaveis de ambiente
    project_id = os.environ['project_id']
    table = os.environ['table']
    view_id = os.environ['view_id']
    diretorio = os.environ['diretorio']
    access_token = os.environ['access_token']
    refresh_token = os.environ['refresh_token']
    token_uri = os.environ['token_uri']
    client_id = os.environ['client_id']
    client_secret = os.environ['client_secret']

    # Log
    logging.info('Inicio da rotina')
    inicio = dt.datetime.now()

    # Cria o report via API
    try:
        response = get_report(setup(access_token, refresh_token, token_uri, client_id, client_secret), view_id)
        df_ga = transform_df(response)
    except Exception as e:
        logging.error('Erro ao criar o relatório via API - {}'.format(e))
        sys.exit(0)

    # Grava os dados da API e os parametros em um arquivo CSV no Storage
    try:
        # Dataframe original
        nome_arquivo = datetime.today().strftime('%Y%m%d') + '_ga360.csv'
        df_ga.to_csv(diretorio + nome_arquivo, sep=',', index=False)
        # Parametros da API
        nome_arquivo_parametro = datetime.today().strftime('%Y%m%d') + '_ga360_parametros.csv'
        dict = {"metrics": 'itemRevenue, avgSessionDuration, pageviews,bounces, transactions, sessions, newUsers, '
                           'itemQuantity, metric1',
                "dimensions": 'date, campaign, sourceMedium, keyword, adGroup, adContent, deviceCategory',
                "viewId": view_id,
                "periodo": '7daysAgo',
                "versao_api": 'V4',
                "data_hora_leitura": datetime.now()}
        df_param = pd.DataFrame(dict, index=[0])
        df_param.to_csv(diretorio + nome_arquivo_parametro, index=False)
    except Exception as e:
        logging.error('Erro ao gravar os dados da API em um CSV no storage - {}'.format(e))
        sys.exit(0)

    # Captura a minina/maxima data no dataframe da API
    try:
        df_ga['date'] = pd.to_datetime(df_ga['date'], format='%Y-%m-%d')
        dt_min_api = df_ga['date'].min().date()
        dt_max_api = df_ga['date'].max().date()
    except Exception as e:
        logging.error('Erro ao capturar a data minima/maxima da API - {}'.format(e))
        sys.exit(0)

    # Apaga os registros após o filtro
    try:
        if check_table_exist(project_id, table):
            del_rows('GA', project_id, dt_min_api, dt_max_api, table, '')
    except Exception as e:
        logging.error('Erro ao apagar os registros - {}'.format(e))
        sys.exit(0)

    # Cria o dataframe do GCP
    try:
        df_ga_gfc = create_dataframe_gcp()
    except Exception as e:
        logging.error('Erro ao criar o dataframe GCP - {}'.format(e))
        sys.exit(0)

    # Concatena os dataframes
    try:
        df_concat = concat_df(df_ga_gfc, df_ga)
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

    # Grava as informações no arquivo de log
    fim = dt.datetime.now()
    tempo = fim - inicio
    logging.info('Tempo de execucao da rotina: ' + str(tempo))
    logging.info('Fim da rotina\n')
