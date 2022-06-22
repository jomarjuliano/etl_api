
import datetime as dt
from datetime import datetime, date
import os
import sys
import pandas as pd
import logging
from googleads import adwords
import _locale
from io import StringIO
from googleads import oauth2
from googleads.adwords import AdWordsClient
from googleads.oauth2 import GoogleRefreshTokenClient
import sendgrid
from sendgrid.helpers.mail import *
from sendgrid import SendGridAPIClient
from google.auth import compute_engine
from google.cloud import bigquery
import google.auth


# Configura o encode
_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])

ORIGEM_DADOS = 'ADWORDS'

# client_bq = bigquery.Client()

credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/bigquery",
    ]
)

# Construct a BigQuery client object.
client_bq = bigquery.Client(credentials=credentials, project=project)


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
                + ' AND NOM_DISCIPLINA = ' + '"' + disciplina + '"'
    query_job = client_bq.query(query)
    query_job.result()


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


# Transforma o e texto do video em 0 ou 1
def convert_video(texto):
    if texto == '0.00':
        val = 0
    else:
        val = 1
    return val


# Concactena os dataframes
def concat_df(df1, df2):
    df = pd.concat([df1, df2], axis=1)

    # Renomeia as colunas
    df = df.rename(columns={'Video played to 100%': 'Video_100'})
    df = df.rename(columns={'Video played to 75%': 'Video_75'})
    df = df.rename(columns={'Video played to 50%': 'Video_50'})
    df = df.rename(columns={'Video played to 25%': 'Video_25'})
    df = df.rename(columns={'Ad group': 'Ad_group'})
    df = df.rename(columns={'Final URL': 'URL_Final'})

    # Converte os campos
    df['Day'] = pd.to_datetime(df['Day'])
    df['Cost'] = df['Cost'].astype(float)

    # Copia os valores das colunas
    df['DAT_REFERENCIA'] = df['Day']
    df['NOM_AD'] = df['Campaign'] + '_' + df['Ad_group']
    df['VAL_INVESTIMENTO'] = df['Cost'] / 1000000
    df['QTD_VIEWS_PLAY'] = df['Views']
    df['QTD_VIEWS_25'] = df['Video_25'].replace("%", "", regex=True)
    df['QTD_VIEWS_50'] = df['Video_50'].replace("%", "", regex=True)
    df['QTD_VIEWS_75'] = df['Video_75'].replace("%", "", regex=True)
    df['QTD_VIEWS_100'] = df['Video_100'].replace("%", "", regex=True)
    df['NOM_ORIGEM_DADOS'] = ORIGEM_DADOS
    df['QTD_IMPRESSOES'] = df['Impressions']
    df['QTD_CLIQUES'] = df['Clicks']
    df['NOM_URL'] = df['URL_Final']\
        .replace("\[", "", regex=True)\
        .replace("\]", "", regex=True)\
        .replace("\"", "", regex=True)

    for index, row in df.iterrows():
        df.at[index, 'TIP_DEVICE'] = row['Device'].upper()
        df.at[index, 'NOM_ANUNCIANTE'] = row['Campaign'].upper()
        df.at[index, 'NOM_DISCIPLINA'] = row['Campaign'].upper()
        df.at[index, 'QTD_VIEWS_25'] = convert_video(row['QTD_VIEWS_25'])
        df.at[index, 'QTD_VIEWS_50'] = convert_video(row['QTD_VIEWS_50'])
        df.at[index, 'QTD_VIEWS_75'] = convert_video(row['QTD_VIEWS_75'])
        df.at[index, 'QTD_VIEWS_100'] = convert_video(row['QTD_VIEWS_100'])

        # Conta a quantidade de '_'
        qtd_caract = row['NOM_AD'].count("_")

        # Quebra do campo NOM_AD
        if qtd_caract == 9:
            data_ad = row['NOM_AD'].split("_", 10)
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
        elif qtd_caract == 10:
            data_ad = row['NOM_AD'].split("_", 11)
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

    # df = df[df.VAL_INVESTIMENTO > 0

    # Apaga as colunas
    df.drop(['Ad_group', 'Campaign', 'Cost', 'Day', 'Device', 'Video_100', 'Video_25', 'Video_50', 'Video_75',
             'Views', 'Clicks', 'Impressions', 'Campaign state', 'URL_Final'], axis=1, inplace=True)

    return df


# Criação do report
def create_report(client, lst_contas):
    report_downloader = client.GetReportDownloader(version='v201809')
    # Percorre as contas para gerar o relatório
    result_final = ''
    for conta in lst_contas:
        client.client_customer_id = conta
        report_query = (adwords.ReportQueryBuilder().
                        Select('AdGroupName', 'CampaignName', 'Cost', 'Date', 'Device', 'VideoQuartile100Rate',
                               'VideoQuartile25Rate', 'VideoQuartile50Rate', 'VideoQuartile75Rate', 'VideoViews',
                               'CampaignStatus', 'Clicks', 'Impressions', 'CreativeFinalUrls').
                        From('AD_PERFORMANCE_REPORT').
                        During('LAST_7_DAYS').
                        # During('20210121' + ',' + '20210131').
                        Build())

        result_query = report_downloader.DownloadReportAsStringWithAwql(report_query,
                                                                        'CSV',
                                                                        skip_report_header=True,
                                                                        skip_column_header=False,
                                                                        skip_report_summary=True,
                                                                        include_zero_impressions=False)
        result_final = result_final + result_query
    df_texto = StringIO(result_final)
    df = pd.read_csv(df_texto, sep=",", encoding="utf-8", low_memory=False)

    return df


def main(self):
    # Variaveis de ambiente
    global from_name, to_name, project_id, table_iniciativa
    project_id = os.environ['project_id']
    table = os.environ['table']
    contas = os.environ['contas']
    developer_token = os.environ['developer_token']
    client_id = os.environ['client_id']
    client_secret = os.environ['client_secret']
    refresh_token = os.environ['refresh_token']
    diretorio = os.environ['diretorio']
    funcao_dispositivo = os.environ['funcao_dispositivo']
    funcao_veiculo = os.environ['funcao_veiculo']
    funcao_anunciante = os.environ['funcao_anunciante']
    from_name = os.environ['from_name']
    to_name = os.environ['to_name']
    table_iniciativa = os.environ['table_iniciativa']
    from_name = os.environ['from_name']
    to_name = os.environ['to_name']
    global sendgrid_key
    sendgrid_key = os.environ['sendgrid_key']

    # Log
    logging.info('Inicio da rotina')
    inicio = dt.datetime.now()

    # Cria uma lista com as contas
    global lst_contas
    lst_contas = contas.split(",")

    # Cria o cliente Google
    try:
        # init oauth refresh token client
        oauth_client = oauth2.GoogleRefreshTokenClient(client_id=client_id,
                                                       client_secret=client_secret,
                                                       refresh_token=refresh_token)
        # adwords client
        adwords_client = adwords.AdWordsClient(developer_token, oauth_client)
    except Exception as e:
        send_email(e, from_name, to_name, ORIGEM_DADOS)
        logging.error('Erro ao criar o cliente Google - {}'.format(e))
        sys.exit(0)

    # Cria o report via API
    try:
        df_adwords = create_report(adwords_client, lst_contas)
    except Exception as e:
        send_email(e, from_name, to_name, ORIGEM_DADOS)
        logging.error('Erro ao criar o relatório via API - {}'.format(e))
        sys.exit(0)

    # Filtra os registros que não tem data
    df_adwords = df_adwords[df_adwords.Day != 'Day']

    # Verifica quantas linhas tem o dataframe
    qtd_registros = df_adwords.shape[0]

    if qtd_registros > 0:

        # Grava os dados da API e os parametros em um arquivo CSV no Storage
        try:
            # Dataframe original
            nome_arquivo = datetime.today().strftime('%Y%m%d') + '_adwords.csv'
            df_adwords.to_csv(diretorio + nome_arquivo, sep=',', index=False)
            # Parametros da API
            nome_arquivo_parametro = datetime.today().strftime('%Y%m%d') + '_adwords_parametros.csv'
            dict = {"campos": 'AdGroupName, CampaignName, Cost, Date, Device, VideoQuartile100Rate, '
                              'VideoQuartile25Rate, VideoQuartile50Rate, VideoQuartile75Rate, VideoViews, '
                              'CampaignStatus, Clicks, Impressions, CreativeFinalUrls',
                    "from": 'AD_PERFORMANCE_REPORT',
                    "periodo": 'LAST_7_DAYS',
                    "versao_api": '201809',
                    "data_hora_leitura": datetime.now()}
            df_param = pd.DataFrame(dict, index=[0])
            df_param.to_csv(diretorio + nome_arquivo_parametro, index=False)
        except Exception as e:
            send_email(e, from_name, to_name, ORIGEM_DADOS)
            logging.error('Erro ao gravar os dados da API em um CSV no storage - {}'.format(e))
            sys.exit(0)

        # Captura a minima/maxima data no dataframe
        try:
            df_adwords['Day'] = pd.to_datetime(df_adwords['Day'], format='%Y-%m-%d')
            dt_min_df = df_adwords['Day'].min().date()
            dt_max_df = df_adwords['Day'].max().date()
        except Exception as e:
            logging.error('Erro ao capturar a data minima do dataframe - {}'.format(e))
            sys.exit(0)

        # Apaga os registros após o filtro
        try:
            del_rows(ORIGEM_DADOS, dt_min_df, dt_max_df, table, '')
        except Exception as e:
            logging.error('Erro ao apagar os registros - {}'.format(e))
            sys.exit(0)

        # Cria o dataframe do GCP
        try:
            df_adwords_gfc = create_dataframe_gcp()
        except Exception as e:
            logging.error('Erro ao criar o dataframe GCP - {}'.format(e))
            sys.exit(0)

        # Concatena os dataframes
        try:
            df_concat = concat_df(df_adwords_gfc, df_adwords)
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

        # Atualiza NOM_ANUNCIANTE, NOM_DISCIPLINA e TIP_DEVICE
        try:
            query = 'UPDATE ' + table + \
                    ' SET TIP_DEVICE = ' + funcao_dispositivo + '(TIP_DEVICE), NOM_ANUNCIANTE = ' + \
                    funcao_anunciante + '(NOM_ANUNCIANTE)' + ' ,NOM_DISCIPLINA = ' + \
                    funcao_veiculo + '(UPPER(NOM_DISCIPLINA))' + \
                    ' WHERE DAT_REFERENCIA BETWEEN "' + str(dt_min_df) + '"' + ' AND ' + '"' + str(dt_max_df) + '"' + \
                    ' AND NOM_ORIGEM_DADOS = ' + '"' + ORIGEM_DADOS + '"'
            query_job = client_bq.query(query)
            query_job.result()
            logging.info('Update realizado com sucesso.')
        except Exception as e:
            send_email(e, from_name, to_name, ORIGEM_DADOS)
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
            send_email(e, from_name, to_name, ORIGEM_DADOS)
            logging.error('Erro ao realizar o update na base de dados - {}'.format(e))
            sys.exit(0)

    else:
        logging.info('Nenhum registro para o periodo informado.')

    # Grava as informações no arquivo de log
    fim = dt.datetime.now()
    tempo = fim - inicio
    logging.info('Tempo de execucao da rotina: ' + str(tempo))
    logging.info('Fim da rotina' + '\n')
