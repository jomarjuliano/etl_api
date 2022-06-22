import pandas as pd
import logging
import configparser
import sys
import datetime as dt
import os
from datetime import datetime, date
from utils import *
from google.cloud import storage


ORIGEM_DADOS = 'RTBHOUSE'

client_bq = bigquery.Client()


# Concactena os dataframes
def concat_df(df1, df2):
    df = pd.concat([df1, df2], axis=1)

    # Renomeia as colunas
    df = df.rename(columns={'Cost (BRL)': 'Cost'})

    # Copia os valores das colunas
    df['DAT_REFERENCIA'] = df['Date']
    df['VAL_INVESTIMENTO'] = df['Cost']
    df['NOM_DISCIPLINA'] = ORIGEM_DADOS
    df['NOM_SITE'] = ORIGEM_DADOS
    df['NOM_AD'] = df['Size']
    df['NOM_ORIGEM_DADOS'] = 'PARCEIRO'

    # Quebra a string Placement Name
    for index, row in df.iterrows():
        if row['Size'].count('_') >= 10:
            data_ad = row['Size'].split("_",  10)
            df.at[index, 'NOM_CAMPANHA'] = data_ad[0] + '_' + data_ad[1]
            df.at[index, 'TIP_DEVICE'] = row['Device'].upper()
            df.at[index, 'NOM_ANUNCIANTE'] = (data_ad[1]).upper()
            df.at[index, 'NOM_ORIGEM'] = data_ad[2]
            df.at[index, 'NOM_MIDIA'] = data_ad[3]
            df.at[index, 'NOM_INICIATIVA'] = data_ad[4]
            df.at[index, 'TIP_CANAL'] = data_ad[5]
            df.at[index, 'TIP_COMPRA'] = data_ad[6]
            df.at[index, 'TIP_ESTRATEGIA'] = data_ad[7]
            df.at[index, 'TIP_FORMATO'] = data_ad[8]
            df.at[index, 'NOM_CRIATIVO'] = data_ad[9]
            df.at[index, 'NOM_SEGMENTACAO'] = data_ad[10]

    # Apaga as colunas
    df.drop(['Date', 'Size', 'Device', 'Imps', 'Clicks', 'Cost'], axis=1, inplace=True)
    return df


def main(self):
    # Variaveis de ambiente
    project_id = os.environ['project_id']
    table = os.environ['table']
    funcao_dispositivo = os.environ['funcao_dispositivo']
    funcao_anunciante = os.environ['funcao_anunciante']

    # Storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('arquivos_etl')

    # Log
    inicio = dt.datetime.now()

    # Percorre os arquivos que estão na pasta "Importados"
    blobs = bucket.list_blobs(prefix='importados/rtbhouse/', delimiter=None)

    # Percorre os arquivos no bucket
    for blob in blobs:

        # Verifica se existe algum arquivo
        if blob.name[20:] != '':

            # Valida se o nomme do arquivo é válido
            if verify_file_name(os.path.splitext(blob.name[20:])[0], 'rtbhouse'):

                # Valida se o arquivo está zerado
                # if blob.chunk_size > 0:
                # if not blob.chunk_size is none:

                # Leitura do Excel
                try:
                    df_rtbhouse = pd.read_csv('gs://arquivos_etl/' + blob.name, delimiter=';')
                except Exception as e:
                    logging.error('Erro ao carregar o Excel no dataframe - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/rtbhouse/' + blob.name[20:])
                    sys.exit(0)

                # Verifica se o arquivo veio zerado
                if df_rtbhouse.empty:
                    logging.info('Arquivo zerado.')
                    sys.exit(0)

                # Captura a minina/maxima data no dataframe do Excel
                try:
                    df_rtbhouse['Date'] = pd.to_datetime(df_rtbhouse['Date'])
                    dt_min_excel = df_rtbhouse['Date'].min().date()
                    dt_max_excel = df_rtbhouse['Date'].max().date()
                except Exception as e:
                    logging.error('Erro ao capturar a data minima/maxima do Excel - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/rtbhouse/' + blob.name[20:])
                    sys.exit(0)

                # Apaga os registros após o filtro
                try:
                    del_rows('PARCEIRO', project_id, dt_min_excel, dt_max_excel, table, ORIGEM_DADOS)
                except Exception as e:
                    logging.error('Erro ao apagar os registros - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/rtbhouse/' + blob.name[20:])
                    sys.exit(0)

                # Cria o dataframe do GCP
                try:
                    df_rtbhouse_gfc = create_dataframe_gcp()
                except Exception as e:
                    logging.error('Erro ao criar o dataframe GCP - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/rtbhouse/' + blob.name[20:])
                    sys.exit(0)

                # Concatena os dataframes
                try:
                    df_concat = concat_df(df_rtbhouse_gfc, df_rtbhouse)
                except Exception as e:
                    logging.error('Erro ao concatenar os dataframes - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/rtbhouse/' + blob.name[20:])
                    sys.exit(0)

                # Converte os tipos das colunas
                try:
                    df_final = convert_columns_types(df_concat)
                except Exception as e:
                    logging.error('Erro ao converter os tipos das colunas - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/rtbhouse/' + blob.name[20:])
                    sys.exit(0)

                # Insere os dados no BigQuery
                try:
                    if not df_final.empty:
                        insert_gfc(df_final, table, project_id)
                    else:
                        logging.info('Não existem registros para serem inseridos.')
                except Exception as e:
                    logging.error('Erro ao inserir os dados do dataframe no BigQuery - {}'.format(e))
                    bucket.rename_blob(blob, 'erros/rtbhouse/' + blob.name[20:])
                    sys.exit(0)

                # Move o arquivo para a pasta processados
                try:
                    bucket.rename_blob(blob, 'processados/rtbhouse/' + blob.name[20:])
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

                # else:
                #     logging.info('Arquivo zerado.')

            else:
                logging.error('Arquivo com nomenclatura inválida - {}'.format(os.path.splitext(blob.name[20:])[0]))
                # bucket.rename_blob(blob, 'erros/rtbhouse/' + blob.name[20:])
        else:
            logging.info('Não existem arquivos na pasta.')

    # Grava as informações no arquivo de log
    fim = dt.datetime.now()
    tempo = fim - inicio
    logging.info('Tempo de execucao da rotina: ' + str(tempo))
    logging.info('Fim da rotina\n')

