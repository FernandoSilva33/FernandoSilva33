import os
import sys
import pandas as pd
from impala.dbapi import connect, Error as ImpalaError
from datetime import datetime, timedelta
import zipfile
import random
from ftplib import FTP, error_perm
import socket

# Obter o diretório "Documentos" do usuário atual
pasta_documentos = os.path.expanduser("~" + os.sep + "Documents")

# Criar o caminho para a pasta "Automato" dentro da pasta "Documentos"
pasta_automato = os.path.join(pasta_documentos, "Automato Periodo")

# Verificar se a pasta "Automato" já existe, se não, criar
if not os.path.exists(pasta_automato):
    os.makedirs(pasta_automato)

# Verificar se a pasta "Logs" já existe, se não, criar
log_dir = os.path.join(pasta_automato, "Logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Verificar se a pasta "CSV" já existe, se não, criar
zip_dir = os.path.join(pasta_automato, "CSV")
if not os.path.exists(zip_dir):
    os.makedirs(zip_dir)

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nome_arquivo = os.path.join(log_dir, "log_periodo_{}.txt".format(data_log.strftime("%Y.%m.%d_%H%M%S")))
    with open(nome_arquivo, 'a') as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(message)
    
data_log = datetime.now()

log_message('----Iniciando login na BISP----')

# Cria uma lista para armazenar usuário e senha
usuarios_senhas = []

# O arquivo credencials deve ser armazenado na raiz de Automato em Documentos
with open(os.path.join(pasta_automato, 'credencials'), 'r') as txtfile:
    for linha in txtfile:
        usuario, senha = linha.strip().split('|')
        usuarios_senhas.append((usuario, senha))
        
usuario_senha_aleatorio = random.choice(usuarios_senhas)
usuario_esc = usuario_senha_aleatorio[0]
passw = usuario_senha_aleatorio[1]
log_message(f'Acessando com o usuário: {usuario_esc}')

try:
    conn = connect(host='clouderacdp02.prodemge.gov.br', port=21051,
                database='db_bisp_reds_reporting',
                auth_mechanism='PLAIN',
                use_ssl=1,
                user=usuario_esc,
                password=passw,)
    cursor = conn.cursor()
    
    log_message('***** BISP CONECTADA - SUCESSO *****')

    #################################################
    ### LAÇO DE REPETIÇÃO PARA PERÍODO PROGRAMADO ###
    #################################################

    #DECLARAÇÃO DO INÍCIO DO PERÍODO

    mes_31 = [1, 3, 5, 7, 8, 10, 12]
    mes_30 = [4, 6, 9, 11]

    data_atual = datetime(2024, 5, 1)
    data_fim = data_atual + timedelta(9)

    # print(data_atual)
    # print(type(data_atual))
    # print(data_fim)
    # print(type(data_fim))
    # data_now = datetime.now()
    # print(data_now)
    # print(type(data_now))


    # time.sleep(30)

    while data_atual <= data_fim:
        data_nome = data_atual.strftime("%Y%m%d")
        if data_atual.day > 20:
            if data_atual.month in mes_31:
                day_add = 10
            elif data_atual.month in mes_30:
                day_add = 9
            else:
                day_add = 8 # Define o ano bissexto.
        else:
            day_add = 9

        data_fim = data_atual + timedelta(days=day_add)

        data_consulta = data_atual.strftime("%Y%m%d")
        data_term_consulta = data_fim.strftime("%Y%m%d")

        #DATA USADA PARA NOME DOS ARQUIVOS
        data_inicial_tm = data_atual
        data_inicial_tm = datetime.combine(data_inicial_tm, datetime.min.time())  # Define a hora inicial como 00:00:00
    
        data_final_tm = data_fim
        data_final_tm = datetime.combine(data_final_tm, datetime.min.time())
        data_final_tm = data_final_tm.replace(hour=23, minute=59, second=59)

        # Convertendo datetime em strings com a mesma formatação
        data_inicial = data_inicial_tm.strftime("%Y-%m-%d %H:%M:%S")
        data_final = data_final_tm.strftime("%Y-%m-%d %H:%M:%S")

        log_message(f'A data atual é: {data_atual}')
        log_message(f'A data de inicio da extração é: {data_inicial}')
        log_message(f'A data de término da extração é: {data_final}')

        os.chdir(zip_dir)
        log_message(f'Mudou para o diretório: {os.getcwd()}')
        # A QUERY A SER CONSULTADA
        
        query_1 = """
            -- 1 - REDS_RAT - OK
            SELECT	
                OCO.numero_ocorrencia AS "RAT.NUM_ATIVIDADE",
                OCO.natureza_codigo AS "NAT.CODIGO",
                OCO.natureza_descricao AS "NAT.DESCRICAO",
                FROM_TIMESTAMP(OCO.data_hora_inclusao, 'dd/MM/yyyy') AS DTA_HRA_INCLUSAO,
                FROM_TIMESTAMP(OCO.data_hora_fato, 'dd/MM/yyyy') AS DTA_INICIO,
                FROM_TIMESTAMP(OCO.data_hora_fato, 'HH:mm') AS HRA_INICIO,
                FROM_TIMESTAMP(OCO.data_hora_final, 'dd/MM/yyyy') AS DTA_TERMINO,
                FROM_TIMESTAMP(OCO.data_hora_final, 'HH:mm') AS HRA_TERMINO,
                OCO.complemento_natureza_descricao AS 'DES_ALVO_EVENTO',
                OCO.local_imediato_descricao AS 'DES_LUGAR',
                OCO.nome_operacao AS 'NOM_OPERACAO',
                OCO.unidade_responsavel_registro_codigo AS 'COD_UNIDADE_SERVICO',
                OCO.unidade_responsavel_registro_nome AS 'NOM_UNID_RESPONSAVEL',
                OCO.tipo_logradouro_descricao AS 'TIPO_LOGRADOURO',
                OCO.logradouro_nome AS 'LOGRADOURO',
                OCO.descricao_endereco AS 'DES_ENDERECO',
                OCO.numero_endereco AS 'NUM_ENDERECO',
                OCO.complemento_alfa AS 'COMPLEMENTO_ALFA',
                OCO.descricao_complemento_endereco AS 'COMPLEMENTO_ENDERECO',
                OCO.numero_complementar AS 'NUM_COMPLEMENTAR',
                OCO.codigo_bairro AS 'COD_BAIRRO',
                OCO.nome_bairro AS 'NOME_BAIRRO',
                OCO.tipo_logradouro2_descricao AS 'TIPO_LOGRADOURO2',
                OCO.logradouro2_nome AS 'LOGRADOURO2',
                OCO.descricao_endereco_2 AS 'DES_ENDERECO2',
                NULLIF(CAST(OCO.codigo_municipio AS INT), 0) AS "COD_MUNICIPIO",
                OCO.nome_municipio AS 'MUNICIPIO',
                OCO.numero_latitude AS 'LATITUDE',
                OCO.numero_longitude AS 'LONGITUDE',
                MASTER.codigo_unidade_area AS 'COD_UNIDADE_AREA',
                MASTER.unidade_area_militar_nome AS 'NOM_UNIDADE_AREA',
                CONCAT('PM',OCO.digitador_matricula) AS 'DIGITADOR'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia OCO
            LEFT JOIN db_bisp_reds_master.tb_local_unidade_area_pmmg AS 'MASTER'
                ON OCO.id_local = MASTER.id_local
            WHERE 1=1
            AND OCO.nome_tipo_relatorio = 'RAT'
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND OCO.data_hora_alteracao BETWEEN '{}' AND '{}'
            ORDER BY OCO.data_hora_fato;
        """.format(data_inicial, data_final)

        log_message('----Iniciando processamento da query.----')
        cursor.execute(query_1)
        log_message('----Query 1 processada com sucesso!----')

        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

        # Converte as colunas em uppercase
        df.columns = [col.upper() for col in df.columns]
        
        arquivo_csv = f"CARNAVAL_REDS_RAT_{data_consulta}_{data_term_consulta}.csv"
        arquivo_zip = f"CARNAVAL_REDS_RAT_{data_consulta}_{data_term_consulta}.zip"
        df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
        with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
            zipf.write(arquivo_csv)
        
        os.remove(arquivo_csv)
        query_2 = """
            -- 2 -REDS_RAT_EFETIVOS - OK
            SELECT	
                OCO.numero_ocorrencia AS 'NUM_ATIVIDADE',
                OCO.digitador_matricula AS 'NUM_MATRICULA',
                OCO.digitador_nome AS 'NOME',
                OCO.digitador_cargo_efetivo AS 'NOM_CARGO',
                OCO.unidade_responsavel_registro_codigo AS 'COD_UNIDADE_SERVICO',
                OCO.unidade_responsavel_registro_nome AS 'NOM_UNIDADE'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia OCO
            WHERE 1=1
            AND OCO.nome_tipo_relatorio = 'RAT'
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND OCO.data_hora_alteracao BETWEEN '{}' AND '{}'
            ORDER BY OCO.data_hora_fato;
        """.format(data_inicial, data_final)
        cursor.execute(query_2)
        log_message('----Query 2 processada com sucesso!----')

        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

        # Converte as colunas em uppercase
        df.columns = [col.upper() for col in df.columns]

        arquivo_csv = f"CARNAVAL_REDS_RAT_EFETIVOS_{data_consulta}_{data_term_consulta}.csv"
        arquivo_zip = f"CARNAVAL_REDS_RAT_EFETIVOS_{data_consulta}_{data_term_consulta}.zip"
        df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
        with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
            zipf.write(arquivo_csv)
        
        os.remove(arquivo_csv)
        
        query_3 = """
        SELECT	
        -- 3 - REDS_RAT_produtividade - OK
            PROD.numero_ocorrencia AS 'RAT.NUM_ATIVIDADE',
            PROD.indicador_descricao AS 'DESCRICAO',
            PROD.quantidade AS 'QUANTIDADE'	
        FROM
            db_bisp_reds_reporting.tb_ocorrencia OCO
        LEFT JOIN
            db_bisp_reds_reporting.vw_rat_produtividade_ocorrencia_s PROD
            ON OCO.numero_ocorrencia = PROD.numero_ocorrencia
        WHERE 1=1
        AND OCO.data_hora_fato IS NOT NULL
        AND PROD.data_hora_fato BETWEEN '{}' AND '{}'
        AND PROD.quantidade <> 0
        ORDER BY PROD.data_hora_fato;
        """.format(data_inicial, data_final)
        cursor.execute(query_3)
        log_message('----Query 3 processada com sucesso!----')

        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

        # Converte as colunas em uppercase
        df.columns = [col.upper() for col in df.columns]

        arquivo_csv = f"CARNAVAL_REDS_RAT_Produtividade_{data_consulta}_{data_term_consulta}.csv"
        arquivo_zip = f"CARNAVAL_REDS_RAT_Produtividade_{data_consulta}_{data_term_consulta}.zip"
        df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
        with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
            zipf.write(arquivo_csv)
        
        os.remove(arquivo_csv)
        
        query_4 = """
            -- 4 - REDS_RAT_VIATURAS - OK
            SELECT	
                VTR.numero_ocorrencia AS 'NUM_ATIVIDADE',
                VTR.numero_sequencial_viatura AS 'NUM_SEQ_RECURSO',
                VTR.placa AS 'NUM_PLACA',
                VTR.numero_reg AS 'NUM_PREFIXO'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia AS OCO
            LEFT JOIN db_bisp_reds_reporting.tb_viatura_ocorrencia VTR
                ON  OCO.numero_ocorrencia = VTR.numero_ocorrencia
            WHERE 1=1
            AND orgao_sigla = 'PM'
            AND OCO.nome_tipo_relatorio = 'RAT'
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND VTR.data_hora_fato BETWEEN '{}' AND '{}'
            ORDER BY VTR.data_hora_fato;
        """.format(data_inicial, data_final)
        cursor.execute(query_4)
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

        # Converte as colunas em uppercase
        df.columns = [col.upper() for col in df.columns]

        arquivo_csv = f"CARNAVAL_REDS_RAT_VIATURAS_{data_consulta}_{data_term_consulta}.csv"
        arquivo_zip = f"CARNAVAL_REDS_RAT_VIATURAS_{data_consulta}_{data_term_consulta}.zip"
        df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
        with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
            zipf.write(arquivo_csv)
            
        log_message('----Query 4 processada com sucesso!----')
        
        os.remove(arquivo_csv)
        query_5 = """
            -- 5 - REDS_BOS - OK
            SELECT	
                OCO.numero_ocorrencia AS "RAT.NUM_ATIVIDADE",
                OCO.natureza_codigo AS "NAT.CODIGO",
                OCO.natureza_descricao AS "NAT.DESCRICAO",
                FROM_TIMESTAMP(OCO.data_hora_inclusao, 'dd/MM/yyyy') AS 'DTA_HRA_INCLUSAO',
                FROM_TIMESTAMP(OCO.data_hora_fato, 'dd/MM/yyyy') AS DTA_INICIO,
                FROM_TIMESTAMP(OCO.data_hora_fato, 'HH:mm') AS HRA_INICIO,
                FROM_TIMESTAMP(OCO.data_hora_final, 'dd/MM/yyyy') AS DTA_TERMINO,
                FROM_TIMESTAMP(OCO.data_hora_final, 'HH:mm') AS HRA_TERMINO,
                OCO.complemento_natureza_descricao AS 'DES_ALVO_EVENTO',
                OCO.local_imediato_descricao AS 'DES_LUGAR',
                OCO.unidade_responsavel_registro_codigo AS 'COD_UNIDADE_SERVICO',
                OCO.unidade_responsavel_registro_nome AS 'NOM_UNID_RESPONSAVEL',
                OCO.tipo_logradouro_descricao AS 'TIPO_LOGRADOURO',
                OCO.logradouro_nome AS 'LOGRADOURO',
                OCO.descricao_endereco AS 'DES_ENDERECO',
                OCO.numero_endereco AS 'NUM_ENDERECO',
                OCO.complemento_alfa AS 'COMPLEMENTO_ALFA',
                OCO.descricao_complemento_endereco AS 'COMPLEMENTO_ENDERECO',
                OCO.numero_complementar AS 'NUM_COMPLEMENTAR',
                OCO.codigo_bairro AS 'COD_BAIRRO',
                OCO.nome_bairro AS 'NOME_BAIRRO',
                OCO.tipo_logradouro2_descricao AS 'TIPO_LOGRADOURO2',
                OCO.logradouro2_nome AS 'LOGRADOURO2',
                OCO.descricao_endereco_2 AS 'DES_ENDERECO2',
                NULLIF(CAST(OCO.codigo_municipio AS INT), 0) AS "COD_MUNICIPIO",
                OCO.nome_municipio AS 'MUNICIPIO',
                OCO.numero_latitude AS 'LATITUDE',
                OCO.numero_longitude AS 'LONGITUDE',
                MASTER.codigo_unidade_area AS 'COD_UNIDADE_AREA',
                MASTER.unidade_area_militar_nome AS 'NOM_UNIDADE_AREA',
                CONCAT('PM',OCO.digitador_matricula) AS 'DIGITADOR'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia OCO
            LEFT JOIN db_bisp_reds_master.tb_local_unidade_area_pmmg AS MASTER
                ON OCO.id_local = MASTER.id_local
            WHERE 1=1
            AND OCO.nome_tipo_relatorio IN ('BOS', 'BOS AMPLO')
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND OCO.data_hora_alteracao BETWEEN '{}' AND '{}'
            ORDER BY OCO.data_hora_fato;
        """.format(data_inicial, data_final)
        cursor.execute(query_5)
        log_message('----Query 5 processada com sucesso!----')

        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

        # Converte as colunas em uppercase
        df.columns = [col.upper() for col in df.columns]

        arquivo_csv = f"CARNAVAL_REDS_BOS_{data_consulta}_{data_term_consulta}.csv"
        arquivo_zip = f"CARNAVAL_REDS_BOS_{data_consulta}_{data_term_consulta}.zip"
        df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
        with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
            zipf.write(arquivo_csv)
        
        os.remove(arquivo_csv)
        query_6 = """
            -- 6 - REDS_BOS_EFETIVOS - OK
            SELECT	
                OCO.numero_ocorrencia AS 'NUM_ATIVIDADE',
                OCO.digitador_matricula AS 'NUM_MATRICULA',
                OCO.digitador_nome AS 'NOME',
                OCO.digitador_cargo_efetivo AS 'NOM_CARGO',
                OCO.unidade_responsavel_registro_codigo AS 'COD_UNIDADE_SERVICO',
                OCO.unidade_responsavel_registro_nome AS 'NOM_UNIDADE'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia OCO
            WHERE 1=1
            AND OCO.nome_tipo_relatorio IN ('BOS', 'BOS AMPLO')
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND OCO.data_hora_alteracao BETWEEN '{}' AND '{}'
            ORDER BY OCO.data_hora_fato;
        """.format(data_inicial, data_final)
        cursor.execute(query_6)
        log_message('----Query 6 processada com sucesso!----')

        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

        # Converte as colunas em uppercase
        df.columns = [col.upper() for col in df.columns]

        arquivo_csv = f"CARNAVAL_REDS_BOS_EFETIVOS_{data_consulta}_{data_term_consulta}.csv"
        arquivo_zip = f"CARNAVAL_REDS_BOS_EFETIVOS_{data_consulta}_{data_term_consulta}.zip"
        df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
        with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
            zipf.write(arquivo_csv)
        
        os.remove(arquivo_csv)
        
        query_7 = """
            -- 7 - REDS_BOS_ENVOLVIDO - OK
            SELECT	
                OCO.numero_ocorrencia AS 'NUM_ATIVIDADE',
                ENV.nome_completo_envolvido AS 'NOM_ENVOLVIDO',
                ENV.nome_mae AS 'NOM_MAE',
                ENV.envolvimento_descricao AS 'TIPO_ENVOLVIMENTO',
                ENV.tipo_logradouro_descricao AS 'TIPO_LOGRADOURO',
                ENV.logradouro_nome AS 'LOGRADOURO',
                ENV.numero_endereco AS 'NUM_ENDERECO',
                ENV.compl_alfa AS 'COMPLEMENTO_ALFA',
                ENV.descricao_complementar_endereco AS 'COMPLEMENTO_ENDERECO',
                ENV.numero_complementar AS 'NUM_COMPLEMENTAR',
                ENV.codigo_bairro AS 'COD_BAIRRO',
                ENV.nome_bairro AS 'NOME_BAIRRO',
                NULLIF(CAST(ENV.codigo_municipio AS INT), 0) AS "COD_MUNICIPIO",
                MUN.dsmunicipiosemacentomaiusc AS 'MUNICIPIO'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia OCO
            LEFT JOIN db_bisp_reds_reporting.tb_envolvido_ocorrencia AS ENV
                ON OCO.numero_ocorrencia = ENV.numero_ocorrencia
            LEFT JOIN db_bisp_shared.tb_dim_municipio AS MUN
                ON ENV.codigo_municipio = MUN.cdmunicipioibge6
            WHERE 1=1
            AND OCO.nome_tipo_relatorio IN ('BOS', 'BOS AMPLO')
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND OCO.data_hora_alteracao BETWEEN '{}' AND '{}'
            AND ENV.nome_completo_envolvido <> ''
            ORDER BY OCO.data_hora_fato;
        """.format(data_inicial, data_final)
        cursor.execute(query_7)
        log_message('----Query 7 processada com sucesso!----')

        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

        # Converte as colunas em uppercase
        df.columns = [col.upper() for col in df.columns]

        arquivo_csv = f"CARNAVAL_REDS_BOS_ENVOLVIDO_{data_consulta}_{data_term_consulta}.csv"
        arquivo_zip = f"CARNAVAL_REDS_BOS_ENVOLVIDO_{data_consulta}_{data_term_consulta}.zip"
        df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
        with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
            zipf.write(arquivo_csv)
        
        os.remove(arquivo_csv)
        
        query_8 = """
            -- 8 - REDS_BOS_VIATURAS - OK
            SELECT	
                VTR.numero_ocorrencia AS 'NUM_ATIVIDADE',
                VTR.numero_sequencial_viatura AS 'NUM_SEQ_RECURSO',
                VTR.placa AS 'NUM_PLACA',
                VTR.numero_reg AS 'NUM_PREFIXO'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia AS OCO
            LEFT JOIN db_bisp_reds_reporting.tb_viatura_ocorrencia VTR
                ON  OCO.numero_ocorrencia = VTR.numero_ocorrencia
            WHERE 1=1
            AND orgao_sigla = 'PM'
            AND OCO.nome_tipo_relatorio IN ('BOS', 'BOS AMPLO')
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND VTR.data_hora_fato BETWEEN '{}' AND '{}'
            ORDER BY VTR.data_hora_fato;
        """.format(data_inicial, data_final)
        cursor.execute(query_8)
        log_message('----Query 8 processada com sucesso!----')

        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

        # Converte as colunas em uppercase
        df.columns = [col.upper() for col in df.columns]

        arquivo_csv = f"CARNAVAL_REDS_BOS_VIATURAS_{data_consulta}_{data_term_consulta}.csv"
        arquivo_zip = f"CARNAVAL_REDS_BOS_VIATURAS_{data_consulta}_{data_term_consulta}.zip"
        df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
        with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
            zipf.write(arquivo_csv)
        os.remove(arquivo_csv)

        data_now = datetime.now()
        data_fim = data_fim + timedelta(days=1)

        if data_fim <= data_now:
            data_atual = data_fim
            log_message(f'Reiniciando protocolo para o dia {data_atual}')
        else:
            break

    # ############################################################################################
    # # O arquivo login_ftp deve ser armazenado na raiz de Automato em Documentos
    # ftp_config = []
    # with open(os.path.join(pasta_automato, 'ftp_login'), 'r') as txtfile:
    #     for linha in txtfile:
    #         host_ftp, porta_ftp, login_ftp, snh_ftp = linha.strip().split('|')
    #         ftp_config.append((host_ftp, porta_ftp, login_ftp, snh_ftp))
                
    # # Acessando os elementos dentro da primeira tupla da lista ftp_config
    # host_ftp, porta_ftp, login_ftp, snh_ftp = ftp_config[0]
    # log_message('Acessando Servidor FTP')
    
    # ############################################################################################
    # ftp_host = host_ftp
    # ftp_port = int(porta_ftp)
    # ftp_username = login_ftp
    # ftp_password = snh_ftp
    # ############################################################################################

    # # Diretório dos arquivos ZIP
    # local_dir_path = zip_dir
    # # Timeout (segundos)
    # timeout_value = 10
    # # Objeto FTP timeout
    # ftp = FTP()
    # ftp.timeout = timeout_value
    # try:
    #     # Conecta no host FTP
    #     ftp.connect(ftp_host, ftp_port)
    #     ftp.login(ftp_username, ftp_password)
    #     # Cria uma lista com todos os arquivos no diretório
    #     files = [f for f in os.listdir(local_dir_path) if os.path.isfile(os.path.join(local_dir_path, f))]
    #     # Get a list of files in the remote directory
    #     remote_files = ftp.nlst()
    #     for file in files:
    #         file_path = os.path.join(local_dir_path, file)
    #         retry_count = 0
    #         max_retries = 10
    #         if file in remote_files:
    #             log_message(f"O arquivo {file} já existe no FTP server.")
    #             continue
    #         while retry_count < max_retries:
    #             try:
    #                 # Abre e envia arquivo em modo binário
    #                 with open(file_path, 'rb') as fp:
    #                     log_message(f"Enviando arquivo: {file_path}")
    #                     ftp.storbinary(f'STOR {file}', fp)
    #                 log_message(f"Arquivo enviado: {file}")
    #                 break  # Sai do loop quando sucesso
    #             except (socket.timeout, error_perm) as e:
    #                 retry_count += 1
    #                 log_message(f"Falha na transferência de {file}: {e}. Tentando novamente...")
    # except Exception as e:
    #     log_message(f"Falha na conexão FTP: {e}")
        
except ImpalaError as e:
    log_message('***** Erro de autenticação: {e} *****')
    input('----Concluido com ERRO - Pressione Enter para fechar----')
except Exception as other_error:
    log_message(f'***** Algo deu errado - Procedimento abortado: {other_error} *****')
    input('----Automato encerrado com ERRO - Pressione Enter para fechar----')
finally:
    if ftp.sock is not None:
            ftp.quit()
    else:
       print("Erro: Conexão FTP não está disponível.")
    log_message("Concluido com sucesso. ARQUIVO LOG GERADO!")
    sys.stdout = sys.__stdout__