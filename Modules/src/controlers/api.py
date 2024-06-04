from ftplib import FTP, error_perm
import os
import socket
import random
from impala.dbapi import connect, Error as ImpalaError
import sys
import src.controlers.utils as utils

log_message = utils.log_message

def conn_bisp():
    # Cria uma lista para armazenar usuário e senha
    usuarios_senhas = []
    # O arquivo credencials deve ser armazenado na raiz de Automato em Documentos
    with open(os.path.join(utils.pasta_automato, 'credencials'), 'r') as txtfile:
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
                        use_ssl=True,
                        user=usuario_esc,
                        password=passw,
                        )  
        cursor = conn.cursor()
    except ImpalaError as e:
            log_message(f'Erro de autenticação: {e}')
            log_message('---- Automato encerrado com ERRO')
    except Exception as other_error:
            log_message(f'Algo deu errado - Procedimento abortado: {other_error}')
            log_message('---- Automato encerrado com ERRO')
    return (cursor)

def send_ftp():
    ftp_config = []
    with open(os.path.join(utils.pasta_automato, 'ftp_login'), 'r') as txtfile:
        for linha in txtfile:
            host_ftp, porta_ftp, login_ftp, snh_ftp = linha.strip().split('|')
            ftp_config.append((host_ftp, porta_ftp, login_ftp, snh_ftp))
                
    # Acessando os elementos dentro da primeira tupla da lista ftp_config
    host_ftp, porta_ftp, login_ftp, snh_ftp = ftp_config[0]
    log_message('Acessando Servidor FTP')

    ############################################################################################
    ftp_host = host_ftp
    ftp_port = int(porta_ftp)
    ftp_username = login_ftp
    ftp_password = snh_ftp
    ############################################################################################

    # Diretório dos arquivos ZIP
    local_dir_path = utils.zip_dir
    # Timeout (segundos)
    timeout_value = 30
    # Objeto FTP timeout
    ftp = FTP()
    ftp.timeout = timeout_value
    try:
        # Conecta no host FTP
        ftp.connect(ftp_host, ftp_port)
        ftp.login(ftp_username, ftp_password)
        # Cria uma lista com todos os arquivos no diretório
        files = [f for f in os.listdir(local_dir_path) if os.path.isfile(os.path.join(local_dir_path, f))]
        # Get a list of files in the remote directory
        remote_files = ftp.nlst()
        for file in files:
            file_path = os.path.join(local_dir_path, file)
            retry_count = 0
            max_retries = 10
            if file in remote_files:
                log_message(f"O arquivo {file} já existe no FTP server.")
                continue
            while retry_count < max_retries:
                try:
                    # Abre e envia arquivo em modo binário
                    with open(file_path, 'rb') as fp:
                        log_message(f"Enviando arquivo: {file_path}")
                        ftp.storbinary(f'STOR {file}', fp) 
                    log_message(f"Arquivo enviado: {file}")
                    break  # Sai do loop quando sucesso
                except (socket.timeout, error_perm) as e:
                    retry_count += 1
                    log_message(f"Falha na transferência de {file}: {e}. Tentando novamente...")
    except Exception as e:
        log_message(f"Falha na conexão FTP: {e}")
    finally:
        if ftp.sock is not None:
            ftp.quit()
            utils.restart_hora()
        else:
            print("FTP Error: FTP conection is not avaliable.")
            log_message("Conclude as FTP error. Read the log file.")
            sys.stdout = sys.__stdout__
            utils.restart_hora()