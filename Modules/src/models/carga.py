import os
import pandas as pd
from datetime import datetime, timedelta
import zipfile
import src.models.query as q
import src.controlers.api as api
import src.controlers.utils as utils
from src.controlers.utils import log_message
from src.views.main import update_main
import time

# regs = 4000       -> HABILITAR PARA TESTES
dt_now = datetime.now().strftime("%d/%m/%y")

def check_carga():
    cursor = api.conn_bisp()
    query_0 = q.query_0
    log_message('Conferindo carga diária da BISP...')
    cursor.execute(query_0)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    dt = df.loc[0, "datapreenc"]
    regs = df.loc[1, "cont"]
    
    #global regs  -> HABILITAR PARA TESTES
    global dt_now
    
    regs_mean = df['cont'].mean()
    regs_util = round(regs_mean-(regs_mean*0.3), 2)
    
    time.sleep(5)
        
    if dt == dt_now and regs > regs_util:
        log_message(f'Sucesso. Hoje, {dt_now}, a BISP já recebeu carga e na data de ontem temos exatamente {regs} registros carregados.')
        utils.restart_hora()
        update_main()
        carga_automatica()
    else:
        log_message(f'Erro! A BISP na data de hoje, {dt}, recebeu apenas {regs} registros. A carga será feita apenas quando este número for superior a média dos últimos 6 dias menos 30%, que hoje é aproximadamente {regs_util} registros.')
        hora_carga_dttime = datetime.strptime(utils.hora_carga, '%H:%M')     # Transforma em datetime
        hora_carga_dttime += timedelta(minutes=utils.time_try)               # Adiciona o tempo extra
        utils.hora_carga = hora_carga_dttime.strftime('%H:%M')               # Transforma em string novamente
        print(f'O tipo de dado da hora é {type(hora_carga_dttime)}')
        print(f"Hora de carga após adicionar {utils.time_try} minuto: {utils.hora_carga}")
        regs *= 4
        update_main()
        time.sleep(5)    
        os.system('cls')
        
def carga_automatica():
    print('Iniciando carga automática Sigop')

    # Obter o diretório "Documentos" do usuário atual
    pasta_documentos = os.path.expanduser("~" + os.sep + "Documents")

    # Criar o caminho para a pasta "Automato" dentro da pasta "Documentos"
    pasta_automato = os.path.join(pasta_documentos, "Automato")
    if not os.path.exists(pasta_automato): # Verificar se a pasta "Automato" já existe, se não, criar
        os.makedirs(pasta_automato)
    
    # Verificar se a pasta "Logs" já existe, se não, criar
    log_dir = os.path.join(pasta_automato, "Logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    # Verificar se a pasta "Logs" já existe, se não, criar
    zip_dir = os.path.join(pasta_automato, "Zip")
    if not os.path.exists(zip_dir):
        os.makedirs(zip_dir)

    log_message('Iniciando login na BISP')
    
    cursor = api.conn_bisp()
        
    #DATA USADA PARA NOME DOS ARQUIVOS
    data_atual = datetime.now().date()
    data_consulta = data_atual-timedelta(days=1)
    data_nome = data_consulta.strftime("%Y%m%d")
    
    # Define a hora inicial como 00:00:00
    data_inicial_tm = datetime.combine(data_consulta, datetime.min.time())
    
    # Define a hora final como 23:59:59
    data_final_tm = data_inicial_tm.replace(hour=23, minute=59, second=59)
    
    # Convertendo datetime em strings com a mesma formatação
    data_inicial = data_inicial_tm.strftime("%Y-%m-%d %H:%M:%S")
    data_final = data_final_tm.strftime("%Y-%m-%d %H:%M:%S")
    
    log_message(f'A data atual é: {data_atual}')
    log_message(f'A data de extração é: {data_nome}')
    
    os.chdir(zip_dir)
    
    log_message(f'Os arquivos serão salvos em: {os.getcwd()}')
    # A QUERY A SER CONSULTADA
    query_1 = q.query_1.format(data_inicial, data_final)
    log_message('----Iniciando processamento da query.----')
    cursor.execute(query_1)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    # Converte as colunas em uppercase
    df.columns = [col.upper() for col in df.columns]
    arquivo_csv = f"CARNAVAL_REDS_RAT_{data_nome}_{data_nome}.csv"
    arquivo_zip = f"CARNAVAL_REDS_RAT_{data_nome}_{data_nome}.zip"
    df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
    with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
        zipf.write(arquivo_csv)
    os.remove(arquivo_csv)
    log_message('----Query 1 processada com sucesso!----')
    
    query_2 = q.query_2.format(data_inicial, data_final)
    cursor.execute(query_2)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    # Converte as colunas em uppercase
    df.columns = [col.upper() for col in df.columns]
    arquivo_csv = f"CARNAVAL_REDS_RAT_EFETIVOS_{data_nome}_{data_nome}.csv"
    arquivo_zip = f"CARNAVAL_REDS_RAT_EFETIVOS_{data_nome}_{data_nome}.zip"
    df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
    with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
        zipf.write(arquivo_csv)
    os.remove(arquivo_csv)
    log_message('----Query 2 processada com sucesso!----')
    
    query_3 = q.query_3.format(data_inicial, data_final)
    cursor.execute(query_3)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    # Converte as colunas em uppercase
    df.columns = [col.upper() for col in df.columns]
    arquivo_csv = f"CARNAVAL_REDS_RAT_Produtividade_{data_nome}_{data_nome}.csv"
    arquivo_zip = f"CARNAVAL_REDS_RAT_Produtividade_{data_nome}_{data_nome}.zip"
    df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
    with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
        zipf.write(arquivo_csv)
    os.remove(arquivo_csv)
    log_message('----Query 3 processada com sucesso!----')
    
    query_4 = q.query_4.format(data_inicial, data_final)
    cursor.execute(query_4)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    # Converte as colunas em uppercase
    df.columns = [col.upper() for col in df.columns]
    arquivo_csv = f"CARNAVAL_REDS_RAT_VIATURAS_{data_nome}_{data_nome}.csv"
    arquivo_zip = f"CARNAVAL_REDS_RAT_VIATURAS_{data_nome}_{data_nome}.zip"
    df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
    with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
        zipf.write(arquivo_csv)
    os.remove(arquivo_csv)
        
    log_message('----Query 4 processada com sucesso!----')
    
    query_5 = q.query_5.format(data_inicial, data_final)
    cursor.execute(query_5)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    # Converte as colunas em uppercase
    df.columns = [col.upper() for col in df.columns]
    arquivo_csv = f"CARNAVAL_REDS_BOS_{data_nome}_{data_nome}.csv"
    arquivo_zip = f"CARNAVAL_REDS_BOS_{data_nome}_{data_nome}.zip"
    df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
    with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
        zipf.write(arquivo_csv)
    os.remove(arquivo_csv)
    log_message('----Query 5 processada com sucesso!----')
    
    query_6 = q.query_6.format(data_inicial, data_final)
    cursor.execute(query_6)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    # Converte as colunas em uppercase
    df.columns = [col.upper() for col in df.columns]
    arquivo_csv = f"CARNAVAL_REDS_BOS_EFETIVOS_{data_nome}_{data_nome}.csv"
    arquivo_zip = f"CARNAVAL_REDS_BOS_EFETIVOS_{data_nome}_{data_nome}.zip"
    df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
    with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
        zipf.write(arquivo_csv)
    os.remove(arquivo_csv)
    log_message('----Query 6 processada com sucesso!----')
    
    query_7 = q.query_7.format(data_inicial, data_final)
    cursor.execute(query_7)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    # Converte as colunas em uppercase
    df.columns = [col.upper() for col in df.columns]
    arquivo_csv = f"CARNAVAL_REDS_BOS_ENVOLVIDO_{data_nome}_{data_nome}.csv"
    arquivo_zip = f"CARNAVAL_REDS_BOS_ENVOLVIDO_{data_nome}_{data_nome}.zip"
    df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
    with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
        zipf.write(arquivo_csv)
    os.remove(arquivo_csv)
    log_message('----Query 7 processada com sucesso!----')
    
    query_8 = q.query_8.format(data_inicial, data_final)
    cursor.execute(query_8)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    # Converte as colunas em uppercase
    df.columns = [col.upper() for col in df.columns]
    arquivo_csv = f"CARNAVAL_REDS_BOS_VIATURAS_{data_nome}_{data_nome}.csv"
    arquivo_zip = f"CARNAVAL_REDS_BOS_VIATURAS_{data_nome}_{data_nome}.zip"
    df.to_csv(arquivo_csv, index=False, sep='|', encoding='utf-8')
    with zipfile.ZipFile(arquivo_zip, 'w') as zipf:
        zipf.write(arquivo_csv)
    os.remove(arquivo_csv)
    log_message('----Query 8 processada com sucesso!----')
        
    # O arquivo login_ftp deve ser armazenado na raiz de Automato em Documentos
    api.send_ftp()
    utils.restart_hora()