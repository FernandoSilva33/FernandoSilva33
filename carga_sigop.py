import schedule
import time
import subprocess
import sys
import os
from datetime import datetime

root_dir = 'c:/Automato'
os.chdir(root_dir)

def log_message(message):
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    nome_arquivo = os.path.join(root_dir, "cargaLog_{}.txt".format(now.strftime("%Y.%m.%d_%H%M%S")))
    with open(nome_arquivo, 'a') as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(message)
    
def carga_automatica():
    
    print('Iniciando carga automática Sigop')
    time.sleep(2)
    programa = 'automato.py'
    subprocess.Popen([sys.executable, programa], creationflags=subprocess.CREATE_NEW_CONSOLE)

hora_carga = '05:45'

# Agendar a tarefa no horário armazenado em hora_carga
try:
    schedule.every().day.at(hora_carga).do(carga_automatica)
    log_message('----Se houver apenas esses dizeres a carga desta data falhou----')
    while True:
        now = datetime.now().strftime("%H:%M:%S")
        print(' -----------------------------------------------------')
        print('| GERENCIADOR DE CARGA AUTOMÁTICA - SIGOP - CGA 2024  |')
        print('| ----------------------------------------------------|')
        print(f'| Seção de Banco de Dados e Controle de Qualidade     |')
        print(f'| Este software controla a carga de RAT e BOS que     |')
        print(f'| alimento o módulo SIGOP.                            |')
        print(f'|                       -*-                           |')
        print(f'| A carga atualmente está programada para às {hora_carga}    |')
        print(' -----------------------------------------------------', end='\r\n')
        time.sleep(1)
        schedule.run_pending()
        os.system('cls')
except Exception as e:
    log_message(f'----Procedimento de carga falhou. Erro: {e}----')    
