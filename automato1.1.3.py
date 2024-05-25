import schedule
import time
import os
import utils
import carga as sc

root_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(root_dir)
log_message = utils.log_message

hora_carga = '01:50'

# Agendar a tarefa no horário armazenado em hora_carga
try:
    schedule.every().day.at(hora_carga).do(sc.carga_automatica)
    while True:
        # now = datetime.now().strftime("%H:%M:%S")
        print(' -----------------------------------------------------')
        print('| GERENCIADOR DE CARGA AUTOMÁTICA - SIGOP - CGA 2024  |')
        print('| ----------------------------------------------------|')
        print(f'| Seção de Banco de Dados e Controle de Qualidade     |')
        print(f'| Este software controla a carga de RAT e BOS que     |')
        print(f'| alimento o módulo SIGOP.                            |')
        print(f'|                       -*-                           |')
        print(f'| A carga atualmente está programada para às {hora_carga}    |')
        print(' -----------------------------------------------------', end='\r\n')
        time.sleep(15)
        schedule.run_pending()
        os.system('cls')
except Exception as e:
    log_message(f'----Procedimento de carga falhou. Erro: {e}----')    
