import schedule
import time
import os
import utils
import carga as sc

root_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(root_dir)
log_message = utils.log_message

try:
    schedule.every().day.at(utils.hora_carga).do(sc.check_carga)
    while True:
        print(' -----------------------------------------------------')
        print('| GERENCIADOR DE CARGA AUTOMÁTICA - SIGOP - CGA 2024  |')
        print('| ----------------------------------------------------|')
        print('| Seção de Banco de Dados e Controle de Qualidade     |')
        print('| Este software controla a carga de RAT e BOS que     |')
        print('| alimento o módulo SIGOP.                            |')
        print('|                       -*-                           |')
        print(f'| A carga atualmente está programada para às {utils.hora_carga}    |')
        print(' -----------------------------------------------------', end='\r\n')
        time.sleep(1)
        schedule.run_pending()
        os.system('cls')
except Exception as e:
    log_message(f'----Procedimento de carga falhou. Erro: {e}----')    
