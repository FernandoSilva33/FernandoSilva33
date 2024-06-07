import schedule
import time
import os
from ..controlers import utils as ut
from ..models import carga as sc
from src.controlers.utils import log_message

root_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(root_dir)

def update_main():
    schedule.clear()  # Limpa o agendamento atual
    schedule.every().day.at(ut.hora_carga).do(sc.check_carga)

try:
    schedule.every().day.at(ut.hora_carga).do(sc.check_carga)
    while True:
        print(' -----------------------------------------------------')
        print('| GERENCIADOR DE CARGA AUTOMÁTICA - SIGOP - CGA 2024  |')
        print('| ----------------------------------------------------|')
        print('| Seção de Banco de Dados e Controle de Qualidade     |')
        print('| Este software controla a carga  RAT e BOS que       |')
        print('| alimento o módulo SIGOP.                            |')
        print('|                       -*-                           |')
        print(f'| A carga atualmente está programada para às {ut.hora_carga}    |')
        print(' -----------------------------------------------------', end='\r\n')
        time.sleep(1)
        schedule.run_pending()
        os.system('cls')
        update_main()
except Exception as e:
    log_message(f'Procedimento de carga falhou. Erro: {e}')
