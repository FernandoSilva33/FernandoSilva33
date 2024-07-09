from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
import os

# Obter o diretório "Documentos" do usuário atual
pasta_documentos = os.path.expanduser("~" + os.sep + "Documents")

# Criar o caminho para a pasta "Automato" dentro da pasta "Documentos"
pasta_automato = os.path.join(pasta_documentos, "Automato")
log_dir = os.path.join(pasta_automato, "Logs")  # Cria pasta Logs
zip_dir = os.path.join(pasta_automato, "Zip")   # Cria pasta Zip

hora_carga = '19:43'    # Hora inicial de tentativa de carga.
time_try = 15           # Tempo (em minutos) para a nova tentativa de carga
    
log_file = os.path.join(log_dir, 'Log_carga.log')  # Nome base do arquivo

handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=21)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%d-%m-%Y %H:%M:%S')
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Função para registrar mensagens de log (adaptada)
def log_message(message, level=logging.INFO):
    logger.log(level, message)  # Usa o logger para registrar a mensagem
    print(message)  # Exibe a mensagem no console (opcional)

def prorrogacao():
    global hora_carga
    hora_carga_dttime = datetime.strptime(hora_carga, '%H:%M')  # Transforma em datetime
    hora_carga_dttime += timedelta(minutes=time_try)            # Adiciona o tempo extra
    hora_carga = hora_carga_dttime.strftime('%H:%M')            # Transforma em string novamente
    os.system('cls')
    return(hora_carga)

def restart_hora():
    global hora_carga
    hora_carga = '05:30'
    return(hora_carga)
