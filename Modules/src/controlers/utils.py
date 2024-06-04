from datetime import datetime, timedelta
import logging
import os

# Obter o diretório "Documentos" do usuário atual
pasta_documentos = os.path.expanduser("~" + os.sep + "Documents")
# Criar o caminho para a pasta "Automato" dentro da pasta "Documentos"
pasta_automato = os.path.join(pasta_documentos, "Automato")
log_dir = os.path.join(pasta_automato, "Logs")
zip_dir = os.path.join(pasta_automato, "Zip")

today = datetime.now().strftime("%d-%m-%y")

hora_carga = '07:18'    # Hora inicial de tentativa de carga.
time_try = 30           # Tempo (em minutos) para a nova tentativa de carga
    
# Configurar o logging
logging.basicConfig(
    filename=os.path.join(log_dir, f'Log_carga_{today}.log'),  # Arquivo de log
    level=logging.INFO,  # Nível de log (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='[%(asctime)s] %(levelname)s: %(message)s',  # Formato das mensagens
    datefmt='%d-%m-%Y %H:%M:%S'  # Formato da data/hora
)

# Função para registrar mensagens de log
def log_message(message, level=logging.INFO):
    logging.log(level, message)  # Registra a mensagem com o nível especificado
    print(message)  # Exibe a mensagem no console (opcional)

def prorrogacao():
    global hora_carga
    hora_carga_dttime = datetime.strptime(hora_carga, '%H:%M')  # Transforma em datetime
    hora_carga_dttime += timedelta(minutes=time_try)            # Adiciona o tempo extra
    hora_carga = hora_carga_dttime.strftime('%H:%M')            # Transforma em string novamente
    print(f'O tipo de dado da hora é {type(hora_carga_dttime)}')
    return(hora_carga)

def restart_hora():
    global hora_carga
    hora_carga = '05:30'
    return(hora_carga)
