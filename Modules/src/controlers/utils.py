from datetime import datetime, timedelta
import os

# Obter o diretório "Documentos" do usuário atual
pasta_documentos = os.path.expanduser("~" + os.sep + "Documents")
# Criar o caminho para a pasta "Automato" dentro da pasta "Documentos"
pasta_automato = os.path.join(pasta_documentos, "Automato")
log_dir = os.path.join(pasta_automato, "Logs")

hora_carga = '21:09'
time_try = 1
    
def log_message(message):
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    nome_arquivo = os.path.join(log_dir, "cargaLog_{}.txt".format(now.strftime("%Y.%m.%d_%H%M%S")))
    with open(nome_arquivo, 'a') as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(message)

def prorrogacao():
    global hora_carga
    hora_carga_dttime = datetime.strptime(hora_carga, '%H:%M')  # Transforma em datetime
    hora_carga_dttime += timedelta(minutes=time_try)  # Adiciona o tempo extra
    hora_carga = hora_carga_dttime.strftime('%H:%M')  # Transforma em string novamente
    print(f'O tipo de dado da hora é {type(hora_carga_dttime)}')
    return(hora_carga)

def restart_hora():
    global hora_carga
    hora_carga = '05:30'
    return(hora_carga)
