from datetime import datetime
import os

# Obter o diretório "Documentos" do usuário atual
pasta_documentos = os.path.expanduser("~" + os.sep + "Documents")
# Criar o caminho para a pasta "Automato" dentro da pasta "Documentos"
pasta_automato = os.path.join(pasta_documentos, "Automato")
log_dir = os.path.join(pasta_automato, "Logs")

hora_carga = '05:30'
time_try = 30

def log_message(message):
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    nome_arquivo = os.path.join(log_dir, "cargaLog_{}.txt".format(now.strftime("%Y.%m.%d_%H%M%S")))
    with open(nome_arquivo, 'a') as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(message)
    
def restart_hora():
    hora_carga = '05:30'
    return(hora_carga)