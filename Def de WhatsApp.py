import pyautogui as pg
import time
import datetime
import pywhatkit as pwk
import webbrowser as wb

def whats_message(message_zap):
    group_id = 'DfndJwhfLSu33AkopmxA1S'
    message = message_zap
    pwk.sendwhatmsg_to_group_instantly(group_id, message)
    time.sleep(1)
    pwk.whats.core.close_tab()

def whats_message(message_zap):
    group_id = 'DfndJwhfLSu33AkopmxA1S'
    file_path = pasta_automato
    message = message_zap
    pwk.sendwhatmsg_to_group_file(group_id, file_path, message)
    time.sleep(1)
    pwk.whats.core.close_tab()