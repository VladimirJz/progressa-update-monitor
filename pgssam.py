#!/usr/bin/env python3

###########
# Imports #
###########

import os
import sys
import logging
import signal
import socket
import time
import traceback
import configparser

## pgss 
from approval_monitor import main

# log levels = CRITICAL = 50, ERROR = 40 , WARNING = 30 , INFO = 20 DEBUG = 10 NOTSET = 0
## SERVICE SETTINGS
LOG_FILE_NAME='/opt/progressa/logs/approval-monitor-service.log'
LOG_LEVEL=10
LOG_LOGGER_NAME='pgss-approval-mon'
LOG_OUTPUT_FORMAT='%(asctime)s.%(msecs)03d |[%(levelname)-8s] : %(message)s'
PROCESS_NAME='Monitor de Productos Autorizados'
###


formatter=logging.Formatter(LOG_OUTPUT_FORMAT)
file_handler = logging.FileHandler(LOG_FILE_NAME)
file_handler.setFormatter(formatter)
#logger=logging.getLogger(__name__) 
#logger.setLevel(logging.INFO)
                                           
# Get logger
logger_service = logging.getLogger(LOG_LOGGER_NAME)
logger_service.addHandler(file_handler)      
logger_service.setLevel(LOG_LEVEL)
# Setup handler

####################
# Global Variables #
####################


# Para controlar el modo de operación 
#con una variable de entorno 
if "DEBUG" in os.environ:
    # Use Environment Variable
    if os.environ["DEBUG"].lower() == "true":
        DEBUG = True
    elif os.environ["DEBUG"].lower() == "false":
        DEBUG = False
    else:
        raise ValueError("DEBUG no tiene un valor correcto'")
else:
    # si no existe la variable, usamos el mod DEBUG , 
    # si se ejecuta desde una termina, y no por systemd
    if os.isatty(sys.stdin.fileno()):
        DEBUG = True
    else:
        DEBUG = False
        

script_name = os.path.basename(__file__)

# Get logger
#logger = logging.getLogger("main")

DEBUG_INTERVAL_TIME = 20
PRODUCTION_INTERVAL_TIME = 20



# USR1 flag
USR1_KILL_SIGNAL_SET = False


########################
# Kill Signal Handler #
########################

def signal_handler(*_):
    logger_service.debug("\nExiting...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def set_usr1_flag(*_):
    global USR1_KILL_SIGNAL_SET
    USR1_KILL_SIGNAL_SET = True

# Escucha una petición KILL par lanzar el main.
signal.signal(signal.SIGUSR1, set_usr1_flag)

#############################################
# valida la ejecución de una sola instancia #
#############################################

lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)

try:
    lock_socket.bind('\0' + script_name)
except socket.error:
    logger_service.error(f"Otra instancia de {script_name} ya se encuentra en ejecución")
    sys.exit(1)




########
# Main #
########

def get_time_waited(last_run):
    """ 
        Retorna el tiempo transcurrido desde <last_run>
    """
    return time.perf_counter() - last_run
# para iniciar sin esperar:
last_run = -1000

while True:
    ###########################
    # Service loop            #
    ###########################
    time.sleep(1)

    if USR1_KILL_SIGNAL_SET:
        # Reset  flag y continua a  main()
        USR1_KILL_SIGNAL_SET = False
    elif DEBUG and get_time_waited(last_run) >= DEBUG_INTERVAL_TIME:
        # continua
        pass
    elif not DEBUG and get_time_waited(last_run) >= PRODUCTION_INTERVAL_TIME:
        # continua
        pass
    else:
        # continua esperando
        logger_service.info('...')
        #print('Waiting')
        continue

    try:
 
        logger_service.info(f'Ejecutando : {PROCESS_NAME}')
        success=main()
        #print('executed')
        if success:            
            logger_service.info('Proceso ejecutado correctamente')
        else:
            logger_service.warning(f'Ocurrio un error en la ejecución de la rutina')
        

    except Exception as e:
        logger_service.error(e)

    finally:
        last_run = time.perf_counter()
        logger_service.debug(f" {DEBUG_INTERVAL_TIME} segundos hasta la siguiente ejecución...")