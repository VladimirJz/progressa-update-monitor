
###########
# Imports #
###########

import os
import sys
import logging
import signal
import socket
import time
from datetime import datetime
import traceback

import configparser
from update_monitor import test,log_path,main

####################
# Global Variables #
####################
DEBUG_WAIT_TIME = 180
PRODUCTION_WAIT_TIME = 180


ADMIN_EMAILS = ["vladimir.jz@hotmail.com"]


SINGLE_FAIL_COUNT_LIMIT=10; # Numero de Ejecuciones fallidas que se logean individualmente
FAIL_BLOCK_SIZE= 20  # despues de SINGLE_FAIL_COUNT_LIMIT  fallas en la rutina principal, se logea cada FAIL_BLOCK_SIZE eventos
                
USR1_KILL_SIGNAL_SET = False


if "DEBUG" in os.environ:
    # Use Environment Variable
    if os.environ["DEBUG"].lower() == "true":
        DEBUG = True
    elif os.environ["DEBUG"].lower() == "false":
        DEBUG = False
    else:
        raise ValueError("DEBUG environment variable not set to 'true' or 'false'")
else:
    # Use run mode
    if os.isatty(sys.stdin.fileno()):
        DEBUG = True
    else:
        DEBUG = False



LOG_OUTPUT_FORMAT='%(asctime)s.%(msecs)03d |[%(levelname)-8s] : %(message)s'
formatter=logging.Formatter(LOG_OUTPUT_FORMAT)
file_handler = logging.FileHandler('/opt/progressa/logs/update-monitor-service.log')
file_handler.setFormatter(formatter)

                                           
# Get logger
logger_service = logging.getLogger("pgssum")
logger_service.addHandler(file_handler) 
if not DEBUG:
    logger_service.setLevel(logging.INFO)
else:
    logger_service.setLevel(logging.DEBUG)
    
# Setup handler





# Script name
script_name = os.path.basename(__file__)
script_path= os.path.dirname(__file__)



logger_service.info(f'...................')
logger_service.info(f'Iniciando servicio.')
logger_service.info(f'La rutina base es [{ script_name }]')
logger_service.info(f'Work Path:{script_path}')
logger_service.info(f'Modo DEBUG ={  DEBUG }')

########################
# Kill Signal Handlers #
########################

def signal_handler(*_):
    logger_service.info("El servido ha recibido una señal <kill>.")
    logger_service.info("Deteniendo servicio")
    logger_service.info("Servicio detenido.")
    logger_service.info('=')
    sys.exit(0)




signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


#handle kill signal 
def set_usr1_flag(*_):
    global USR1_KILL_SIGNAL_SET
    logger_service.warning('Se recibió una llamada externa.')
    logger_service.warning('Recibido : [-SIGUSR1]')
    logger_service.warnig('La espera se omitira.')
    USR1_KILL_SIGNAL_SET = True

signal.signal(signal.SIGUSR1, set_usr1_flag)


lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)

try:
    lock_socket.bind('\0' + script_name)
except socket.error:
    logger_service.error(f"Existe una instancia de  {script_name} en ejecución.")
    sys.exit(1)

#############
# Functions #
#############

def get_time_waited(last_run):
    """

    """
    return time.perf_counter() - last_run

##############
# Properties #
##############




########
# Main #
########

last_run = -1000
fail_count=0


while True:
    ###########################
    # Wait Between Iterations #
    ###########################
    
    time.sleep(1)


    if USR1_KILL_SIGNAL_SET:
        USR1_KILL_SIGNAL_SET = False
    elif DEBUG and get_time_waited(last_run) >= DEBUG_WAIT_TIME:
        pass
    elif not DEBUG and get_time_waited(last_run) >= PRODUCTION_WAIT_TIME:
        pass
    else:
        logger_service.debug('En espera ...')
        continue

    try:
 
        logger_service.debug('Ejecutando rutina principal')
        if not main():
            fail_count=fail_count+1
            if(fail_count==1):
                first_fail=datetime.now()
            if(fail_count<SINGLE_FAIL_COUNT_LIMIT or fail_count % FAIL_BLOCK_SIZE==0):
                logger_service.error(f'La rutina principal devolvió un código de error (+{fail_count}).')
            if( fail_count> SINGLE_FAIL_COUNT_LIMIT and  fail_count % FAIL_BLOCK_SIZE*2==0):
                logger_service.error(f"Tiempo transcurrido: {  datetime.now() - first_fail}s ")
        else:
            fail_count=0
            logger_service.debug('Rutina principal Ejecutada')

    except Exception as e:
        logger_service.error(e)

    finally:
        last_run = time.perf_counter()
        logger_service.debug(f"Esperando [{DEBUG_WAIT_TIME}s] hasta las siguiente ejecución.")
