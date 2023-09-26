
import sys,os
from safi.core import Request,Connector
from safi.extras import Utils
from safi.cmd  import *
import datetime
import requests.exceptions
from multiprocessing import Process,Pool
import asyncio
import functools

import random
import time


def progress(n):
    n=n+1
    print(n, end='\r', flush=True)
    #return n

def log_path():
    return

def test():
    output={'success':True,'message':'ERROR'}
    print('testing...')
    return False


def save_request(args,end_point=None,success=None):
    OBJETCTYPE=1
    SERVICEID= 1
    if success:
        args['accepted_at']=datetime.datetime.now()
    
    args['date']=datetime.date.today()
    args['object_type']=OBJETCTYPE
    args['service_id']=SERVICEID
    #args['end_point']=end_point
    args['attempts']=1
    


    args=Utils.to_json([args])
    r=Utils.post(args[0],end_point)
    return r


def task(endpoint,request_timeout,json_request):
   
    output=[]
    response={}
    credito_id=json_request['key']
    json_string=json_request['string']


    response['object_key']=int(credito_id)
    response['original_request']=json_string
    response['end_point']=endpoint
  
    sleep_time = random.uniform(19, 22)

    time.sleep(sleep_time)
    print(sleep_time)
    
    try:
        #result=db.get(requests)
        r=Utils.post(json_string,end_point=endpoint, timeout=request_timeout)
        response['text']=r.text
        response['status_code']=r.status_code
        response['reason']=r.reason
        response['message']=r.text
        response['ok']=r.ok
        response['elapsed']=str(r.elapsed)
        
        output.append(response)

    except Exception as e:
        #print(f' call ',requests.routine,'-', requests.parameters)
        print(e)
        response['status_code']=500
        response['reason']="Task Exception"
        response['text']="Ocurrio un error al procesar la tarea ."
        response['message']= str(e)
        response['ok']=False
        output.append(response)
        return response
    
    except requests.exceptions.ReadTimeout as e:
        response['status_code']=504
        response['reason']='Timeout Error'
        response['text']=f'Se alcanzó el timeout despues de {request_timeout} segundos.'
        response['message']=e
        response['ok']=False
        output.append(response)
        return response
    
    return response


def main():
    def log_path():
        return log_file

    output={'success':False,'message':''}


    #----- Archivo de configuración
    NUM_THREADS=16
    BLOCK_SIZE=16
    CONFIG_FILE='/opt/progressa/srv/update-monitor/safi.cfg'
    request_timeout=20
    script_path= os.path.dirname(__file__)
    current_script=os.path.basename(__file__)[:-3]
    DEFAULT_LOG_FILE=os.path.join(script_path +'/'+ current_script  + '.log')
    

    
 
    print (f'Registrando salida en :{DEFAULT_LOG_FILE}')
    try:
        settings=Utils.load_settings(CONFIG_FILE)
        log_settings=Utils.load_settings(CONFIG_FILE,section='LOG')

        log_file=log_settings.get('logfile',DEFAULT_LOG_FILE)
        log_level=log_settings.get('loglevel','INFO')
        logger=Utils.log_handler(log_file,level=log_level)        
    except Exception as e:
        print ('Error el archivo de configuración no tiene el fomato correcto')
        return False

    try:
        logger.debug('Leyendo archivo de configuración')
        service_settings=Utils.load_settings(CONFIG_FILE,section='SERVICE')
        endpoint=service_settings.get('updatebalanceendpoint')
        save_response=service_settings.get('saveresponse',0)
        save_response=bool(int(save_response))
        save_req_endpoint=service_settings.get('saveresponseendpoint',None)
        service_sleep=service_settings.get('sleepat','00:00').split(':')
        service_wakeup=service_settings.get('wakeupat','00:00').split(':')

        
     



    except Exception as e:
        logger.error('Ocurrió un error al leer el archivo de configuración.')
        logger.error(e)
        print(e)
        return False
   
    request_task=functools.partial(task,endpoint,request_timeout)

    debug=False
    if log_settings['servicelogtrace']==1:
        debug=True
    
    start_job = datetime.datetime.now()
    sleep_time=start_job.replace(minute=int(service_sleep[1]),hour=int(service_sleep[0]) ,microsecond=0)
    wakeup_time=start_job.replace(minute=int(service_wakeup[1]),hour=int(service_wakeup[0]),microsecond=0)

    print(f'start job: {start_job}')
    print(f'sleep : {sleep_time}')
    print(f'wakeup : {wakeup_time}')
    print('')
    if wakeup_time<start_job<sleep_time :
        #print('Activo')
        try:
            os.remove('service.lock')
        except OSError:
            pass

        # Comienza el flujo normal de ejecución
        logger.debug('Comienza ejecución de rutina')
        is_available=False


        try :
            logger.debug('Intentando conexión a la Base de datos')
            db=Connector(**settings)
            is_available=db.is_available
        except Exception as e:
            logger.critical('La Base de datos no esta disponible.' , exc_info=debug)
            logger.critical(e)
            return  False

        if is_available :
            logger.debug('Conexión a la BD exitosa.')
            saldos_actualizados=Request.Integracion(PGSS_SALDOSINSTRUMENTO).add(Instrumento='T')
            try:
                results=db.get(saldos_actualizados)
            except Exception:
                logger.critical('Error al ejecutar rutina ',exc_info=True)
                return  False
                
            if(results.status_code==0):
                logger.debug(f'Rutina ejecutada satisfactoriamente')
                logger.debug(f'La rutina devolvió { results.rowcount  } resultados.')
                    

                if(results.rowcount>0):
                    logger.info(f'Procesando [{results.rowcount }] registros.')
                    json_requests=results.to_json(key='PrestamoId')
                    response_list=[]
                    #print(json_requests)
                    n=0
                    if True: 
                        logger.info(f'El EndPoint esta disponible: { endpoint }')
                        success=0
                        error=0
                        logger.debug(f'Comienza consumo del EndPoint.')
                        #print(json_requests)
                        #with concurrent.futures.ThreadPoolExecutor(NUM_THREADS) as executor:
                        #    futures = [executor.submit(request_task, json) for json in json_requests]
                        r#equest_results=as_completed(futures)
                        #print(type(request_results))
                        #print(request_results)
                        with Pool(NUM_THREADS) as pool:
                            n= n + len(request_list)
                            async_results=pool.map_async(request_task, json_requests,chunksize=BLOCK_SIZE, callback=progress(n))
                            pool.close() # for async
                            pool.join() # for async 
                        
                        
                        exitosos=0
                        fallidos=0
                        
                        request_results=async_results.get( timeout=25)
                        #print(type(async_results))
                        
                        for response in request_results:
                            res=future.result()
                            print(f'Procesado Registro : { res["object_key"]}, con Estatus= {res["status_code"]}')
                            logger.debug(f'Procesado InstrumentoID :{ res["object_key"] }')

                         
                            if(not res["ok"]):
                                error=error+1
                                logger.error(f'Petición rechazada: [ Endpoint response: {res["status_code"]} -{res["reason"]}  -  elapsed at: { res["elapsed"] }]')
                                logger.error(f'Respuesta: {res["status_code"]}')
                                logger.error(f'Motivo: {res["reason"]}')
                                logger.error(f'        {res["text"]}')
                                logger.error(f'=>>\n { res["original_request"]} \n<<=')
                                    
                            else:
                                success=success+1
                                logger.debug(f'Petición exitosa : [ Endpoint response: {res["status_code"]} -{res["reason"]}  -  elapsed at: { res["elapsed"] } ]')
                            
                        #             error=error+1
                            #print(res)
                            #print(type(res))
                           # block_results= res.get( timeout=60)
                      
                        ## CODIGO ORIGINAL 
                        # for json in json_requests:
                        #     print('='*20)
                        #     response={}
                        #     #print (json)
                        #     PrestamoId=json['key']
                        #     json_string=json['string']
                        
                
                        #     try :
                        #         logger.debug(f'Procesando InstrumentoID { PrestamoId }')
                        #         r=Utils.post(json_string,end_point=endpoint, timeout=request_timeout)
                        
                        #         logger.debug(f'Se obtuvo respuesta por parte del Servidor.')
                        #         response['status_code']=r.status_code
                        #         response['reason']=r.reason
                        #         response['message']=r.text
                        #         if(not r.ok):
                        #             error=error+1
                        #             logger.error(f'Petición rechazada, Response: {r.status_code} -{r.reason}  -  elapsed at: { r.elapsed }')
                        #             logger.error(f'Respuesta: {r.text}')
                        #             logger.error(f'=>>\n { json_string} \n<<=')
                                    
                        #         else:
                        #             success=success+1
                        #             logger.debug(f'Petición exitosa, Endpoint Response: {r.status_code} -{r.reason}  -  elapsed at: { r.elapsed }')

                        #     except requests.exceptions.ReadTimeout as e:
                        #         error=error+1
                        #         response['status_code']=000
                        #         response['reason']='.'
                        #         response['message']=e
                        #         logger.error(f'Se alcanzó un timeout en la petición al endpoint despues de {request_timeout}s ')
                            

                        #     except Exception as e:
                        #         error=error+1
                        #         response['status_code']=000
                        #         response['reason']='Ocurrio un error el procesar la petición.'
                        #         response['message']=e
                        #         logger.critical('Error al procesar la peticion, ocurrio un problema con la conexión.' , exc_info=True)
                        #         logger.critical(e)

                                
                        #     else:
                                        
                        #         response['object_key']= int(PrestamoId)
                        #         response['original_request']=json_string
                        #         response['end_point']=endpoint

                        #         response_list.append(response)
                        #         #print(response_list)




                        end_job = datetime.datetime.now()
                        elapsed_time=end_job-start_job
                        logger.info('☑ Procesados ' + str(results.rowcount) +' registros,   Exitosos:'  + str(success) + ', Fallidos:' + str(error)+ ', en ' +  str(elapsed_time.total_seconds())  + ' segundos.')
                        
                        an_error_occurred=False
                        #return True
                    else:
                        logger.critical('El endPoint no esta disponible.')
                        an_error_occurred=True

                        #return False
                    if save_response:
                        logger.info(u'\u2502')
                        logger.info( u'\u2514' +  u'\u2500'  +  ' Se registra en bitacora de consumos WS...')
                        error=0
                        print('logger')
                        for response in request_results:
                            try:
                                r=save_request(res,end_point=save_req_endpoint ,success=True)
                                #r=Utils.post(response ,end_point=save_req_endpoint)
                            except Exception as e:
                                error=error+1
                                if(error==1):
                                    logger.error(u'\u2514' +  u'\u2500'  +  ' Ocurrio un error al consumir el WS.')
                                logger.debug(u'\u2502')
                                logger.debug(e)
                                logger.debug(response)
                            else:
                                if not r.ok :
                                    logger.debug(r.reason)
                                    logger.debug(response)

                                    error=error+1
                        if error>0:
                            logger.info( u'\u2514' +  u'\u2500'  +  f' {error} Request en la bitacora no fueron registrados.')
                        logger.debug(u'\u2514' +  u'\u2500'  +  f' Registro en bitacora terminado.')
                    logger.info('━'*73)
                    return an_error_occurred


                else:
                    logger.info('☑ No hay datos que procesar.')
                    an_error_occurred=False
                    return True
        else: 
            return False 

        return True
        
    else: # Si la rutina entra en modo reposo
        #print('SleepMode')
        logger.debug(f'El modo reposo esta activo.')
        if os.path.exists('service.lock'):
            return True
        else:
            open('service.lock','a+')
            logger.info(f'La rutina ha entrado en modo reposo. ')
            logger.info(f'La ejecución se retomara hasta {service_wakeup }')
            #print('La rutina he entrado en modo reposo.')
            #print(f'Hasta {service_wakeup}')
            return True
        
    

    
#-------------------------------------------------------------------------
#  RUN !
#-------------------------------------------------------------------------
     
if __name__ == '__main__':
    main()


#--------------------------------------------------------------------------






