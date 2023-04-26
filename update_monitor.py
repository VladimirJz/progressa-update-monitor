
import sys,os
from safi.core import Request,Connector
from safi.extras import Utils
from safi.cmd  import *
import datetime
import requests.exceptions

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


def main():
    def log_path():
        return log_file

    output={'success':False,'message':''}


    #----- Archivo de configuración
    CONFIG_FILE='/opt/progressa/srv/update-monitor/safi.cfg'
    request_timeout=10
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
        print ('Error')
        return False

    try:
        logger.debug('Leyendo archivo de configuración')
        service_settings=Utils.load_settings(CONFIG_FILE,section='SERVICE')
        endpoint=service_settings.get('updatebalanceendpoint')
        save_response=service_settings.get('saveresponse',0)
        save_response=bool(int(save_response))
        save_req_endpoint=service_settings.get('saveresponseendpoint',None)
    except Exception as e:
        logger.error('Ocurrió un error al leer el archivo de configuración.')
        logger.error(e)
        return False
   
        


    logger.debug('Comienza ejecución de rutina')
    is_available=False
    start_job = datetime.datetime.now()
    debug=False
    if log_settings['servicelogtrace']==1:
        debug=True

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
                if not Utils.is_reachable(endpoint): 
                    logger.info(f'El EndPoint esta disponible: { endpoint }')
                    success=0
                    error=0
                    logger.debug(f'Comienza consumo del EndPoint.')
                    #print(json_requests)
                    for json in json_requests:
                        print('='*20)
                        response={}
                        #print (json)
                        PrestamoId=json['key']
                        json_string=json['string']
                    
             
                        try :
                            logger.debug(f'Procesando InstrumentoID { PrestamoId }')
                            r=Utils.post(json_string,end_point=endpoint, timeout=request_timeout)
                    
                            logger.debug(f'Se obtuvo respuesta por parte del Servidor.')
                            response['status_code']=r.status_code
                            response['reason']=r.reason
                            response['message']=r.text
                            if(not r.ok):
                                error=error+1
                                logger.error(f'Petición rechazada, Response: {r.status_code} -{r.reason}  -  elapsed at: { r.elapsed }')
                                logger.error(f'Respuesta: {r.text}')
                                logger.error(f'=>>\n { json_string} \n<<=')
                                
                            else:
                                success=success+1
                                logger.debug(f'Petición exitosa, Endpoint Response: {r.status_code} -{r.reason}  -  elapsed at: { r.elapsed }')

                        except requests.exceptions.ReadTimeout:
                                logger.error(f'Se alcanzó un timeout en la petición al endpoint despues de {request_timeout}s ')
                            

                        except Exception as e:
                            logger.critical('Error al procesar la peticion, ocurrio un problema con la conexión.' , exc_info=True)
                            logger.critical(e)
                            response['status_code']=000
                            response['reason']='Ocurrio un error el procesar la petición.'
                            response['message']=e

                            
                        else:
                                       
                            response['object_key']= int(PrestamoId)
                            response['original_request']=json_string
                            response['end_point']=endpoint

                            response_list.append(response)
                            #print(response_list)




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
                    for response in response_list:
                        try:
                            r=save_request(response,end_point=save_req_endpoint ,success=True)
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
#-------------------------------------------------------------------------
#  RUN !
#-------------------------------------------------------------------------
     
main()

#--------------------------------------------------------------------------






