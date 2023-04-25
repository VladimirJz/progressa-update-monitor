
import sys
from safi.core import Request,Connector
from safi.extras import Utils
from safi.cmd  import *
import datetime

def log_path():
    return

def test():
    output={'success':True,'message':'ERROR'}
    print('testing...')
    return False

def main():
    def log_path():
        return log_file

    output={'success':False,'message':''}
    def save_request(args,end_point=None,success=None):
        OBJETCTYPE=3
        SERVICEID= 3
        if success:
            args['accepted_at']=datetime.datetime.now()
        
        args['date']=datetime.date.today()
        args['object_type']=OBJETCTYPE
        args['service_id']=SERVICEID
        #args['end_point']=end_point
        args['attempts']=1
        


        print(args)
        args=Utils.to_json([args])
        print(args[0])
        r=Utils.post(args[0],end_point)
        return r

    #----- Archivo de configuración
    CONFIG_FILE='/opt/progressa/srv/update-monitor/safi.cfg'
    try:
        settings=Utils.load_settings(CONFIG_FILE)
        log_settings=Utils.load_settings(CONFIG_FILE,section='LOG')
        log_file=log_settings.get('logfile','')
        service_settings=Utils.load_settings(CONFIG_FILE,section='SERVICE')
        logger=Utils.log_handler(log_file)        
        save_resposeWS=service_settings.get('saveresponseendpoint',None)
        endpoint=service_settings.get('updatebalanceendpoint')
    except Exception as e:
        print(e)
        logger.error(e)
        return False
        



    is_available=False
    start_job = datetime.datetime.now()
    debug=False
    if log_settings['servicelogtrace']==1:
        debug=True

    try :
        db=Connector(**settings)
        is_available=db.is_available
    except Exception as e:
        logger.critical('La Base de datos no esta disponible.' , exc_info=debug)
        logger.critical(e)
        return  False

    if is_available :
        logger.info('Conexión a la BD exitosa.')
        saldos_actualizados=Request.Integracion(PGSS_SALDOSINSTRUMENTO).add(Instrumento='T')
        try:
            results=db.get(saldos_actualizados)
        except Exception:
            logger.critical('Error al ejecutar rutina ',exc_info=True)
            return  False
            
        if(results.status_code==0):
            logger.info(f'Información generada correctamente, registros  por procesar:{results.rowcount }')
            if(results.rowcount>0):
                json_requests=results.to_json(key='PrestamoId')
                if not Utils.is_reachable(endpoint): 
                    logger.info(f'El EndPoint esta disponible: { endpoint }')
                    success=0
                    error=0
                    logger.info(f'Comienza consumo del EndPoint.')
                    print(json_requests)
                    for json in json_requests:
                        response={}
                        print (json)
             
                        try :
                            r=Utils.post(response['string'],end_point=endpoint)
                            if(debug):
                                logger.info(f'Petición exitosa, Endpoint Response: {r.status_code} -{r.reason}  -  elapsed at: { r.elapsed }')
                            if(not r.ok):
                                error=error+1
                                logger.warning(f'Petición rechazada, Response: {r.status_code} -{r.reason}  -  elapsed at: { r.elapsed }')
                                logger.warning(f'Respuesta: {r.text}')
                                logger.warning('= payload =========>>\n' + json['string'],)
                                logger.warning('<<===========payload=')
                            else:
                                success=success+1
                            

                        except Exception as e:
                            logger.critical('Error al procesar la peticion POST.' , exc_info=True)
                            logger.critical(e)
                            return False
                        
                        else:
                            response['status_code']=r.status_code
                            response['reason']=r.reason
                            response['message']=r.text
                            
                        response['object_key']= int(PrestamoId)
                        response['original_request']=str(json_string)
                        response['end_point']='http://10.90.0.71:28108/api/v1/ProductosProgressa/insert'

                        return False
                    end_job = datetime.datetime.now()
                    elapsed_time=end_job-start_job
                    logger.info('Procesados ' + str(results.rowcount) +' registros,   Exitosos:'  + str(success) + ', Fallidos:' + str(error)+ ', en ' +  str(elapsed_time.total_seconds())  + ' segundos.')
                    return True
                else:
                    logger.critical('El endPoint no esta disponible.')

                    return False
            else:
                logger.info('Terminado, No hay datos que procesar.')
                return True
    return True
#-------------------------------------------------------------------------
#  RUN !
#-------------------------------------------------------------------------
     
main()

#--------------------------------------------------------------------------






