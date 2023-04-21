
import sys
from safi.core import Request,Connector
from safi.extras import Utils
from safi.cmd  import *
import datetime




def main():
    is_available=False
    CONFIG_FILE='/opt/progressa/safi.cfg'
    start_job = datetime.datetime.now()
    ws_config=Utils.load_settings(CONFIG_FILE,section='WEBSERVICES')
    log_config=Utils.load_settings(CONFIG_FILE,section='LOG')
    debug=False
    if log_config['servicelogtrace']==1:
        debug=True

    endpoint=ws_config.get('bodesasaldos')
    settings=Utils.load_settings(CONFIG_FILE)        
    logger=Utils.log_handler('/opt/progressa/actualizasaldos.log')

    try :
        db=Connector(**settings)
        is_available=db.is_available
    except Exception:
        logger.critical('La Base de datos no esta disponible.' , exc_info=debug)

    if is_available :
        logger.info('Conexión a la BD exitosa.')
        #saldos_actualizados=Request.Integracion(PGSS_SALDOSDIARIOS).add(Tipo='I',Instrumento='C',OrigenID=111111130)
        saldos_actualizados=Request.Integracion(PGSS_SALDOSDIARIOS).add(Tipo='I')
        #saldos_actualizados=Request.Integracion(PGSS_SALDOSDIARIOS).add(Tipo='I',Instrumento='C',OrigenID=11130)

        logger.info(saldos_actualizados.routine)
        logger.info(saldos_actualizados.parameters)
        try:
         data=db.get(saldos_actualizados)
        except Exception:
         logger.critical('Error al ejecutar rutina ',exc_info=True)
         print(data.data)
        if(data.status_code==0):
            logger.info(f'Información generada correctamente, registros  por procesar:{data.rowcount }')
            if(data.rowcount>0):
                json=data.to_json()
                if not Utils.is_reachable(endpoint): 
                    logger.info(f'El EndPoint esta disponible: { endpoint }')
                    success=0
                    error=0
                    logger.info(f'Comienza consumo del EndPoint.')
                    for resultados in json:
                        try :
                            r=Utils.post(resultados,end_point=endpoint)
                            if(debug):
                                logger.info(f'Petición exitosa, Endpoint Response: {r.status_code} -{r.reason}  -  elapsed at: { r.elapsed }')
                            if(not r.ok):
                                error=error+1
                                logger.warning(f'Petición rechazada, Response: {r.status_code} -{r.reason}  -  elapsed at: { r.elapsed }')
                                logger.warning(f'Respuesta: {r.text}')
                                logger.warning('= payload =========>>\n' + resultados)
                                logger.warning('<<===========payload=')
                            else:
                                success=success+1

                            

                        except Exception as e:
                            logger.critical('Error al procesar la peticion POST.' , exc_info=True)
                            logger.critical(e)

                            return False
                    end_job = datetime.datetime.now()
                    elapsed_time=end_job-start_job
                    logger.info('Procesados ' + str(data.rowcount) +' registros,   Exitosos:'  + str(success) + ', Fallidos:' + str(error)+ ', en ' +  str(elapsed_time.total_seconds())  + ' segundos.')
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






