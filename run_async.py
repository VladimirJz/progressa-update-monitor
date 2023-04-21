
import sys
from safi.core import Request,Connector
from safi.extras import Utils
from safi.cmd  import *
import datetime
from multiprocessing import Process,Pool,TimeoutError
import functools
from requests.exceptions import ConnectTimeout,Timeout,ConnectionError,HTTPError

def progress(n):
    n=n+1
    print(n, end='\r', flush=True)
    #return n

def post_data(end_point,data):
    #print('runing_for_Task')
    #end_point='http://localhost:8000/bodesa/api/saldosdetalle/'
    r=Utils.post(data,end_point=end_point)
    #logger.info(r.reason)
    if isinstance(r,Exception):
        response={'success':False,'status_code':503,'message':'Ocurrio una Excepción al procesar la petición','data': r,'request':data}
    else:
        if r.ok :
            response={'success':r.ok,'status_code':r.status_code,'message':r.reason,'data':r.text,'request':data}
        else:
            response={'success':r.ok,'status_code':r.status_code,'message':r.reason,'data':r.text,'request':data}

    return response

def main():
    POOL_SIZE=128
    NUM_THREADS=16
    block_response=[]
    api_response=[]
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
        dummy=Utils.post({},endpoint)
    except Exception:
        logger.critical(f"El endpoint no esta disponible '{endpoint}'")
        logger.critical(f'Ejecución terminada.')
        sys.exit()



        

    try :
        db=Connector(**settings)
        is_available=db.is_available
    except Exception:
        logger.critical('La Base de datos no esta disponible.' , exc_info=debug)

    if is_available :
        logger.info('Conexión a la BD exitosa.')
        saldos_actualizados=Request.Integracion(PGSS_SALDOSDIARIOS).add(Tipo='I',Instrumento='C',OrigenID=11130)

        # print(saldos_actualizados.routine)
        # print(saldos_actualizados.parameters)
        try:
         data=db.get(saldos_actualizados)
        except Exception:
         logger.critical('Error al ejecutar rutina ',exc_info=True)
         print(data.data)
        if(data.status_code==0):
            n=0
            logger.info(f'Información generada correctamente, registros  por procesar:{data.rowcount }')
            if(data.rowcount>0):
                post_data_to=functools.partial(post_data,endpoint)
                json_rows=data.to_json()
                if True: # Utils.is_reachable(endpoint): 
                    logger.info(f'seleccionando Endpoint:  { endpoint }.')
                    success=0
                    error=0
                    logger.info(f'Comienza consumo del EndPoint.')
                    block_generator=Utils.paginate(json_rows,POOL_SIZE)
                    logger.info(f'Obteniendo bloque de datos.')
                    for data_block in block_generator:
                           #print(data_block)
                            logger.info(f'Procesando el requests pool')
                            with Pool(NUM_THREADS) as pool:
                                n= n +len(data_block)
                                api_response.append(pool.map_async(post_data_to, data_block,chunksize=5))
                                pool.close() # for async
                                #logger.info(f'Envio POST async.')
                                pool.join() # for async  
                    logger.info(f'Terminando.')
                    for block_response in  api_response:
                        try:
                            logger.info(f'Procesando Server Async response...')
                            response=block_response.get(timeout=3)
                            for r in response:
                                print(r)
                                if not r['success']:
                                    error=error+1
                                    if r['status_code']==503:
                                        logger.error(f"No se pudo procesar la peticiòn, Response: {r['status_code']} -{r['message']} ")
                                        logger.error('= payload >>\n' + r['data'])
                                        logger.error(r['request'])
                                        logger.error('<< payload =')
                                    else:
                                        logger.error(f"Petición rechazada, Response: {r['status_code']} -{r['message']} ")
                                        logger.error(f"{r['data']}")
                                        logger.error(f" \n'{r['request']} \n'")
                                else:
                                    success=success+1
                        except exceptions.Timeout as e:
                            print(e)
                            logger.error(e)
                        except exceptions.ConnectionError as e:
                            logger.error(e)
                        except exceptions.HTTPError as e:
                            logger.error(e)
                
                    end_job = datetime.datetime.now()
                    elapsed_time=end_job-start_job
                    logger.info('Procesados (async)'+ str(NUM_THREADS)+ ':'+ str(POOL_SIZE)  + '=  ' + str(data.rowcount) +' registros,   Exitosos:'  + str(success) + ', Fallidos:' + str(error)+ ', en ' +  str(elapsed_time.total_seconds())  + ' segundos.')
                    return True
                else:
                    logger.critical(f'El endPoint no esta disponible: { endpoint } ')
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






