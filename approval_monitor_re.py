
import sys
from safi.core import Request,Connector
from safi.extras import Utils
from safi.cmd  import *
import datetime


def save_request(args,end_point=None,success=None):
    OBJETCTYPE=3
    SERVICEID= 3
    if success:
        args['accepted_at']=datetime.datetime.now()
    
    args['date']=datetime.date.today()
    args['object_type']=OBJETCTYPE
    args['service_id']=SERVICEID
    args['attempts']=1
    


    print(args)
    args=Utils.to_json([args])
    print(args[0])
    r=Utils.post(args[0],end_point)
    return r

   


def main():
    CONFIG_FILE='/opt/progressa/srv/approval-monitor/safi.cfg'
    LINEACREDITO=3
    SERVICE_ID=3
    #load task settings
    service_settings=Utils.load_settings(CONFIG_FILE,section='SERVICE')
    safi_settings=Utils.load_settings(CONFIG_FILE,section='DATABASE')
    log_settings=Utils.load_settings(CONFIG_FILE,section='LOG')
    debug_trace=False
    if(log_settings['debugtrace']==1):
        debug_trace=True
    
    start_job = datetime.datetime.now()
    try :
        safi=Connector(**safi_settings)
        is_available=safi.is_available
    except Exception:
            logger.critical('La Base de datos no esta disponible.' , exc_info=debug)

    if is_available:
        logger.debug('Conexión a la BD exitosa.')

    lineas_autorizadas=Request.Integracion(PGSS_LINEASCREDITO).add()
    resultados=safi.get(lineas_autorizadas)
    json_request=resultados.to_json(key='idLineaCredito')
    for json in json_request:
        #print(json)
       
        json_string=json['string']
        r=Utils.post(json_string,'http://10.90.0.71:28108/api/v1/ProductosProgressa/insert')
  
        linea_credito=json['key']
        status_code=500
        reason='Fail'
        message='False'  
        response={}
        response['object_key']= int(linea_credito)
        response['status_code']=r.status_code
        response['reason']=r.reason
        response['message']=r.text
        response['original_request']=str(json_string)
        response['end_point']='http://10.90.0.71:28108/api/v1/ProductosProgressa/insert'
        print(response)
        control_endpoint='http://10.90.100.70:3000/ctrl/v1.0/outgoing-request/'
        sr=save_request(response,end_point=control_endpoint ,success=True)

    return True
#-------------------------------------------------------------------------
#  RUN !
#-------------------------------------------------------------------------
     
main()

#--------------------------------------------------------------------------






