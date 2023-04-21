
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
    #args['end_point']=end_point
    args['attempts']=1
    


    print(args)
    args=Utils.to_json([args])
    print(args[0])
    r=Utils.post(args[0],end_point)
    return r

   


def main():
    LINEACREDITO=3
    SERVICE_ID=3
    is_available=False
    CONFIG_FILE='/opt/progressa/srv/approval-monitor/safi.cfg'
    start_job = datetime.datetime.now()
    service_settings=Utils.load_settings(CONFIG_FILE,section='SERVICE')
    db_settings=Utils.load_settings(CONFIG_FILE,section='DATABASE')
    db=Connector(**db_settings)
    lineas_autorizadas=Request.Integracion(PGSS_LINEASCREDITO).add()

    resultados=db.get(lineas_autorizadas)
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






