import sys, simplejson
from datetime import datetime

from linkaform_api import settings, network, utils

from account_settings import *

from accesos_utils import Accesos


if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    all_data = simplejson.loads(sys.argv[2])
    data = all_data.get("data", {})
    option = data.get("option",'')
    location = data.get("location",'')
    curp_record= data.get("curp_record",'')
    location = data.get("location",'')
    list_data= data.get("list_data",[])
    #-CONFIGURATION
    #-EXECUTION
    if option !='':
        if option == 'query':
            lis_data = acceso_obj.get_data_query(location)
            sys.stdout.write(simplejson.dumps({"json": lis_data}))
        '''
        elif option == 'add_data' and curp_record:
            set_data_update(curp_record, list_data)
            sys.stdout.write(simplejson.dumps({"msg": "Success"}))
        elif option == 'get_items' and curp_record:
            lis_data = get_data_query_items(curp_record)
            sys.stdout.write(simplejson.dumps({"json": lis_data}))
        elif option == 'set_out' and curp_record:
            flag_out = set_out_user(curp_record,location)
            sys.stdout.write(simplejson.dumps({"msg": "Success"}))
        '''
    else:
        sys.stdout.write(simplejson.dumps({"msg": "Failed"}))
