import sys, simplejson
from linkaform_api import settings
from account_settings import *

from servicios_utils import ServicioMaquinariaPesada

if __name__ == "__main__":
    script_obj = ServicioMaquinariaPesada(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()

    #-FILTROS
    data = script_obj.data
    data = data.get('data',{})
    option = data.get('option','get_records')
    form_information = data.get('formInformation',{})
    gestor = data.get('gestor','')
    cliente = data.get('cliente','')
    if option == 'creation_record':
        res_create = script_obj.created_record(form_information)
        sys.stdout.write(simplejson.dumps({'status':res_create}))
    elif option == 'get_catalog':
        res_catalog = script_obj.get_information_catalogs()
        sys.stdout.write(simplejson.dumps({'data':res_catalog}))
    elif option == 'get_records':
        res_query = script_obj.get_query_events(gestor, cliente)
        sys.stdout.write(simplejson.dumps({'data':res_query}))
    else:
        sys.stdout.write(simplejson.dumps({'msg':'empty'}))
