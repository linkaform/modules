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
    date_from = data.get('date_ini','2026-05-01')
    date_to = data.get('date_fin','2026-05-19')
    res_query_servicio = script_obj.get_query_informe_servicio_gastos(date_from, date_to)
    res_query_comercial = script_obj.get_query_comercial(date_from, date_to)
    return_data = res_query_servicio + res_query_comercial
    sys.stdout.write(simplejson.dumps({'data':return_data}))
