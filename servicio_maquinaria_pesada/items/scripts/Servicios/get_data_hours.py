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
    date_from = data.get('date_ini','')
    date_to = data.get('date_fin','')
    num_order = data.get('num_od','')
    res_query_servicio = script_obj.get_query_informe_servicio_hours(date_from, date_to, num_order)
    res_query_inspection = script_obj.get_query_care_inspection(date_from, date_to)
    res_query_entrada_salida_equipos = script_obj.get_query_entrada_salida_equipos(date_from, date_to, num_order)
    res_query_post_ventas = script_obj.get_query_gestion_post_venta(date_from, date_to)
    res_query_ventas = script_obj.get_query_gestion_ventas(date_from, date_to)
    
    if num_order!='':
        sys.stdout.write(simplejson.dumps({
            'res_query_servicio':res_query_servicio,
            'res_query_entrada_salida_equipos':res_query_entrada_salida_equipos,
        }))
    else:
        sys.stdout.write(simplejson.dumps({
            'res_query_servicio':res_query_servicio,
            'res_query_inspection':res_query_inspection,
            'res_query_entrada_salida_equipos':res_query_entrada_salida_equipos,
            'res_query_post_ventas':res_query_post_ventas,
            'res_query_ventas':res_query_ventas,
        }))
