# -*- coding: utf-8 -*-
import sys, simplejson, copy
from linkaform_api import settings
from account_settings import *

from stock_greenhouse_utils import Stock

class Stock(Stock):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, folio_solicitud=folio_solicitud, sys_argv=sys_argv, use_api=use_api)
        self.load(module='Product', **self.kwargs)
        self.load(module='Product', module_class='Warehouse', import_as='WH', **self.kwargs)
        #####################################################
        # Falta cambiar ids hardcodeados de la forma Cycle Count Greenhouse en las funciones
        #####################################################

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    data = stock_obj.data.get('data', {})
    option = data.get('option', '')
    warehouse = data.get('warehouse', '')
    search_params = data.get('search_params', {})
    cycle_data = data.get('cycle_data', {})
    folio = data.get('folio', '')

    if option == 'get_warehouses':
        # Obtener catalogo de Greenhouses
        response = stock_obj.get_warehouses()
    elif option == 'get_locations_by_warehouse':
        # Obtener catalogo de Locations a partir del Greenhouse seleccionado
        response = stock_obj.get_locations_by_warehouse(warehouse=warehouse)
    elif option == 'get_all_products':
        # Obtener catalogo de Productos
        response = stock_obj.get_all_products()
    elif option == 'get_cycle_counts':
        # Obtener la tabla de Cycle Counts
        response = stock_obj.get_cycle_counts()
    elif option == 'add_cycle_count':
        # Agregar un registro a la tabla de Cycle Counts
        response = stock_obj.add_cycle_count(data=cycle_data)
    elif option == 'edit_cycle_count':
        # Edita un registro a la tabla de Cycle Counts
        response = stock_obj.edit_cycle_count(data=cycle_data, folio=folio)
    elif option == 'delete_cycle_count':
        # Elimina un registro a la tabla de Cycle Counts
        response = stock_obj.delete_cycle_count(folio=folio)
    elif option == 'search_cycle_counts':
        # Busca registros en la tabla de Cycle Counts a partir de los parametros
        response = stock_obj.search_cycle_counts(search_params=search_params)
    else:
        # En caso de que no se seleccione una opcion
        response = stock_obj.create_response("error", 404, "No se recibio ninguna opcion")
        
    stock_obj.HttpResponse({"data": response})