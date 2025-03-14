# -*- coding: utf-8 -*-
import sys, simplejson, copy
from linkaform_api import settings
from account_settings import *

from stock_greenhouse_utils import Stock

class Stock(Stock):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, folio_solicitud=folio_solicitud, sys_argv=sys_argv, use_api=use_api)
        #TODO llevarse esto a la app de stock_greenhouse#####
        self.load(module='Accesos', **self.kwargs)
        self.load(module='Product', **self.kwargs)
        self.load(module='Product', module_class='Warehouse', import_as='WH', **self.kwargs)
        self.f.update({
            'cycle_count_warehouse_name': '6442e4831198daf81456f274',
            'cycle_count_location': '65ac6fbc070b93e656bd7fbe',
            'cycle_count_product_code': '61ef32bcdf0ec2ba73dec33d',
            'cycle_count_rows': '67d32f752aa3844751c1de82',
            'cycle_count_columns': '67d32f752aa3844751c1de83',
            'cycle_count_adjust': '67d32f752aa3844751c1de84',
            'cycle_count_adjust_quantity': '67d32f752aa3844751c1de85',
            'cycle_count_total': '67d32f752aa3844751c1de86',
            'cycle_count_comment': '67d32f752aa3844751c1de87',
            'cycle_count_count_status': '67d32f752aa3844751c1de88'
        })
        #####################################################
        # Falta cambiar ids hardcodeados de la forma Cycle Count Greenhouse
        # Y llevarse las funciones a app tambien
        #####################################################

    def create_response(self, status, status_code, message="", data=[]):
        """
            Crea una respuesta estructurada para la API.

            Parámetros:
                status (str): Indica que sucedio con la petición(error, success, etc.).
                status_code (int): Código de estado HTTP de la respuesta.
                message (str, opcional): Mensaje descriptivo de la respuesta.
                data (list, opcional): Datos devueltos en la respuesta.

            Retorna:
                dict: Diccionario con el estado, código, mensaje y datos.
        """
        response = {}
        response = {
            "status": status,
            "status_code": status_code,
            "message": message,
            "data": data
        }
        return response

    def get_warehouses(self):
        selector = {"_id": {"$gt": None}}
        response = {}

        fields = ["_id", f"answers.{self.f['warehouse']}"]

        mango_query = {
            "selector": selector,
            "fields": fields,
            "limit": 50
        }

        try:
            warehouses_list = self.lkf_api.search_catalog(self.WH.CATALOG_WAREHOUSE_ID, mango_query)

            if not warehouses_list:
                response = self.create_response("success", 200, "No se encontraron Greenhouses")
                return response

            formated_warehouses_list = []
            for warehouse in warehouses_list:
                format_warehouse = {}
                if 'Greenhouse' in warehouse.get(self.f['warehouse']):
                    format_warehouse.update({
                        'id': warehouse.get('_id'),
                        'name': warehouse.get(self.f['warehouse']),
                    })
                    formated_warehouses_list.append(format_warehouse)
            response = self.create_response("success", 200, "Greenhouses obtenidos con exito", formated_warehouses_list)
            return response
        
        except Exception as e:
            response = self.create_response("error", 500, f"Error al realizar la busqueda: {e}")
            return response

    def get_locations_by_warehouse(self, warehouse):
        selector = {}

        if not warehouse:
            selector = {"_id": {"$gt": None}}
        else:
            selector.update({f"answers.{self.f['warehouse']}": warehouse})

        fields = ["_id", f"answers.{self.f['warehouse_location']}"]

        mango_query = {
            "selector": selector,
            "fields": fields,
            "limit": 500
        }

        try:
            locations_list = self.lkf_api.search_catalog(self.WH.WAREHOUSE_LOCATION_ID, mango_query)

            if not locations_list:
                response = self.create_response("success", 200, "No se encontraron Mesas en ese Warehouse")
                return response

            formated_locations_list = []
            for location in locations_list:
                format_location = {}
                format_location.update({
                    'id': location.get('_id'),
                    'table': location.get(self.f['warehouse_location']),
                })
                formated_locations_list.append(format_location)
            response = self.create_response("success", 200, "Mesas obtenidas con exito", formated_locations_list)
            return response
        
        except Exception as e:
            response = self.create_response("error", 500, f"Error al realizar la busqueda: {e}")
            return response

    def get_all_products(self):
        selector = {}

        selector = {"_id": {"$gt": None}}

        fields = ["_id", f"answers.{self.f['product_code']}", f"answers.{self.f['product_name']}"]

        mango_query = {
            "selector": selector,
            "fields": fields,
            "limit": 200
        }

        try:
            products_list = self.lkf_api.search_catalog(self.Product.PRODUCT_ID, mango_query)

            if not products_list:
                response = self.create_response("success", 200, "No se encontraron Productos")
                return response

            formated_products_list = []
            for product in products_list:
                format_product = {}
                format_product.update({
                    'id': product.get('_id'),
                    'code': product.get(self.f['product_code']),
                    'name': product.get(self.f['product_name']),
                })
                formated_products_list.append(format_product)
            response = self.create_response("success", 200, "Productos obtenidos con exito", formated_products_list)
            return response
        
        except Exception as e:
            response = self.create_response("error", 500, f"Error al realizar la busqueda: {e}")
            return response

    def get_cycle_counts(self):
        match_query = {
            "deleted_at": {"$exists": False},
            "form_id": 130586,
        }

        query = [
            {'$match': match_query},
        ]

        try:
            cycle_counts = self.format_cr(self.cr.aggregate(query))

            if not cycle_counts:
                response = self.create_response("success", 200, "No se encontraron registros en Cycle Counts")
                return response

            formated_cycle_counts_list = []
            for cycle in cycle_counts:
                format_cycle = {}
                format_cycle.update({
                    'id': cycle.get('_id'),
                    'folio': cycle.get('folio'),
                    'warehouse': cycle.get('cycle_count_warehouse_name'),
                    'location': cycle.get('cycle_count_location'),
                    'product': cycle.get('cycle_count_product_code'),
                    'total': cycle.get('cycle_count_total')
                })
                formated_cycle_counts_list.append(format_cycle)
            response = self.create_response("success", 200, "Cycle Counts obtenidos con exito", formated_cycle_counts_list)
            return response
        except Exception as e:
            response = self.create_response("error", 500, f"Error al realizar la busqueda: {e}")
            return response
            

    def add_cycle_count(self, data={}):
        if not data:
            response = self.create_response("error", 500, f"No se envio informacion para agregar el registro")
            return response
        
        metadata = self.lkf_api.get_metadata(form_id=130586)
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": "Stock",
                    "Process": "Creación de Cycle Count",
                    "Action": "add_cycle_count",
                    "File": "stock/app.py"
                }
            },
        })

        answers = {}
        try:
            for key, value in data.items():
                if key == 'warehouse_name':
                    answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID] = {
                        self.f['cycle_count_warehouse_name']: data.get('warehouse_name', ''),
                        self.f['cycle_count_location']: data.get('location', ''),
                    }
                elif key == 'product_code':
                    answers[self.Product.PRODUCT_OBJ_ID] = {
                        self.f['cycle_count_product_code']: value
                    }
                elif key == 'rows':
                    answers[self.f['cycle_count_rows']] = value
                elif key == 'columns':
                    answers[self.f['cycle_count_columns']] = value
                elif key == 'adjust':
                    answers[self.f['cycle_count_adjust']] = value
                elif key == 'adjust_quantity':
                    answers[self.f['cycle_count_adjust_quantity']] = value
                elif key == 'total':
                    answers[self.f['cycle_count_total']] = value
                elif key == 'comment':
                    answers[self.f['cycle_count_comment']] = value
                elif key == 'count_status':
                    answers[self.f['cycle_count_count_status']] = value
                else:
                    pass
            metadata.update({'answers':answers})
            request = self.lkf_api.post_forms_answers(metadata)
            print(request)
            status_code = request.get('status_code')
            if status_code == 201:
                response = self.create_response("success", 200, "Cycle Counts agregado con exito")
                return response
            else:
                data = [request.get('json')]
                response = self.create_response("error", status_code, "Cycle Counts tuvo un error al agregar el registro", data)
                return response
        except Exception as e:
            response = self.create_response("error", 500, f"Error al agregar un registro: {e}")
            return response
    
    def edit_cycle_count(self, data={}, folio=[]):
        if not data or not folio:
            response = self.create_response("error", 500, f"No se envio la informacion necesaria para actualizar el registro")
            return response
        
        answers = {}
        try:
            for key, value in data.items():
                if key == 'warehouse_name':
                    answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID] = {
                        self.f['cycle_count_warehouse_name']: data.get('warehouse_name', ''),
                        self.f['cycle_count_location']: data.get('location', ''),
                    }
                elif key == 'product_code':
                    answers[self.Product.PRODUCT_OBJ_ID] = {
                        self.f['cycle_count_product_code']: value
                    }
                elif key == 'rows':
                    answers[self.f['cycle_count_rows']] = value
                elif key == 'columns':
                    answers[self.f['cycle_count_columns']] = value
                elif key == 'adjust':
                    answers[self.f['cycle_count_adjust']] = value
                elif key == 'adjust_quantity':
                    answers[self.f['cycle_count_adjust_quantity']] = value
                elif key == 'total':
                    answers[self.f['cycle_count_total']] = value
                elif key == 'comment':
                    answers[self.f['cycle_count_comment']] = value
                elif key == 'count_status':
                    answers[self.f['cycle_count_count_status']] = value
                else:
                    pass
            if answers or folio:
                res = self.lkf_api.patch_multi_record( answers = answers, form_id=130586, folios=[folio])
                if res.get('status_code') == 201 or res.get('status_code') == 202:
                    response = self.create_response("success", 200, "Se actualizo exitosamente el registro en Cycle Count")
                    return response
                else:
                    print(res)
                    data = [res.get('json')]
                    response = self.create_response("error", 500, "Cycle Counts tuvo un error al agregar el registro", data)
                    return response
            else:
                response = self.create_response("error", 500, "No se mandarón parametros para actualizar")
                return response
        except Exception as e:
            response = self.create_response("error", 500, f"Error al editar un registro: {e}")
            return response

    def delete_cycle_count(self, folio):
        folio = [folio]
        list_records = []
        if len(folio) > 0:
            for element in folio:
                response = self.get_record_by_folio(element, 130586, select_columns={'_id':1,})
                if response.get('_id'):
                    list_records.append("/api/infosync/form_answer/"+str(response['_id'])+"/")
                else:
                    response = self.create_response("error", 404, "No se encontro el folio correspondiente")
                    return response
        else:
            response = self.create_response("error", 500, "Lista de folios vacia, envie folio")
            return response

        if len(list_records) > 0:
            request = self.Accesos.check_status_code(self.lkf_api.patch_record_list({"deleted_objects": list_records,}))
            status_code = request.get('status_code')
            if status_code == 202:
                response = self.create_response("success", 200, "Se elimino exitosamente el registro de Cycle Counts")
                return response
            else:
                response = self.create_response("error", 500, "Hubo un error al eliminar el registro de Cycle Counts")
                return response
        else:
            response = self.create_response("error", 404, "No se encontro los folios correspondiente")
            return response

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    data = stock_obj.data.get('data', {})
    option = data.get('option', '')
    warehouse = data.get('warehouse', '')
    cycle_data = data.get('cycle_data', '')
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
    else:
        # En caso de que no se seleccione una opcion
        response = stock_obj.create_response("error", 404, "No se recibio ninguna opcion")
        
    stock_obj.HttpResponse({"data": response})