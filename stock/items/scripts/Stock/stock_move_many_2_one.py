# -*- coding: utf-8 -*-
import sys, simplejson
from copy import deepcopy

from stock_utils import Stock
from account_settings import *

class Stock(Stock):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.all_sku = []
        self.mf.update({
            'error_onts': '66e0cd760cc8e3fb75f23803',
            'xls_file': '66c797955cfca4851db2c3b8',
            'product_material': '66b10b87a1d4483b5369f409',
            # 'series_group':'66c75ca499596663582eed59',
            # 'num_serie': '66c75d1e601ad1dd405593fe',
        })


    def load_onts(self):
        self.read_xls_onts()

    def show_error_app(self, field_id, label, msg):
        raise Exception( simplejson.dumps({
            field_id: { 'msg': [msg], 'label': label, 'error': [] }
        }) )

    def get_inventory_records(self, warehouse_name, warehouse_location):
        """
        Se ontienen todos los inventarios que tiene el Almacen de donde se hará la salida
        """
        mango_query = {
            "selector":{
                "answers": {
                    "$and":[ 
                        { self.f['warehouse']: {'$eq': warehouse_name} },
                        { self.f['warehouse_location']: {'$eq': warehouse_location} },
                    ]
                }
            },
            "limit":10000,
            "skip":0
        }
        records_catalog = self.lkf_api.search_catalog( self.STOCK_INVENTORY_ID, mango_query )
        dict_skus = {}
        for r in records_catalog:
            productCode = r.get( self.f['product_code'] )
            productSku = r.get( self.f['sku'] )
            productLot = r.get( self.f['lot_number'] )
            productName = r.get( self.f['product_name'] )
            productCantidad = r.get( self.f['actuals'] )
            if not productCode or not productSku:
                continue
            dict_skus[ f'{productCode}_{productSku}' ] = {
                self.f['lot_number']: productLot,
                self.f['product_name']: [productName,],
                self.f['actuals']: [productCantidad,],
            }
        return dict_skus

    def share_filter(self, filter_name, uri_user, CATALOG_ID_SHARE):
        catalog_share = f"/api/infosync/get_catalogs/{CATALOG_ID_SHARE}/"
        res_share_catalog = self.share_item(uri_user, catalog_share, "can_read_item", filter_name=filter_name)
        return catalog_share
        # # Se crea y se comparte el filtro para el catalogo de Warehouse Locations
        # catalog_share = f"/api/infosync/get_catalogs/{self.WAREHOUSE_LOCATION_ID}/"
        # res_share_catalog = self.share_item(uri_user, catalog_share, "can_read_item", filter_name=filter_name)
        # print('== res_share_catalog=',res_share_catalog)
        # if res_share_catalog.get('status_code', 0) == 201:
        #     # Se crea y se comparte el filtro para el catalogo de Warehouse Locations Destinations
        #     catalog_share = f"/api/infosync/get_catalogs/{self.WAREHOUSE_LOCATION_DEST_ID}/"
        #     filter_name += 'Destination'
        #     print(f'==== Destination filter_name= {filter_name} catalog_share= {catalog_share}')
        #     res_share_catalog_destination = self.share_item(uri_user, catalog_share, "can_read_item", filter_name=filter_name)
        #     print('== res_share_catalog_destination=',res_share_catalog_destination)

    def share_filter_and_forms_to_connection(self):
        # Warehouse Name del Destination
        connection_name = self.answers.get(self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID, {}).get( self.f['warehouse_dest'] )
        # Buscar la lista de contratistas para filtrar por nombre. Voy a necesitar su ID y correo
        all_connections = self.lkf_api.get_all_connections()
        connections = { c.get('first_name'): {'id': c.get('id'), 'email': c.get('username')} for c in all_connections }
        if connections.get(connection_name):
            self.answers['id_user_to_assign'] = connections[connection_name].get('id')
            # Es una conexion, se debe crear su filtro
            uri_user = f"/api/infosync/user/{connections[connection_name].get('id')}/"
            # Filtro para Warehouse Location
            query_to_filter = { "$and": [ {self.f['warehouse']: {'$eq': connection_name}} ] }
            filter_name = connection_name.replace(' ', '')
            res_filter = self.lkf_api.create_filter(self.WH.WAREHOUSE_LOCATION_ID, filter_name, query_to_filter)
            print("== res_filter=",res_filter)
            # Si el filtro se crea correctamente se lo debo compartir al contratista
            if res_filter.get('status_code', 0) == 201:
                res_share_catalog_location = self.share_filter( filter_name, uri_user, self.WH.WAREHOUSE_LOCATION_ID )
                print('res_share_catalog_location =',res_share_catalog_location)

            # Filtro para Warehouse Location Destination
            query_to_filter = { "$and": [ {self.f['warehouse_dest']: {'$eq': connection_name}} ] }
            filter_name += 'Destination'
            res_filter = self.lkf_api.create_filter(self.WH.WAREHOUSE_LOCATION_DEST_ID, filter_name, query_to_filter)
            print("== res_filter=",res_filter)
            if res_filter.get('status_code', 0) == 201:
                res_share_catalog_location_dest = self.share_filter( filter_name, uri_user, self.WH.WAREHOUSE_LOCATION_DEST_ID )
                print('res_share_catalog_location_dest =',res_share_catalog_location_dest)

            # Se comparte la carpeta de las formas de Stock
            folder_share = f"/api/infosync/item/{self.FOLDER_FORMS_ID}/"
            res_share_folder_forms = self.share_item(uri_user, folder_share, "can_share_item")
            print('== res_share_folder_forms=',res_share_folder_forms)

    def validate_move_qty(self, product_code, sku, lot_number, warehouse, location, move_qty, date_to=None, **kwargs):
        inv = self.get_product_stock(product_code, sku=sku,lot_number=lot_number, warehouse=warehouse, location=location,  
                date_to=date_to, **kwargs)


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    status = stock_obj.answers[stock_obj.f['inv_adjust_status']]
    print('status', status)
    # if status == 'cargar_onts':
    #     stock_obj.load_onts()
    # else:
    #     stock_obj.read_xls_file()
    try:
        header, records = stock_obj.read_xls_file()
    except:
        print('no hay excel')
        header = None
        records = None
    if header:
        if not records:
            stock_obj.LKFException('El archivo cargado no contiene datos, favor de revisar')
    # print('answ3rs', stock_obj.answers)
    #stock_obj.share_filter_and_forms_to_connection()
    response = stock_obj.move_one_many_one()
    stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        }))
