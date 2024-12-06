# -*- coding: utf-8 -*-
import sys, simplejson
from copy import deepcopy
from datetime import datetime
from bson import ObjectId

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

    def append_onts(self, records):
        move_group = self.answers.get( self.f['move_group'], [] )
        base_row_set = deepcopy(move_group[0])
        base_row_set[self.f['move_group_qty']] = 1
        self.answers[self.f['move_group']] = []
        series_unique = []
        for num_serie in records:
            print('aqui vamso, hay que iterar y agregar las lineas')
            num_serie = self.strip_special_characters(self.unlist(num_serie))
            if not num_serie:
                continue
            if num_serie in series_unique:
                    self.LKFException( '', dict_error= {
                        f"{self.f['lot_number']}": {
                        "msg": ['Se encontraron series repetidas en el excel: {}'.format( num_serie )], 
                        "label": "Serie Repetida", "error": []}}
                        )
            series_unique.append(num_serie)
            row_set = deepcopy(base_row_set)
            row_set[self.CATALOG_INVENTORY_OBJ_ID][self.f['lot_number']] = num_serie
            self.answers[self.f['move_group']].append(row_set)
        return self.answers[self.f['move_group']] 

    def carga_materiales(self, header, records):
        #ESTA FUNCION ES MUY PARECECIDA A STOCK_MOVE_IN SOLO QUE 
        #CAMBIA EL TIPO DEL DEL CATALGOO DENTRO GRUPO REPETITIVO
        #En este movimiento se requiere que venga de stock inventroy
        header_dict = self.make_header_dict(header)
        """
        # Se revisa que el excel tenga todas las columnas que se requieren para el proceso
        """
        cols_required = ['codigo_de_producto', 'sku', 'cantidad']
        cols_not_found = self.check_keys_and_missing(cols_required, header_dict)
        if cols_not_found:
            cols_not_found = [ c.replace('_', ' ').title() for c in cols_not_found ]
            self.LKFException( f'Se requieren las columnas: {self.list_to_str(cols_not_found)}' )

        """
        # Se revisan los renglones del excel para verificar que los codigos y skus existan en el catalogo
        """
        dict_products_skus = self.get_skus_records()

        # print('++ dict_products_skus =',dict_products_skus)
        pos_codigo = header_dict.get('codigo_de_producto')
        pos_sku = header_dict.get('sku')
        pos_cantidad = header_dict.get('cantidad')

        """
        Se procesan los renglones del excel para armar los sets del grupo repetitivo
        se evalua que los codigos y skus de los productos existan en el catálogo, si alguno no existe se marca error
        """
        error_rows = []
        sets_to_products = []
        for pos_row, rec in enumerate(records):
            num_row = pos_row + 2
            product_code = rec[pos_codigo]
            sku = rec[pos_sku]
            cantidad = rec[pos_cantidad]
            if not product_code and not sku and not cantidad:
                continue
            if not product_code or not sku:
                error_rows.append(f'RENGLON {num_row}: Debe indicar el código del producto y el sku')
                continue
            if not cantidad:
                error_rows.append(f'RENGLON {num_row}: Debe indicar una cantidad')
                continue
            info_product = dict_products_skus.get( f'{product_code}_{sku}' )
            if not info_product:
                error_rows.append(f'RENGLON {num_row}: No se encontro el codigo {product_code} con el sku {sku}')
                continue
            info_product.update({
                self.f['prod_qty_per_container'] : [],
                self.f['product_code'] : str(product_code),
                self.f['sku'] : str(sku),
                self.f['lot_number']: 'LotePCI001',
            })
            
            sets_to_products.append({
                self.CATALOG_INVENTORY_OBJ_ID: info_product,
                self.f['inv_adjust_grp_status'] : "todo",
                self.f['move_group_qty'] : cantidad,
            })
        if error_rows:
            self.LKFException( self.list_to_str(error_rows) )
        if self.answers.get( self.f['move_group'] ):
            self.answers[ self.f['move_group'] ] += sets_to_products
        else:
            self.answers[ self.f['move_group'] ] = sets_to_products
        return header_dict, records

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
        acctual_containers = inv.get('actuals')
        print('acctual_containers',acctual_containers)
        if acctual_containers == 0:
            msg = f"This lot {lot_number} has 0 containers left, if this is NOT the case first do a inventory adjustment"
            msg_error_app = {
                    f"{self.f['product_lot_actuals']}": {
                        "msg": [msg],
                        "label": "Please check your lot inventory",
                        "error":[]
      
                    }
                }
            #TODO set inventory as done
            self.LKFException( simplejson.dumps( msg_error_app ) )   

        if move_qty > acctual_containers:
        # if False:
            #trying to move more containeres that there are...
            cont_diff = move_qty - acctual_containers
            msg = f"There actually only {acctual_containers} containers and you are trying to move {move_qty} containers."
            msg += f"Check this out...! Your are trying to move {cont_diff} more containers than they are. "
            msg += f"If this is the case, please frist make an inventory adjustment of {cont_diff} "
            msg += f"On warehouse {warehouse} at location {location} and lot number {lot_number}"
            msg_error_app = {
                    f"{self.f['inv_move_qty']}": {
                        "msg": [msg],
                        "label": "Please check your Flats to move",
                        "error":[]
      
                    }
                }
            self.LKFException( simplejson.dumps( msg_error_app ) )
        return True

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    status = stock_obj.answers[stock_obj.f['inv_adjust_status']]
    if hasattr(stock_obj,'folio'):
        folio = stock_obj.folio
    if not folio:
        today = stock_obj.get_today_format()
        folio = f"SAL{datetime.strftime(today, '%y%m%d')}"
        next_folio = stock_obj.get_record_folio(stock_obj.STOCK_ONE_MANY_ONE, folio)
        folio = f"{folio}-{next_folio}"

    if not stock_obj.record_id:
        stock_obj.record_id = stock_obj.object_id() 
    stock_obj.folio = folio
    # if status == 'cargar_onts':
    #     stock_obj.load_onts()
    # else:
    #     stock_obj.read_xls_file()
    #try:
    header, records = stock_obj.read_xls_file()
    # except:
    #     print('no hay excel')
    #     header = None
    #     records = None
    if header:
        if not records:
            stock_obj.LKFException('El archivo cargado no contiene datos, favor de revisar')
    # print('answ3rs', stock_obj.answers)
    #stock_obj.share_filter_and_forms_to_connection()
    response = stock_obj.move_one_many_one(records)
    stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        'metadata':{"id":stock_obj.record_id, "folio":stock_obj.folio}
        }))
