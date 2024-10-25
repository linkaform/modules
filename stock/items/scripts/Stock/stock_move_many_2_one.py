# -*- coding: utf-8 -*-
import sys, simplejson

from stock_utils import Stock

from account_settings import *

class Stock(Stock):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.mf.update({
            'error_onts': '66e0cd760cc8e3fb75f23803',
            'xls_file': '66c797955cfca4851db2c3b8',

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
        records_catalog = stock_obj.lkf_api.search_catalog( stock_obj.STOCK_INVENTORY_ID, mango_query )
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

    def read_xls_file(self):

        file_url_xls = self.answers.get( self.mf['xls_file'] )
        if not file_url_xls:
            print('no hay excel de carga masiva')
            return False
        
        file_url_xls = file_url_xls[0].get('file_url')

        """
        Para evitar que se carguen los renglones cada que editan el registro se revisa
        si ya tiene folio, entonces es una edicion y se debe revisar si en la version previa ya existia un excel
        si ya existia un excel entonces se ignora
        """

        if self.folio: 
            if self.current_record.get('other_versions'):
                # print('entra al other_versions')
                prev_version = stock_obj.get_prev_version(self.current_record['other_versions'], select_columns=[ 'answers.{}'.format( self.mf['xls_file'] ) ])
            else:
                print('Ya tiene folio pero aun no hay mas versiones... revisando el current_record en la BD')
                prev_version = stock_obj.get_record_from_db(self.form_id, self.folio, select_columns=[ 'answers.{}'.format( self.mf['xls_file'] ) ])
            print('prev_version=',prev_version)
            if prev_version.get('answers', {}).get( self.mf['xls_file'] ):
                print( 'ya hay un excel previamente cargado... se ignora en esta ejecucion =',prev_version.get('answers', {}).get( self.mf['xls_file'] ) )
                return False

        header, records = stock_obj.read_file( file_url_xls )
        header_dict = stock_obj.make_header_dict(header)

        """
        # Se revisa que el excel tenga todas las columnas que se requieren para el proceso
        """
        cols_required = ['codigo_de_producto', 'sku', 'cantidad']
        cols_not_found = stock_obj.check_keys_and_missing(cols_required, header_dict)
        if cols_not_found:
            cols_not_found = [ c.replace('_', ' ').title() for c in cols_not_found ]
            self.show_error_app( self.mf['xls_file'], 'Excel de carga masiva', f'Se requieren las columnas: {stock_obj.list_to_str(cols_not_found)}' )

        """
        Se obtiene la lista de productos que tiene el Warehouse Origen
        """
        warehouse_name = self.answers.get( self.WAREHOUSE_LOCATION_OBJ_ID, {} ).get( self.f['warehouse'], '' )
        warehouse_location = self.answers.get( self.WAREHOUSE_LOCATION_OBJ_ID, {} ).get( self.f['warehouse_location'], '' )
        dict_products_inventory = self.get_inventory_records(warehouse_name, warehouse_location)
        # print(simplejson.dumps(dict_products_inventory, indent=4))

        print('++ records =',records)
        pos_codigo = header_dict.get('codigo_de_producto')
        pos_sku = header_dict.get('sku')
        pos_cantidad = header_dict.get('cantidad')

        """
        Se procesan los renglones del excel para armar los sets del grupo repetitivo
        se evalua que los codigos y skus de los productos existan en el catálogo, si alguno no existe se marca error
        """
        error_rows = []
        sets_to_products = []
        dict_totales_move = {}
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
            product_code_sku = f'{product_code}_{sku}'
            info_product = dict_products_inventory.get( product_code_sku )
            if not info_product:
                error_rows.append(f'RENGLON {num_row}: No se encontro el codigo {product_code} con el sku {sku}')
                continue

            if not dict_totales_move.get(product_code_sku):
                dict_totales_move[product_code_sku] = 0
            dict_totales_move[product_code_sku] += cantidad

            info_product.update({
                self.f['product_code'] : str(product_code),
                self.f['sku'] : str(sku),
            })
            
            sets_to_products.append({
                self.STOCK_INVENTORY_OBJ_ID: info_product,
                self.f['move_group_qty'] : cantidad,
            })
        if error_rows:
            self.show_error_app( self.mf['xls_file'], 'Excel de carga masiva', stock_obj.list_to_str(error_rows) )
        if self.answers.get( self.f['move_group'] ):
            self.answers[ self.f['move_group'] ] += sets_to_products
        else:
            self.answers[ self.f['move_group'] ] = sets_to_products

        """
        Se revisa que las cantidades no pasen de la cantidad disponible en el stock
        """
        error_cantidades = []
        for prod_sku, cant_solicitada in dict_totales_move.items():
            p = prod_sku.split('_')[0]
            s = prod_sku.split('_')[1]
            cant_disponible = stock_obj.unlist( dict_products_inventory.get( prod_sku, {} ).get( self.f['actuals'], 0 ) )
            if cant_solicitada > cant_disponible:
                error_cantidades.append( f'Producto: {p} SKU: {s} la cantidad solicitada {cant_solicitada} es mayor a la cantidad disponible {cant_disponible}' )
        if error_cantidades:
            self.show_error_app( self.mf['xls_file'], 'Excel de carga masiva', stock_obj.list_to_str(error_cantidades) )

        # self.show_error_app( 'folio', 'Folio', 'En Pruebas!' )

    def read_xls_onts(self):
        data_xls = self.read_xls( self.mf['xls_file'] )
        if not data_xls:
            return False
        header = data_xls.get('header')
        records = data_xls.get('records')
        header_dict = stock_obj.make_header_dict(header)
        """
        # Se revisa que el excel tenga todas las columnas que se requieren para el proceso
        """
        cols_required = ['serie_ont']
        cols_not_found = stock_obj.check_keys_and_missing(cols_required, header_dict)
        if cols_not_found:
            cols_not_found = [ c.replace('_', ' ').title() for c in cols_not_found ]
            self.LKFException( f'Se requieren las columnas: {stock_obj.list_to_str(cols_not_found)}' )

        pos_serie = header_dict.get('serie_ont')
        warehouse_name = self.answers.get( self.WAREHOUSE_LOCATION_OBJ_ID, {} ).get( self.f['warehouse'], '' )
        warehouse_location = self.answers.get( self.WAREHOUSE_LOCATION_OBJ_ID, {} ).get( self.f['warehouse_location'], '' )

        """
        mango_query = {"selector":{"answers": {"$and":[ 
            {"6442e4831198daf81456f274":{"$eq":"Almacen Distribuidor"}},
            {"65ac6fbc070b93e656bd7fbe":{"$eq":"PCI Guadalajara"}},
            {"620a9ee0a449b98114f61d77":{"$in": ["HWTC82C3D9AF","HWTC82D990AF","HWTC82CA9123"]}}
        ]}},
        "limit":20000,"skip":0}
        """
        list_onts = [ r[ pos_serie ] for r in records if r[pos_serie] ]
        if not list_onts:
            return False
        mango_query = {"selector":{"answers": {"$and":[ 
            {self.f['warehouse']: {"$eq": warehouse_name}},
            {self.f['warehouse_location']: {"$eq": warehouse_location}},
            {self.f['lot_number']: {"$in": list_onts}}
        ]}},
        "limit":20000,"skip":0}
        records_inventory = self.lkf_api.search_catalog( self.STOCK_INVENTORY_ID, mango_query )
        if not records_inventory:
            self.show_error_app(self.mf['xls_onts'], 'Excel de carga ONTs', 'No se encontró ninguna ONT')

        """
        Se preparan los sets para el grupo repetitivo de productos
        """
        sets = []
        not_found = []
        for record_inv in records_inventory:
            cap_serie = record_inv.get( self.mf['capture_num_serie'] )
            if type(cap_serie) == str:
                cap_serie = [cap_serie,]
            num_serie_found = record_inv.get( self.f['lot_number'] )
            actuals = record_inv.get( self.f['actuals'] )
            product_code = record_inv.get( self.f['product_code'] )
            sku = record_inv.get( self.f['sku'] )
            
            try:
                is_valid = self.validate_move_qty(product_code, sku, num_serie_found, warehouse_name, warehouse_location, 1)
            except:
                is_valid = False
                not_found.append([num_serie_found, f'No existe disponibilidad en el almacen de esta ONT: {num_serie_found}'])
                list_onts.remove( num_serie_found )

            if is_valid:
                set_line = {
                 self.CATALOG_INVENTORY_OBJ_ID: {
                    self.f['product_code']: product_code,
                    self.f['sku']: sku,
                    self.f['lot_number']: num_serie_found,
                    self.f['product_name']: [record_inv.get( self.f['product_name'] ),],
                    self.f['actuals']: [record_inv.get( self.f['actuals'] ),],
                    self.f['product_type']: [record_inv.get( self.f['product_type'] ),],
                    self.mf['capture_num_serie']: cap_serie,
                },
                 self.f['move_qty_requested']: 1,
                 self.f['move_group_qty']: 1,
                }
                if num_serie_found in list_onts:
                    list_onts.remove( num_serie_found )
                sets.append(set_line)


        if sets:
            self.answers[self.f['move_group']] = sets
        if list_onts or not_found:
            for ont in list_onts:
                not_found.append([ont, f'No se encontró ONT: {ont} en el Inventario'])

            not_valid_xls = self.lkf_api.make_excel_file(['Serie ONT', 'Error',], not_found, self.current_record['form_id'], self.mf['error_onts'])
            if not_valid_xls:
                self.answers.update({self.mf['error_onts']:[ not_valid_xls[self.mf['error_onts']], ]})

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
        connection_name = self.answers.get(self.WAREHOUSE_LOCATION_DEST_OBJ_ID, {}).get( self.f['warehouse_dest'] )
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
            res_filter = self.lkf_api.create_filter(self.WAREHOUSE_LOCATION_ID, filter_name, query_to_filter)
            print("== res_filter=",res_filter)
            # Si el filtro se crea correctamente se lo debo compartir al contratista
            if res_filter.get('status_code', 0) == 201:
                res_share_catalog_location = self.share_filter( filter_name, uri_user, self.WAREHOUSE_LOCATION_ID )
                print('res_share_catalog_location =',res_share_catalog_location)

            # Filtro para Warehouse Location Destination
            query_to_filter = { "$and": [ {self.f['warehouse_dest']: {'$eq': connection_name}} ] }
            filter_name += 'Destination'
            res_filter = self.lkf_api.create_filter(self.WAREHOUSE_LOCATION_DEST_ID, filter_name, query_to_filter)
            print("== res_filter=",res_filter)
            if res_filter.get('status_code', 0) == 201:
                res_share_catalog_location_dest = self.share_filter( filter_name, uri_user, self.WAREHOUSE_LOCATION_DEST_ID )
                print('res_share_catalog_location_dest =',res_share_catalog_location_dest)

            # Se comparte la carpeta de las formas de Stock
            folder_share = f"/api/infosync/item/{self.FOLDER_FORMS_ID}/"
            res_share_folder_forms = self.share_item(uri_user, folder_share, "can_share_item")
            print('== res_share_folder_forms=',res_share_folder_forms)




if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    status = stock_obj.answers[stock_obj.f['inv_adjust_status']]
    if status == 'cargar_onts':
        stock_obj.load_onts()
    else:
        stock_obj.read_xls_file()
    stock_obj.share_filter_and_forms_to_connection()
    response = stock_obj.move_one_many_one()
    stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        }))
