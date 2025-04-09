# -*- coding: utf-8 -*-
import sys, simplejson
from copy import deepcopy
from datetime import datetime
from bson import ObjectId

from stock_utils import Stock
from account_settings import *

from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pymongo.read_preferences import ReadPreference

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

    def ejecutar_transaccion(self, new_record, folio_serie_record={}):
        # Inicia una sesión
        if self.get_enviroment() == 'prod':
            with self.client.start_session() as session:
                # Define el bloque de transacción
                #HACE TRANSACCIONES ACIDAS
                def write_records(sess):
                    if new_record.get('answers'):
                        # Crea los nuevo registro de salida de stock
                        self.records_cr.insert_one(new_record, session=sess)
                        # Inserta directamente los registros nuevos de invetarios
                        # Pone un cache para que sean calulados los inventarios una
                        # vez dentro del intenvario
                        self.direct_move_in(new_record)
                        # Pone en 0 el stock de donde salio
                        self.move_out_stock(new_record)
                try:
                    # Comienza la transacción
                    session.with_transaction(
                        write_records,
                        read_concern=ReadConcern("snapshot"),  
                        write_concern=WriteConcern("majority"),  
                        read_preference=ReadPreference.PRIMARY  
                    )
                    print("Transacción completada exitosamente.")
                except (ConnectionFailure, OperationFailure)  as e:
                    print(f"Error conexion: {e}")
                    self.LKFException( '', dict_error= {
                        f"Error": {
                        "msg": [f'Error en la conexion. {e}'], 
                        "label": "Serie Repetida", "error": []}}
                        )
        else:
            # try:
            if True:
                if new_record.get('answers'):
                    # Crea los nuevo registro de salida de stock
                    self.records_cr.insert_one(new_record)
                    # Inserta directamente los registros nuevos de invetarios
                    # Pone un cache para que sean calulados los inventarios una
                    # vez dentro del intenvario
                    response = self.direct_move_in(new_record)
                    # Pone en 0 el stock de donde salio
                    self.move_out_stock(new_record)
                    #print('response = ', response)

            # except Exception as e:
            #     print('error: ', e)
            #     series = [s['ont_serie'] for s in folio_serie_record]
            #     self.LKFException( '', dict_error= {
            #             f"{self.f['lot_number']}": {
            #             "msg": [f'INTENTA NUEVAMENTE:  Se encontraron series repetidas en el excel. '], 
            #             "label": "Serie Repetida", "error": []}}
            #             )

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
                self.f['inv_adjust_grp_status'] : "done",
                self.f['move_group_qty'] : cantidad,
            })
        if error_rows:
            self.LKFException( self.list_to_str(error_rows) )
        if self.answers.get( self.f['move_group'] ):
            self.answers[ self.f['move_group'] ] += sets_to_products
        else:
            self.answers[ self.f['move_group'] ] = sets_to_products
        return header_dict, records

    def create_records(self, groups):
        move_group = self.answers.get( self.f['move_group'], [] )
        base_row_set = deepcopy(move_group[0])
        self.answers[self.f['move_group']] = []
        base_row_set[self.f['move_group_qty']] = 1
        series_unique = []
        total_groups = len(groups)
        base_record = deepcopy(self.current_record)
        base_record.update(self.get_complete_metadata())
        base_record['answers'][self.f['move_group']] = []
        base_record["editable"] = False 
        for idx, records in enumerate(groups):
            folio_serie_record = []
            # self.answers[self.f['move_group']] = []
            new_record = {}
            new_folio = f"{self.folio}-{idx+1}/{total_groups}"
            if idx > 0:
                new_record = deepcopy(base_record)
                new_record['folio'] = new_folio
            for num_serie in records:
                # num_serie = row[ pos_serie ]
                num_serie = self.strip_special_characters(num_serie)
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
                row_set[self.STOCK_INVENTORY_OBJ_ID][self.f['lot_number']] = num_serie
                row_set[self.f['inv_adjust_grp_status']] = 'done'

                folio_serie_record.append({'folio':new_folio, 'ont_serie':num_serie})
                if idx == 0:
                    self.current_record['folio'] = new_folio
                    self.answers[self.f['move_group']].append(row_set)
                    # self.this_record_sets.append(num_serie)
                else:
                    new_record['answers'][self.f['move_group']].append(row_set)
                    new_record['answers'][self.f['inv_adjust_status']] = 'done'
            
            if new_record:
                self.ejecutar_transaccion(new_record)
                product_lot = [x['ont_serie'] for x in folio_serie_record]
            # else:
            #     self.move_out_stock(new_record)
            #     self.ejecutar_transaccion({} )
        return True
    
    def move_out_stock(self, new_record):
        record_data = self.get_record_move_data(new_record)
        updated_ids = []

        for idx, product_data in record_data['product'].items():
            product_code = product_data['product_code']
            sku = product_data['product_sku']
            product_lot = product_data['product_lot']
            move_qty = product_data.get('move_qty',0)
            match_query = self.lot_match(product_code, sku, record_data['warehouse'], record_data['location'], product_lot)
            product_db_info = self.format_cr(self.cr.find(match_query), get_one=True)
            print('match_query',match_query)
            updated_ids.append(product_db_info.get('_id'))
            print('product_db_info',product_db_info)
            product_stock = self.get_product_stock(product_code, sku=sku, lot_number=product_lot, warehouse=record_data['warehouse'], location=record_data['location'])
            acutals = product_stock.get('actuals',0)
            new_acutals = acutals - move_qty
            if new_acutals <= 0:
                status = 'done'
            else:
                status = 'active'
            update_res = self.cr.update_many(match_query, {"$set":{
                f"answers.{self.f['actual_eaches_on_hand']}":new_acutals,
                f"answers.{self.f['product_lot_actuals']}":new_acutals,
                f"answers.{self.f['move_out']}":move_qty,
                f"answers.{self.f['status']}":status,
                }})
        self.cache_set({
                        'cache_type': 'direct_move_in',
                        'inserted_ids':updated_ids,
                        })

    def get_record_move_data(self, data):
        move_group = data['answers'].get( self.f['move_group'], [] )
        warehouse_from = data['answers'].get(self.WH.WAREHOUSE_LOCATION_OBJ_ID)
        warehouse = warehouse_from.get( self.WH.f['warehouse'])
        warehouse_location = warehouse_from.get( self.WH.f['warehouse_location'])
        product_by_set = {}
        print('move_group', move_group)
        for idx, rec_set in enumerate(move_group):
            print('record_set=', rec_set)
            product_cat = rec_set.get(self.CATALOG_INVENTORY_OBJ_ID)
            product_code = product_cat.get(self.Product.f['product_code'])
            product_sku = product_cat.get(self.Product.f['product_sku'])
            product_lot = product_cat.get(self.f['product_lot'])
            move_qty = rec_set.get(self.f['move_group_qty'])
            product_by_set[idx] = {
                'product_code': product_code,
                'product_sku': product_sku,
                'product_lot': product_lot,
                'move_qty': move_qty
                }
        return {'warehouse':warehouse, 'location':warehouse_location, 'product':product_by_set}

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
            # Si el filtro se crea correctamente se lo debo compartir al contratista
            if res_filter.get('status_code', 0) == 201:
                res_share_catalog_location = self.share_filter( filter_name, uri_user, self.WH.WAREHOUSE_LOCATION_ID )

            # Filtro para Warehouse Location Destination
            query_to_filter = { "$and": [ {self.f['warehouse_dest']: {'$eq': connection_name}} ] }
            filter_name += 'Destination'
            res_filter = self.lkf_api.create_filter(self.WH.WAREHOUSE_LOCATION_DEST_ID, filter_name, query_to_filter)
            if res_filter.get('status_code', 0) == 201:
                res_share_catalog_location_dest = self.share_filter( filter_name, uri_user, self.WH.WAREHOUSE_LOCATION_DEST_ID )

            # Se comparte la carpeta de las formas de Stock
            folder_share = f"/api/infosync/item/{self.FOLDER_FORMS_ID}/"
            res_share_folder_forms = self.share_item(uri_user, folder_share, "can_share_item")

    def validate_move_qty(self, product_code, sku, lot_number, warehouse, location, move_qty, date_to=None, **kwargs):
        inv = self.get_product_stock(product_code, sku=sku,lot_number=lot_number, warehouse=warehouse, location=location,  
                date_to=date_to, **kwargs)
        acctual_containers = inv.get('actuals')
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

    def validate_onts(self, records):
        move_group = self.answers.get( self.f['move_group'], [] )
        product_lots = [r[0] for r in records]
        base_row_set = deepcopy(move_group[0])
        lot_size = len(records)
        warehouse_from = self.answers.get(self.WH.WAREHOUSE_LOCATION_OBJ_ID)
        warehouse = warehouse_from.get( self.WH.f['warehouse'])
        warehouse_location = warehouse_from.get( self.WH.f['warehouse_location'])
        product_cat = base_row_set.get(self.CATALOG_INVENTORY_OBJ_ID)
        product_code = product_cat.get(self.Product.f['product_code'])
        product_sku = product_cat.get(self.Product.f['product_sku'])
        stock_size = self.validate_lote_size(product_code, product_sku, warehouse, warehouse_location, product_lots, lot_size)
        if stock_size == 0:
            print('valid')
        elif stock_size < 0:
            print('Error hay de menos')
            self.LKFException(f'ERROR HAY {abs(stock_size)}, de menos')
        elif stock_size > 0:
            print('valido hay suficientes... pero en este caso no deberia...')
        return True

    def lot_match(self, product_code, product_sku, warehouse, location, product_lot):
        match_query ={ 
         'form_id': self.FORM_INVENTORY_ID,  
         'deleted_at' : {'$exists':False},
         } 
        match_query.update({f"answers.{self.Product.SKU_OBJ_ID}.{self.f['product_code']}":product_code})
        match_query.update({f"answers.{self.Product.SKU_OBJ_ID}.{self.f['product_sku']}":product_sku})
        match_query.update({f"answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}":warehouse})
        match_query.update({f"answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse_location']}":location})
        if type(product_lot) == str:
            match_query.update({f"answers.{self.f['product_lot']}": product_lot})
        elif type(product_lot) == list:
            match_query.update({f"answers.{self.f['product_lot']}":{"$in": product_lot}})

        return match_query

    def validate_lote_size(self, product_code, product_sku, warehouse, location, product_lot, lote_size):
        match_query = self.lot_match(product_code, product_sku, warehouse, location, product_lot)
        query = [
            {'$match': match_query},
            {'$project':{
                '_id':0,
                'product_code':f"$answers.{self.Product.SKU_OBJ_ID}.{self.f['product_code']}",
                'warehouse':f"$answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}",
                'actuals':f"$answers.{self.f['actual_eaches_on_hand']}",
            }},
            {'$group':{
                '_id':{
                    'product_code':'$product_code',
                    'warehouse':'$warehouse'
                },
                'actuals':{'$sum':'$actuals'}
            }},
            {"$project":{
                "_id":0,
                "product_code":"$_id.product_code",
                "warehouse":"$_id.warehouse",
                "actuals": "$actuals",
            }},
        ]
        stock =  self.format_cr(self.cr.aggregate(query), get_one=True)
        stock_size = stock.get('actuals',0)
        return stock_size - lote_size




if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    status = stock_obj.answers[stock_obj.f['inv_adjust_status']]
    if hasattr(stock_obj,'folio'):
        folio = stock_obj.folio
    if not folio:
        today = stock_obj.get_today_format()
        folio = "SAL"
        next_folio = stock_obj.get_record_folio(stock_obj.STOCK_ONE_MANY_ONE, folio)
        folio = f"{folio}-{next_folio}"
    if not stock_obj.record_id:
        stock_obj.record_id = stock_obj.object_id() 
    stock_obj.folio = folio
    stock_obj.folio = folio
    stock_obj.current_record['folio'] = folio
    stock_obj.answers[stock_obj.f['folio_recepcion']] = folio
    # if status == 'cargar_onts':
    #     stock_obj.load_onts()
    # else:
    #     stock_obj.read_xls_file()
    #try:

    stock_obj.set_mongo_connections()
    header, records = stock_obj.read_xls_file()
    groups = []
    if stock_obj.proceso_onts:
        groups = stock_obj.validate_onts(records)
        groups = stock_obj.do_groups(header, records)
        stock_obj.create_records(groups)
    # except:
    #     print('no hay excel')
    #     header = None
    #     records = None
    if header:
        if not records:
            stock_obj.LKFException('El archivo cargado no contiene datos, favor de revisar')
    # print('answ3rs', stock_obj.answers)
    #stock_obj.share_filter_and_forms_to_connection()
    #response = stock_obj.move_one_many_one(records)
    stock_obj.current_record['answers'] = stock_obj.answers
    if groups:
        stock_obj.folio = f"{folio}-1/{len(groups)}"
    response = stock_obj.direct_move_in(stock_obj.current_record)
    stock_obj.move_out_stock(stock_obj.current_record)
    # if not stock_obj.proceso_onts:
    #     response = stock_obj.move_one_many_one()
    stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        'metadata':{"id":stock_obj.record_id, "folio":stock_obj.folio}
        }))
