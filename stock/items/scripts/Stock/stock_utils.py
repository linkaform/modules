# -*- coding: utf-8 -*-

from sre_parse import BRANCH
import sys, simplejson, time
from bson import ObjectId
from datetime import datetime, timedelta, date
from copy import deepcopy

from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pymongo.read_preferences import ReadPreference

from lkf_addons.addons.stock.app import Stock

today = date.today()
year_week = int(today.strftime('%Y%W'))


class Stock(Stock):
    """
    Stock management utility class that extends the base Stock class.
    Provides additional functionality for inventory management, stock movements,
    and ONTs (Order Number Tracking) processing.
    
    Attributes:
        proceso_onts (bool): Flag indicating if ONTs processing is enabled
        FORM_CATALOG_DIR (dict): Mapping between form IDs and catalog IDs
        f (dict): Form field mappings
        answer_label (dict): Labels for form answers
        FOLDER_FORMS_ID (str): ID of the stock forms folder
        mf (dict): Media field mappings
        max_sets (int): Maximum number of records per set for processing
    """

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        """
        Initialize the Stock utility class.
        
        Args:
            settings (dict): Configuration settings
            folio_solicitud (str, optional): Request folio number
            sys_argv (list, optional): System arguments
            use_api (bool, optional): Flag to indicate API usage
            **kwargs: Additional keyword arguments
        """
        super().__init__(settings, folio_solicitud=folio_solicitud, sys_argv=sys_argv, use_api=use_api)
        self.load('JIT', **self.kwargs)
        self.proceso_onts = False
        self.warehouse_from = None
        self.location_from = None

        # La relacion entre la forma de inventario y el catalogo utilizado para el inventario
        # por default simpre dejar los mismos nombres
        self.FORM_CATALOG_DIR = {
            self.FORM_INVENTORY_ID: self.CATALOG_INVENTORY_ID,
        }

        # Initialize form field mappings
        self.f.update({
            'evidencia': '66b568e374a06756f51954b2',
            'evidencia_salida': '671fc9b70df25c3156a8d458',
            'parts_group': '62c5da67f850f35cc2483346',
            'fecha_recepcion': '000000000000000000000111',
            'fecha_recepcion_inventario': '65dfecbf3199f9a04082955e',
            'folio_recepcion': '679bd3e1e213a8e4c124c05c',
            'observaciones_stock': '6799c243cf81bd103124bf51',
            'observaciones_move_stock': '66b561232c91d2011147118c',
        })

        # Initialize labels and form IDs
        self.answer_label = self._labels()
        self.FOLDER_FORMS_ID = self.lkm.item_id('Stock', 'form_folder').get('id')
        self.mf = {
            'xls_file': '66c797955cfca4851db2c3b8',
            'xls_onts': '66e0cd760cc8e3fb75f23803',
            'capture_num_serie': '66c75e0c0810217b0b5593ca'
        }
        self.max_sets = 2500

    def do_groups(self, header, records):
        """
        Process records in groups based on ONTs (Order Number Tracking).
        
        Args:
            header (dict): Header information containing column mappings
            records (list): List of records to process
            
        Returns:
            list: List of record groups, each containing up to max_sets records
            
        Raises:
            LKFException: If product and SKU information is missing
        """
        pos_serie = header.get('serie_ont')
        error_rows = []
        move_group = self.answers.get(self.f['move_group'], [])
        
        if move_group:
            onts = [x[pos_serie] for x in records]
            groups = [onts[i:i + self.max_sets] for i in range(0, len(onts), self.max_sets) ]
            print('records=', len(groups))
        else:

            self.LKFException( '', dict_error= {
                        "msg": 'Favor de indicar el numero de producto y sku a procesar en la carga de ONTS'} 
                )
        return groups

    def get_record_metadata(self, form_id=None, metadata={}, pop_common=True):
        now = datetime.now()
        if not metadata:
            metadata = deepcopy(self.current_record)
        if pop_common:
            metadata.pop('_id', None)
            metadata.pop('form_id', None)
            metadata.pop('answers', None)
            metadata.pop('voucher_id', None)
            metadata.pop('other_versions', None)
            metadata.pop('index', None)
            metadata.pop('folio', None)
            metadata.pop('record_id', None)
        if form_id:
            metadata['form_id'] = form_id
        metadata['start_date'] = now
        metadata['end_date'] = now
        metadata['start_timestamp'] = time.time()
        metadata['end_timestamp'] = time.time()
        metadata['updated_at'] = now
        return metadata

    def direct_move_math(self, answers, move_in=0, move_out=0, product_stock={}, stock_pack=1):
        stock_move_in = product_stock.get('stock_in', answers.get(self.f['move_in'], 0))
        stock_move_out = product_stock.get('stock_out', answers.get(self.f['move_out'], 0))
        stock_sale = product_stock.get('sales', answers.get(self.f['product_lot_sales'], 0))
        stock_cuarentin = product_stock.get('cuarentin', answers.get(self.f['product_lot_cuarentin'], 0))
        stock_scrapped = product_stock.get('scrapped', answers.get(self.f['product_lot_scrapped'], 0))
        stock_adjustments = product_stock.get('adjustments', answers.get(self.f['product_lot_adjustments'], 0))
        actuals = product_stock.get('actuals', answers.get(self.f['actuals'], 0))

        answers[self.f['product_lot_produced']] = answers.get(self.f['product_lot_produced'], 0)
        answers[self.f['move_in']] = stock_move_in + move_in
        answers[self.f['move_out']] = stock_move_out + move_out
        answers[self.f['product_lot_sales']] = stock_sale
        answers[self.f['product_lot_cuarentin']] = stock_cuarentin
        answers[self.f['product_lot_scrapped']] = stock_scrapped
        answers[self.f['product_lot_adjustments']] = stock_adjustments
        answers[self.f['actuals']] = actuals + move_in - move_out
        answers[self.f['actual_eaches_on_hand']] = (stock_pack*actuals) + move_in - move_out
        new_actuals = answers[self.f['actuals']]
        if new_actuals <= 0:
            status = 'done'
        else:
            status = 'active'
        answers[self.f['status']] = status   
        return answers

    def direct_move_in(self, new_record):
        """
        Crea entrda deirecta a stock invetory
        Se corren validaciones

        args: Registro del movimeinto

        return: res que puede ser un diccionario con idx de la linea del grupo repetitivo
        y el resultado de cada movimiento
        si se creo un nuevo registro regresa

        {idx:
            {
            'new_id':true,
            'folio':xx,
            }}
        si es una edicion
        {idx:
            {
            'acknowledged':true,
            'modified_count':1,
            'folio':xx,
            }}
        si es un proceso de ont
        {
            'acknowledged':true,
            'modified_count':1,
        }
        """
        now = datetime.now()
        res = {}
        #solo se usa en pci y en pruebas de exposion de materiales
        # print('emntra a direct move in....', simplejson.dumps(self.answers, indent=4))
        answers = self._labels(data=new_record)
        self.answer_label = self._labels()
        warehouse = self.answer_label['warehouse']
        if not self.warehouse_from:
            self.warehouse_from = warehouse
        location = self.answer_label['warehouse_location']
        if not self.location_from:
            self.location_from = location
        warehouse_to = self.answer_label['warehouse_dest']
        location_to = self.answer_label['warehouse_location_dest']
        move_lines = answers['move_group'] 
        # Información original del Inventory Flow
        status_code = 0
        move_locations = []
        folios = []
        data_from = {'warehouse':warehouse, 'warehouse_location':location}
        new_records_data = []
        metadata = self.get_record_metadata(self.FORM_INVENTORY_ID)
        metadata['created_at'] = now
        metadata['updated_at'] = now
        new_stock_records = []

        folio = new_record['folio']
        new_stock_records2, update_stock_records = self.direct_move_in_line_validations(folio, metadata, move_lines)
        new_records = []
        update_records = []
        for idx, this_record in new_stock_records2.items():
            if this_record:
                new_records.append(this_record)
        if new_records:
            if self.get_enviroment() == 'prod':
                with self.client.start_session() as session:
                    # Define el bloque de transacción
                    #HACE TRANSACCIONES ACIDAS
                    def write_records(sess):
                        try:
                            return self.cr.insert_many(new_records)
                        except Exception as e:
                            print(f"Error en la creacion de registros de Stock: {e}")
                            self.LKFException(f" Error en la creacion de registros de Stock.")
                    try:
                        # Comienza la transacción
                        res_new_stock = session.with_transaction(
                            write_records,
                            read_concern=ReadConcern("snapshot"),  
                            write_concern=WriteConcern("majority"),  
                            read_preference=ReadPreference.PRIMARY  
                        )
                        print("Transacción completada exitosamente.")
                    except (ConnectionFailure, OperationFailure)  as e:
                        print(f"Error conexion: {e}")
                        self.LKFException( f'Error en la conexion. {e}' )

            else:
                #Se insertan los nuevos registros a la base de datos
                try:
                    res_new_stock = self.cr.insert_many(new_records)
                    print('res_new_stock', res_new_stock)
                except Exception as e:
                    self.LKFException(f" Error en la creacion de registros de Stock.")
            sync_ids = []
            for idx, rec_idx in enumerate(res_new_stock.inserted_ids):
                res[idx] = {
                    'new_id':rec_idx,
                    'folio':new_records[idx]['folio'],
                    }
                sync_ids.append(str(rec_idx))
            print('syncing catalogs.... =', len(sync_ids))
            self.lkf_api.sync_catalogs_records({"catalogs_ids":  [self.CATALOG_INVENTORY_ID],"form_answers_ids":  sync_ids, "status": "created"})
        for idx, this_record in update_stock_records.items():
            if this_record:
                update_records.append(this_record)

        if update_records:
            sync_ids = []
            for idx, record in update_stock_records.items():
                if not record:
                    continue
                this_res = self.cr.update_one({'_id':record['_id']}, {'$set':record})
                res[idx] = {
                    'acknowledged': this_res.acknowledged, 
                    'modified_count': this_res.modified_count, 
                    'folio':record['folio']}
                sync_ids.append(str(record['_id']))
            print('updating syncing catalogs.... =', len(sync_ids))
            self.lkf_api.sync_catalogs_records({
                'catalogs_ids':[self.CATALOG_INVENTORY_ID],
                'form_answers_ids':sync_ids,
                'status':'edited',
                })

            # res_update_stock = self.cr.update_many(update_stock_records)
        # if self.proceso_onts:
        #     if new_stock_records2:
        #         print('new_stock_records2', new_stock_records2)
        #         print('new_stock_records2', new_stock_records2d)
        #         res = self.cr.insert_many(new_stock_records2)
        #         self.cache_set({
        #                         'cache_type': 'direct_move_in',
        #                         'inserted_ids':res.inserted_ids,
        #                         })
        #         print('res.inserted_ids', res.inserted_ids)
        #         return res.inserted_ids
        return res

    def validate_move_qty_onts(self, warehouse, location, move_lines, move_type='out'):
        """
        Realiza validaciones de salida y movimeinto de inventarios y complemente informacion
        de embarque

        args:
            folio, es el folio del registro
            metadata: diccionario con metadata de registro padre
            move_lines: son las lineas de moviemintos
        """
        product_lots = []
        for moves in move_lines:
            product_code = moves['product_code']
            product_sku = moves['sku']
            product_lot = moves['product_lot']
            product_lots.append(product_lot)
        stock_size = self.get_direct_stock(product_code, product_sku, warehouse, location, product_lots)
        
        if stock_size != len(product_lots) and move_type == 'out':
            msg = f"La cantidad de series de salida con coincide con la cantidad en inventario"
            msg_error_app = {
                    f"{self.f['product_lot_actuals']}": {
                        "msg": [msg],
                        "label": "Please check your lot inventory",
                        "error":[]
      
                    }
                }
            #TODO set inventory as done
            self.LKFException( simplejson.dumps( msg_error_app ) )  
        
    def direct_move_in_line_validations(self, folio, metadata, move_lines):
        """
        Realiza validaciones de salida y movimeinto de inventarios y complemente informacion
        de embarque

        args:
            folio, es el folio del registro
            metadata: diccionario con metadata de registro padre
            move_lines: son las lineas de moviemintos
        """
        new_stock_records2 = {}
        update_stock_records = {}
        skus = self.get_group_skus(move_lines)
        warehouse = self.answer_label['warehouse']
        location = self.answer_label['warehouse_location']
        warehouse_to = self.answer_label['warehouse_dest']
        location_to = self.answer_label['warehouse_location_dest']
        fecha_recepcion = self.answers.get(self.f['fecha_recepcion'])
        observaciones = self.answers.get(self.f['stock_move_comments'])
        folio_recepcion = self.base_folio #self.answers.get(self.f['folio_recepcion'])
        wh_type = self.WH.warehouse_type(warehouse)
        if self.proceso_onts:
            self.validate_move_qty_onts(warehouse, location, move_lines, move_type='in')
        for idx, moves in enumerate(move_lines):
            update_stock_records[idx] = []
            new_stock_records2[idx] = []
            status = moves.get('inv_adjust_grp_status')
            if status == 'done':
                continue
            move_qty = moves.get('move_group_qty', 0)
            if wh_type.lower() == 'stock' and self.proceso_onts == False:
                # El producto esta ingresando desde un almacen tipo stock se deve de validar su existenica
                vals = self.validate_move_qty(moves['product_code'], moves['sku'], moves['product_lot'], warehouse, location, move_qty)
            
            this_metadata = deepcopy(metadata)
            if moves.get('folio'):
                this_metadata['folio'] = moves['folio']
            else:
                this_metadata['folio'] = f'{folio}-{idx+1}'
            # records = self.cr.find({'form_id':self.FORM_INVENTORY_ID, 'folio':folio},{'folio':1})
            # folio_exists
            # move_line = self.answers[self.f['move_group']][idx]
            answers = self.stock_inventory_model(moves, skus[moves['product_code']], labels=True)
            answers.update(self.direct_move_math(answers, move_in= moves['move_group_qty']))
            answers.update({
                self.WH.WAREHOUSE_LOCATION_OBJ_ID:{
                self.f['warehouse']:warehouse_to,
                self.f['warehouse_location']:location_to},
                self.f['product_lot']:moves['product_lot'],
                self.f['fecha_recepcion_inventario']: fecha_recepcion,
                self.f['observaciones_stock']: observaciones,
                self.f['folio_recepcion']: folio_recepcion,
                    }
                )
            this_metadata['answers'] = answers

            if self.proceso_onts:
                #ONTS....
                new_stock_records2[idx] = this_metadata
                continue
            exists = self.product_stock_exists(
                            product_code=moves['product_code'], 
                            sku=moves['sku'], 
                            lot_number=moves['product_lot'], 
                            warehouse=warehouse_to, 
                            location=location_to)
            if exists:
                this_metadata['_id'] = exists['_id']
                this_metadata['folio'] = exists['folio']
                product_stock = self.get_product_stock(
                    moves['product_code'], 
                    sku=moves['sku'], 
                    lot_number=moves['product_lot'], 
                    warehouse=warehouse_to, 
                    location=location_to)
                this_metadata['answers'].update(self.direct_move_math(exists['answers'], move_in=move_qty, product_stock=product_stock))
                update_stock_records[idx] = this_metadata
            else:
                ##### try to create the inventory
                new_stock_records2[idx] = this_metadata
        return new_stock_records2, update_stock_records

    def get_direct_stock(self, product_code, product_sku, warehouse, location, product_lot):
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
        print('query=', query)
        print('self.cr', self.cr)
        stock =  self.format_cr(self.cr.aggregate(query), get_one=True)
        return stock.get('actuals',0)

    def get_group_skus(self, products):
        search_codes = {}
        for product in products:
            product_code =  product.get('product_code')
            if not product_code:
                product_code = product[self.Product.SKU_OBJ_ID][self.f['product_code']]
            sku =  self.unlist(product.get('sku'))
            if not sku:
                sku = self.unlist(product[self.Product.SKU_OBJ_ID][self.f['sku']])
            search_codes[product_code] = search_codes.get(product_code, [])
            search_codes[product_code].append(sku)
            if sku not in search_codes[product_code]:
                search_codes[product_code].append(sku)
        skus = self.get_product_sku(search_codes)
        return skus

    def get_product_sku(self, all_codes):
        #borrar solo para debug
        all_sku = []
        for sku, product_code in all_codes.items():
            if sku not in all_sku:
                all_sku.append(sku.upper())
        skus = {}
        mango_query = self.product_sku_query(all_sku)
        sku_finds = self.lkf_api.search_catalog(self.Product.SKU_ID, mango_query)
        for this_sku in sku_finds:
                product_code = this_sku.get(self.f['product_code'])
                skus[product_code] = skus.get(product_code, {})
                skus[product_code].update({
                    'sku':this_sku.get(self.f['sku']),
                    'product_name':this_sku.get(self.f['product_name']),
                    'product_category':this_sku.get(self.f['product_category']),
                    'product_type':this_sku.get(self.f['product_type']),
                    'product_department':this_sku.get(self.f['product_department']),
                    'sku_color':this_sku.get(self.f['sku_color']),
                    'sku_image':this_sku.get(self.f['sku_image'],),
                    'sku_note':this_sku.get(self.f['sku_note'],),
                    'sku_package':this_sku.get(self.f['sku_package'],),
                    'sku_per_package':this_sku.get(self.f['reicpe_per_container'],),
                    'sku_size' : this_sku.get(self.f['sku_size']),
                    'sku_source' : this_sku.get(self.f['sku_source']),
                    })
        return skus

    def carga_onts(self, header, records):
        header_dict = self.make_header_dict(header)
        
        """
        # Se revisa que el excel tenga todas las columnas que se requieren para el proceso
        """
        cols_required = ['serie_ont']
        cols_not_found = self.check_keys_and_missing(cols_required, header_dict)
        if cols_not_found:
            cols_not_found = [ c.replace('_', ' ').title() for c in cols_not_found ]
            self.LKFException( f'Se requieren las columnas: {self.list_to_str(cols_not_found)}' )

        #for testin only 20 records 
        # records = records[0:20]
        return header_dict, records

    def fill_with_zeros(self, answers):
        answers[self.f['product_lot_produced']] = 0
        answers[self.f['move_in']] = 0
        answers[self.f['move_out']] = 0
        answers[self.f['product_lot_sales']] = 0
        answers[self.f['product_lot_cuarentin']] = 0
        answers[self.f['product_lot_scrapped']] = 0
        answers[self.f['product_lot_adjustments']] = 0
        answers[self.f['actuals']] = 0
        answers[self.f['actual_eaches_on_hand']] = 0
        answers[self.f['status']] = 'active'
        return answers

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

    # def get_product_sku(self, all_codes):
    #     #migrara a branch de magnolia
    #     all_sku = []
    #     for sku, product_code in all_codes.items():
    #         if sku not in all_sku:
    #             all_sku.append(sku.upper())
    #     skus = {}
    #     mango_query = self.product_sku_query(all_sku)
    #     print('SKU_ID =',self.Product.SKU_ID)
    #     print('mango_query =',mango_query)
    #     sku_finds = self.lkf_api.search_catalog(self.Product.SKU_ID, mango_query)
    #     for this_sku in sku_finds:
    #             product_code = this_sku.get(self.f['product_code'])
    #             skus[product_code] = skus.get(product_code, {})
    #             skus[product_code].update({
    #                 'sku':this_sku.get(self.f['sku']),
    #                 'product_name':this_sku.get(self.f['product_name']),
    #                 'product_category':this_sku.get(self.f['product_category']),
    #                 'product_type':this_sku.get(self.f['product_type']),
    #                 'product_department':this_sku.get(self.f['product_department']),
    #                 'sku_color':this_sku.get(self.f['sku_color']),
    #                 'sku_image':this_sku.get(self.f['sku_image'],),
    #                 'sku_note':this_sku.get(self.f['sku_note'],),
    #                 'sku_package':this_sku.get(self.f['reicpe_container'],),
    #                 'sku_per_package':this_sku.get(self.f['reicpe_per_container'],),
    #                 'sku_size' : this_sku.get(self.f['sku_size']),
    #                 'sku_source' : this_sku.get(self.f['sku_source']),
    #                 })
 
    def explote_kit(self, bom_lines, warehouse=None, location=None):
        bom_res = self.JIT.explote_kit(bom_lines, warehouse, location)
        add_row = 0
        rows_added = 0
        for idx, row in enumerate(bom_res):
            if row.get('bom_type') or row.get('bom_name'):
                row['move_group_qty'] = row['qty']
                if add_row > 0:
                    self.answers[self.f['move_group']].insert(idx+rows_added, self.answers[self.f['move_group']][idx+rows_added - 1])
                add_row += 1
            
        return bom_res

    def format_stock_cache(self, move, move_type, warehouse_to, location_to):
        print('move', move)
        cache_data = {
            '_id': f"{move['product_code']}_{move['sku']}_{move['product_lot']}_{warehouse_to}_{location_to}",
            'product_lot': move['product_lot'],
            'product_code':move['product_code'],
            'sku':move['sku'],
            'warehouse': warehouse_to,
            'warehouse_location': location_to,
            'record_id':self.record_id,
            'function':'direct_move_in_137'
            }
        if move_type == 'in':
            cache_data.update({'move_in': move['move_group_qty']})
        elif move_type == 'out':
            cache_data.update({'move_out': move['move_group_qty']})
        return cache_data

    def get_complete_metadata(self, fields={}):
        now = datetime.now()
        format_date = int(now.timestamp())  # Converting to timestamp
        fields['user_id'] = self.current_record['user_id']
        fields['user_name'] = self.current_record['user_name']
        fields['assets'] = {
            "template_conf" : None,
            "followers" : [ 
                {
                    "asset_id" : self.current_record['user_id'],
                    "email" :  self.current_record['created_by_email'],
                    "name" : self.current_record['user_name'],
                    "username" : self.user['username'],
                    "rtype" : "user",
                    "rules" : None
                }
            ],
            "supervisors" : [],
            "performers" : [],
            "groups" : []
        }
        fields['created_at'] = now
        fields['updated_at'] = now
        fields['editable'] = True
        fields['start_timestamp'] = time.time()
        fields['end_timestamp'] = time.time()
        fields['start_date'] = now
        fields['end_date'] = now
        fields['duration'] = 0
        fields['created_by_id'] = self.current_record['user_id']
        fields['created_by_name'] = self.current_record['user_name']
        fields['created_by_email'] = self.current_record['created_by_email']
        fields['timezone'] = "America/Monterrey"
        fields['tz_offset'] = -360
        fields['other_versions'] = []  
        return fields

    def get_enviroment(self):
        if self.settings.config.get('MONGODB_HOST'):
            if 'db4' in self.settings.config['MONGODB_HOST']:
                return 'prod'
            if 'dbs2' in self.settings.config['MONGODB_HOST']:
                return 'preprod'
        return 'local'

    def get_skus_records(self):
        """
        Se revisa en el catalogo de productos que exista el codigo y sku para obtener la informacion en los readonly
        """
        records_catalog = self.lkf_api.search_catalog( self.Product.SKU_ID )
        dict_skus = {}
        for r in records_catalog:
            productCode = r.get( self.f['product_code'] )
            productSku = r.get( self.f['sku'] )
            productName = r.get( self.f['product_name'] )
            productTipoMaterial = r.get( self.mf['product_material'] )
            if not productCode or not productSku:
                continue
            TipoMaterial = [self.unlist(productTipoMaterial)] if type(productTipoMaterial) == list else [productTipoMaterial]
            # if TipoMaterial and type(TipoMaterial[0]) == list:
            #     TipoMaterial = TipoMaterial[0]
            dict_skus[ f'{productCode}_{productSku}' ] = {
                self.f['product_name']: [self.unlist(productName)],
                self.mf['product_material']: TipoMaterial,
            }
        return dict_skus

    def get_record_folio(self, form_id, folio):
        records = self.cr.find({'form_id':form_id, 'folio':{'$regex': f"^{folio}"} },{'folio':1})
        max_folio = 0
        for r in records:
            tfolio = r.get('folio','')
            folio_list = tfolio.split('-')
            #try:
            if True:
                idx_folio = int(folio_list[1])
                if idx_folio > max_folio:
                    max_folio = idx_folio
            # except:
            #     print('folio not found')
        return max_folio + 1
    
    def get_record_ids_by_folios(self, response):
        query_find = {'form_id':self.FORM_INVENTORY_ID}
        folios = []
        folio_ids = {}
        folios = [r['folio'] for idx, r in response.items() if r.get('folio')]
        if isinstance(folios, list):
            query_find.update({'folio':{'$in':folios}})
        elif isinstance(folios, str):
            query_find.update({'folio':folios})

        res = self.cr.find(query_find,{'folio':1})
        for r in res:
            folio_ids[r['folio']] = str(r['_id'])
        # db_ids = [r['lkf_id'] for r in self.class_cr.find(query)]

        return folio_ids

    def make_direct_stock_move(self, move_type='out'):
        """
        Crea momimientos de almacen haciendo registros directo a la base de datos
        1. Crea registro de stock inventary (entrada al amacen)
        args: move_type

        """
        self.answer_label = self._labels()
        #se crea entrada al amcen
        response = self.direct_move_in(self.current_record)
        # folio_ids = self.get_record_ids_by_folios(response)
        for idx, res in response.items():
            if res.get('new_id'):
                self.answers[self.f['move_group']][idx][self.f['inv_adjust_grp_status']] = 'done'
            elif res.get('acknowledged') and res['acknowledged']:
                self.answers[self.f['move_group']][idx][self.f['inv_adjust_grp_status']] = 'done'
            else:
                self.answers[self.f['move_group']][idx][self.f['inv_adjust_grp_status']] = 'error'
            if res.get('folio'):
                self.answers[self.f['move_group']][idx][self.f['move_dest_folio']] = res['folio']

        if move_type == 'out':
            if self.proceso_onts:
                self.move_out_stock_ont(self.current_record)
            else:
                self.move_out_stock(self.current_record)
        self.answers[self.f['inv_adjust_status']] =  'done'
        return response  

    def move_out_stock(self, new_record):
        """
        Movimiento de salida de stock

        args:
            new_record: registro con los datos del movimiento
        return:
            updated_ids: lista de ids de los registros actualizados
        
        """
        record_data = self.get_record_move_data(new_record)
        updated_ids = {}
        deleted_sync_ids = []
        edited_sync_ids = []
        print('va a mover uno por uno en move_out_stock')

        for idx, product_data in record_data['product'].items():
            product_code = product_data['product_code']
            sku = product_data['product_sku']
            product_lot = product_data['product_lot']
            move_qty = product_data.get('move_qty',0)
            print('moving 1')
            match_query = self.lot_match(product_code, sku, record_data['warehouse'], record_data['location'], product_lot)
            cr_data = self.cr.find(match_query)
            product_db_info = cr_data.next()
            print('moving 2')
            product_db_info.update(self.get_record_metadata(metadata=product_db_info, pop_common=False))
            product_stock = self.get_product_stock(product_code, sku=sku, lot_number=product_lot, warehouse=record_data['warehouse'], location=record_data['location'])
            # acutals = product_stock.get('actuals',0)
            product_db_info['answers'].update(self.direct_move_math(product_db_info['answers'],  move_in=0, move_out=move_qty, product_stock=product_stock))
            rec_id = product_db_info.pop('_id')
            this_res = self.cr.update_one({'_id':ObjectId(rec_id)}, {"$set":product_db_info })
            print('moving 3')
            updated_ids[idx] = {
                    'acknowledged': this_res.acknowledged, 
                    'modified_count': this_res.modified_count, 
                    'matched_count': this_res.matched_count, 
                    '_id':rec_id
                    }
            # update_res = self.cr.update_many({'_id':product_db_info['_id']}, {"$set":{
            #     f"answers.{self.f['actual_eaches_on_hand']}":new_acutals,
            #     f"answers.{self.f['product_lot_actuals']}":new_acutals,
            #     f"answers.{self.f['move_out']}":move_qty,
            #     f"answers.{self.f['status']}":status,
            #     }})
            sync_action = 'edited'
            print('moving 4')
            if product_db_info['answers'][self.f['status']] == 'done':
                sync_action = 'deleted'
            if sync_action == 'deleted':
                deleted_sync_ids.append(str(rec_id))
            else:
                edited_sync_ids.append(str(rec_id))
        print('deleted catalogs.... =', len(deleted_sync_ids))
        print('edited catalogs.... =', len(edited_sync_ids))
        if deleted_sync_ids:
            sync_res = self.lkf_api.sync_catalogs_records({
                    'catalogs_ids':[self.CATALOG_INVENTORY_ID],
                    'form_answers_ids':deleted_sync_ids,
                    'status':'deleted',
                    })
        if edited_sync_ids:
            sync_res = self.lkf_api.sync_catalogs_records({
                    'catalogs_ids':[self.CATALOG_INVENTORY_ID],
                    'form_answers_ids':edited_sync_ids,
                    'status':'edited',
                    })
        print('moving 5')
        return updated_ids    

    def move_in(self):
        #solo se usa en pci y en pruebas de exposion de materiales
        # print('-- -- -- -- -- -- answers=',self.answers)
        answers = self._labels()

        self.answer_label = self._labels()

        warehouse = self.answer_label['warehouse']
        location = self.answer_label['warehouse_location']
        warehouse_to = self.answer_label['warehouse_dest']
        location_to = self.answer_label['warehouse_location_dest']
        move_lines = self.JIT.explote_kit(self.answer_label['move_group'])
        # Información original del Inventory Flow
        status_code = 0
        move_locations = []
        folios = []
        # lots_in = {}
        data_from = {'warehouse':warehouse, 'warehouse_location':location}
        new_records_data = []
        skus = self.get_group_skus(move_lines)
        metadata = self.lkf_api.get_metadata(self.FORM_INVENTORY_ID)
        for idx, moves in enumerate(move_lines):
            move_line = self.answers[self.f['move_group']][idx]
            if not moves.get('product_lot'):
                moves['product_lot'] = 'LotePCI001'
                move_line[self.f['product_lot']] = 'LotePCI001'
            if moves.get('bom_name'):
               move_line[self.f['bom_name']] = moves['bom_name']

            # product_code = info_product.get(self.f['product_code'])
            # sku = info_product.get(self.f['sku'])
            package = move_line.get(self.f['sku_package'])
            exists = self.product_stock_exists(
                product_code=moves['product_code'], 
                sku=moves['sku'], 
                lot_number=moves['product_lot'], 
                warehouse=warehouse_to, 
                location=location_to)
            cache_data = {
                        '_id': f"{moves['product_code']}_{moves['sku']}_{moves['product_lot']}_{warehouse_to}_{location_to}",
                        'move_in': moves['move_group_qty'],
                        'product_lot': moves['product_lot'],
                        'product_code':moves['product_code'],
                        'sku':moves['sku'],
                        'warehouse': warehouse_to,
                        'warehouse_location': location_to,
                        'record_id': self.record_id
                        }
            if exists:
                if self.folio:
                    cache_data.update({'kwargs': {'nin_folio':self.folio }})
                self.cache_set(cache_data)
                response = self.update_stock(answers=exists.get('answers',{}), form_id=self.FORM_INVENTORY_ID, folios=exists['folio'])
                if not response:
                    comments += f"Error updating product {moves['product_lot']} lot {moves['product_lot']}. "
                    move_line[self.f['inv_adjust_grp_status']] = 'error'
                else:
                    move_line[self.f['move_dest_folio']] = exists['folio']
                    move_line[self.f['inv_adjust_grp_status']] = 'done'
                    move_line[self.f['inv_adjust_grp_comments']] = ""

            else:
                answers = self.stock_inventory_model(moves, skus[moves['product_code']], labels=True)
                answers.update({
                    self.WH.WAREHOUSE_LOCATION_OBJ_ID:{
                    self.f['warehouse']:warehouse_to,
                    self.f['warehouse_location']:location_to},
                    self.f['product_lot']:moves['product_lot']
                        },
                    )
                metadata['answers'] = answers
               
                self.cache_set(cache_data)
                create_resp = self.lkf_api.post_forms_answers(metadata)
                try:
                    new_inv = self.get_record_by_id(create_resp.get('id'))
                except:
                    print('no encontro...')
                status_code = create_resp.get('status_code',404)
                if status_code == 201:
                    folio = create_resp.get('json',{}).get('folio','')
                    move_line[self.f['inv_adjust_grp_status']] = 'done'
                    move_line[self.f['move_dest_folio']] = folio
                else:
                    error = create_resp.get('json',{}).get('error', 'Unkown error')
                    move_line[self.f['inv_adjust_grp_status']] = 'error'
                    move_line[self.f['inv_adjust_grp_comments']] = f'Status Code: {status_code}, Error: {error}'
        return True

    def move_one_many_one(self, records=[]):
        #pci
        move_lines = self.answers[self.f['move_group']]
        if records and self.proceso_onts:
            move_lines = self.append_onts(records)

        warehouse = self.answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse']]
        location = self.answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse_location']]
        warehouse_to = self.answers[self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID][self.f['warehouse_dest']]
        location_to = self.answers[self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID][self.f['warehouse_location_dest']]
        # Información original del Inventory Flow
        status_code = 0
        move_locations = []
        folios = []
        # lots_in = {}
        data_from = {'warehouse':warehouse, 'warehouse_location':location}
        new_records_data = []
        connection_to_assign = self.answers.pop('id_user_to_assign', None)
        for moves in move_lines:
            info_product = moves.get(self.STOCK_INVENTORY_OBJ_ID, {})
            # product_code = info_product.get(self.f['product_code'])
            # sku = info_product.get(self.f['sku'])
            # lot_number = info_product.get(self.f['lot_number'])
            # stock = self.get_stock_info_from_catalog_inventory(answers=moves, data=data_from , **{'get_record':True})
            stock = self.get_stock_info_from_catalog_inventory(answers=moves, data=data_from )
            ###################################
            product_code = stock.get('product_code')
            sku = stock.get('sku')
            lot_number = stock.get('lot_number')
            warehouse = stock.get('warehouse')
            location = stock.get('warehouse_location')
            folios.append(stock['folio'])
            ###################################

            moves[self.f['move_dest_folio']] = stock['folio']
            moves[self.f['inv_adjust_grp_status']] = 'done'
            
            set_location = f"{product_code}_{sku}_{lot_number}_{warehouse_to}_{location_to}"
            msg = None 
            if set_location in move_locations:
                msg = "You are trying to move the same lot_number: {lot_number} twice from the same location. Please check"
                msg += f"warehouse: {stock['warehouse']} / Location: {stock['warehouse_location']}"
            move_locations.append(set_location)
            inv_status = stock.get('record',{}).get('answers',{}).get(self.f['inventory_status'])

            if msg:
                msg_error_app = {
                    f"{self.f['warehouse_location']}": {
                        "msg": [msg],
                        "label": "Please check your availability to this location",
                        "error":[]
                    }
                }
                self.LKFException('', dict_error=msg_error_app)
            # Información que modifica el usuario
            move_qty = moves.get(self.f['move_group_qty'],0)
            moves[self.f['inv_move_qty']] = move_qty
            # se debe de user el folio del moviniemto para que no tome en cuenta en el cache el moviemote
            # self.folio = stock['folio']
            self.validate_move_qty(product_code, sku, lot_number,  warehouse, location, move_qty, **{'cache.folio_from':{'$ne':self.folio}})

            move_vals_from = {'_id': f"{product_code}_{sku}_{lot_number}_{warehouse}_{location}",
                        'move_out':move_qty,
                        'product_code':product_code,
                        'product_lot':lot_number,
                        'warehouse': warehouse,
                        'warehouse_location': location,
                        'record_id': self.record_id
                        }
            if self.folio:
                move_vals_from.update({'kwargs': {'nin_folio':self.folio }})
            move_vals_to = deepcopy(move_vals_from)
            move_vals_to.pop('move_out')
            move_vals_to.update(
                {
                '_id': set_location,
                'warehouse': warehouse_to,
                'warehouse_location': location_to,
                'from_folio': self.folio, 
                'move_in':move_qty,
                'move_qty':move_qty
            })
            # lots_in[set_location] = lots_in.get(set_location, move_vals_to) 
            self.cache_set(move_vals_to)
            self.cache_set(move_vals_from)
            new_lot = stock.get('record',{}).get('answers',{})
            warehouse_ans = self.swap_location_dest(self.answers[self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID])
            new_lot.update(warehouse_ans)
            new_records_data.append(self.create_inventory_flow(answers=new_lot))

        

        res ={}

        create_new_records = []
        for record in new_records_data:
            print('record=', record)
            if self.unlist(record).get('new_record'):
                create_new_records.append(record['new_record'])
            else:
                print('1YA EXISTE... record ya se actualizco usando cache', record)
        print('TODO mover status a lineas de registro')
        res_create = self.lkf_api.post_forms_answers_list(create_new_records)
        #updates records from where stock was moved
        res = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=folios)
        res ={}
        if connection_to_assign:
            for stock_inv_created in res_create:
                new_record = '/api/infosync/form_answer/' + stock_inv_created['json']['id'] +'/'
                response_assign = self.lkf_api.assigne_connection_records( connection_to_assign, [new_record,])
        """
        Despues de crear el registro de Stock Inventory hay que asignarlo al contratista
        """
        #res = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=folios)
        return True

    def xls_header_record(self, xls_file):
        data_xls = self.read_xls(xls_file)
        if data_xls:
            header = data_xls.get('header')
            records = data_xls.get('records')
            records =  [item for item in records if not (isinstance(item, list) and all(elem == '' for elem in item))]
            return header, records 
        else:
            return None, None

    def read_xls_file(self):
        header, records = self.xls_header_record( self.mf['xls_file'])
        
        if not header:
            return None, None
        if 'serie ont' in [x.lower() for x in header]:
            self.proceso_onts = True
            return self.carga_onts(header, records)
        else:
            return self.carga_materiales(header, records)

    def read_xls(self, id_field_xls):
        file_url_xls = self.answers.get( id_field_xls )
        if not file_url_xls: 
            print(f'no hay excel de carga {id_field_xls}')
            return False
        file_url_xls = file_url_xls[0].get('file_url')
        if not hasattr(self, 'prev_version'):
            if self.folio: 
                if self.current_record.get('other_versions'):
                    # print('entra al other_versions')
                    self.prev_version = self.get_prev_version(self.current_record['other_versions'], select_columns=[ 'answers.{}'.format(self.mf['xls_file']), 'answers.{}'.format(self.mf['xls_onts']) ])
                else:
                    print('1Ya tiene folio pero aun no hay mas versiones... revisando el current_record en la BD')
                    self.prev_version = self.get_record_from_db(self.form_id, self.folio, select_columns=[ 'answers.{}'.format(self.mf['xls_file']), 'answers.{}'.format(self.mf['xls_onts']) ])
                print('prev_version=',self.prev_version)
        elif self.prev_version.get('answers', {}).get( id_field_xls ):
            print( 'ya hay un excel previamente cargado... se ignora en esta ejecucion =',self.prev_version.get('answers', {}).get( id_field_xls ) )
            return False
        header, records = self.read_file( file_url_xls )
        return {'header': header, 'records': records}

    def read_xls__temp(self, id_field_xls):
        file_url_xls = self.answers.get( id_field_xls )
        if not file_url_xls:
            print(f'no hay excel de carga {id_field_xls}')
            return False
        file_url_xls = file_url_xls[0].get('file_url')
        prev_version = {}
        if self.folio: 
            if self.current_record.get('other_versions'):
                # print('entra al other_versions')
                prev_version = self.get_prev_version(self.current_record['other_versions'], select_columns=[ 'answers.{}'.format(self.mf['xls_file']), 'answers.{}'.format(self.mf['xls_onts']) ])
            else:
                print('Ya tiene folio pero aun no hay mas versiones... revisando el current_record en la BD')
                prev_version = self.get_record_from_db(self.form_id, self.folio, select_columns=[ 'answers.{}'.format(self.mf['xls_file']), 'answers.{}'.format(self.mf['xls_onts']) ])
        if prev_version.get('answers', {}).get( id_field_xls ):
            print( 'ya hay un excel previamente cargado... se ignora en esta ejecucion =',prev_version.get('answers', {}).get( id_field_xls ) )
            return False
        header, records = self.read_file( file_url_xls )
        return {'header': header, 'records': records}

    def set_mongo_connections(self):
        self.client = self.get_mongo_client()
        dbname = 'infosync_answers_client_{}'.format(self.account_id)
        db = self.client[dbname]
        self.records_cr = db["form_answer"]
        self.ont_cr = db["serie_onts"]
        return True
