# -*- coding: utf-8 -*-

import sys, simplejson, time
from bson import ObjectId
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.stock.app import Stock

today = date.today()
year_week = int(today.strftime('%Y%W'))

class Stock(Stock):

    # _inherit = 'employee'

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, folio_solicitud=folio_solicitud, sys_argv=sys_argv, use_api=use_api)
        self.load('JIT', **self.kwargs)


        # La relacion entre la forma de inventario y el catalogo utilizado para el inventario
        # por default simpre dejar los mismos nombres
        self.FORM_CATALOG_DIR = {
            self.FORM_INVENTORY_ID:self.CATALOG_INVENTORY_ID,
            }

        self.f.update({
            'parts_group':'62c5da67f850f35cc2483346',
            })
        self.answer_label = self._labels()
        self.FOLDER_FORMS_ID = self.lkm.item_id('Stock', 'form_folder').get('id')
        self.mf = {
            'xls_file': '66c797955cfca4851db2c3b8',
            'xls_onts': '66e0cd760cc8e3fb75f23803',
            'capture_num_serie': '66c75e0c0810217b0b5593ca'
        }
        self.max_sets = 2500

    def do_groups(self, header, records):
        pos_serie = header.get('serie_ont')
        error_rows = []
        move_group = self.answers.get( self.f['move_group'], [] )
        if move_group:
            # capture_num_serie = self.unlist( move_group[0].get( self.Product.SKU_OBJ_ID, {} ).get( self.mf['capture_num_serie'] ) ) == 'Si'
            # print('capture_num_serie',capture_num_serie)
            # cantidad_solicitada = move_group[0].get( self.f['move_group_qty'], 0 )
            # print('cantidad_solicitada',cantidad_solicitada)
            # #if capture_num_serie and cantidad_solicitada:
            onts = [x[pos_serie] for x in records]
            groups = [onts[i:i + self.max_sets] for i in range(0, len(onts), self.max_sets) ]
            print('records=', len(groups))
        else:

            self.LKFException( '', dict_error= {
                        "msg": 'Favor de indicar el numero de producto y sku a procesar en la carga de ONTS'} 
                )
        return groups

    def direct_move_in(self, new_record):
        #solo se usa en pci y en pruebas de exposion de materiales
        answers = self._labels(data=new_record)
        self.answer_label = self._labels()
        warehouse = self.answer_label['warehouse']
        location = self.answer_label['warehouse_location']
        warehouse_to = self.answer_label['warehouse_dest']
        location_to = self.answer_label['warehouse_location_dest']
        move_lines = self.answer_label['move_group'] 
        move_lines = answers['move_group'] 
        # Información original del Inventory Flow
        status_code = 0
        move_locations = []
        folios = []
        # lots_in = {}
        data_from = {'warehouse':warehouse, 'warehouse_location':location}
        new_records_data = []
        skus = self.get_group_skus(move_lines)
        metadata = self.lkf_api.get_metadata(self.FORM_INVENTORY_ID)
        if self.proceso_onts:
            metadata.update(self.get_complete_metadata(fields = {'voucher_id':ObjectId('6743d90d5f1c35d02395a7cf')}))
        new_stock_records = []
        folio = new_record['folio']
        for idx, moves in enumerate(move_lines):
            status = moves.get('inv_adjust_grp_status')
            if status == 'done' and not self.proceso_onts:
                continue
            this_metadata = deepcopy(metadata)
            this_metadata['folio'] = f'{folio}-{idx}'
            move_line = self.answers[self.f['move_group']][idx]

            answers = self.stock_inventory_model(moves, skus[moves['product_code']], labels=True)
            answers.update({
                self.WH.WAREHOUSE_LOCATION_OBJ_ID:{
                self.f['warehouse']:warehouse_to,
                self.f['warehouse_location']:location_to},
                self.f['product_lot']:moves['product_lot'],
                    }
                )
            this_metadata['answers'] = answers
            new_stock_records.append(this_metadata)
            if not self.proceso_onts:
                create_resp = self.lkf_api.post_forms_answers(this_metadata)
                try:
                    new_inv = self.get_record_by_id(create_resp.get('id'))
                except:
                    print('no encontro...')
                status_code = create_resp.get('status_code',404)
                if status_code == 201:
                    new_folio = create_resp.get('json',{}).get('folio','')
                    move_line[self.f['inv_adjust_grp_status']] = 'done'
                    move_line[self.f['move_dest_folio']] = new_folio
                else:
                    error = create_resp.get('json',{}).get('error', 'Unkown error')
                    move_line[self.f['inv_adjust_grp_status']] = 'error'
                    move_line[self.f['inv_adjust_grp_comments']] = f'Status Code: {status_code}, Error: {error}'
        if self.proceso_onts:
            if new_stock_records:
                res = self.cr.insert_many(new_stock_records)
                print('INSERTRED IDS....', res.inserted_ids)
                self.cache_set({
                                'cache_type': 'direct_move_in',
                                'inserted_ids':res.inserted_ids,
                                'folio':res.inserted_ids,
                                })
                return res.inserted_ids
        return True

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

    def get_complete_metadata(self, fields={}):
        now = datetime.now()
        format_date = int(now.timestamp())  # Converting to timestamp
        fields['user_id'] = self.user['user_id']
        fields['user_name'] = self.user['username']
        fields['assets'] = {
            "template_conf" : None,
            "followers" : [ 
                {
                    "asset_id" : self.user['user_id'],
                    "email" : self.user['email'],
                    "name" : "PCI Automatico",
                    "username" : None,
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
        fields['created_by_id'] = self.user['user_id']
        fields['created_by_name'] = self.user['username']
        fields['created_by_email'] = "linkaform@operacionpci.com.mx"
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
            if record.get('new_record'):
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
        self.proceso_onts = False
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
            print('prev_version=',prev_version)
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
        
    # def stock_one_many_one(self, move_type, product_code=None, sku=None, lot_number=None, warehouse=None, location=None, date_from=None, date_to=None, status='done', **kwargs):
    #     unwind =None
    #     if move_type not in ('in','out'):
    #         raise('Move type only accepts values "in" or "out" ')
    #     match_query = {
    #         "deleted_at":{"$exists":False},
    #         }
    #     match_query.update(self.stock_kwargs_query(**kwargs))
    #     query_forms = self.STOCK_ONE_MANY_ONE_FORMS
    #     if len(query_forms) > 1:
    #         form_query = {"form_id":{"$in":query_forms}}
    #     else:
    #         form_query = {"form_id":self.STOCK_ONE_MANY_ONE_FORMS[0]}
    #     match_query.update(form_query)
    #     if date_from or date_to:
    #         match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))

    #     unwind = {'$unwind': '$answers.{}'.format(self.f['move_group'])}
    #     unwind_query = {}
    #     unwind_stage = []
    #     # print('move type.............', move_type)
    #     # print('warehouse', warehouse)
    #     # print('location', location)
    #     # print('product_code', product_code)
    #     # print('sku', sku)
    #     # print('lot_number', lot_number)
    #     if move_type =='in':
    #         if warehouse:
    #             match_query.update({f"answers.{self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f['warehouse_dest']}":warehouse})
    #         if location:
    #             match_query.update({f"answers.{self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f['warehouse_location_dest']}":location})        
    #     if move_type =='out':
    #         if warehouse:
    #             match_query.update({f"answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}":warehouse})
    #         if location:
    #             match_query.update({f"answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse_location']}":location})
       
    #     if product_code:
    #         p_code_query = {"$or":[
    #             {f"answers.{self.f['move_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code},
    #             {f"answers.{self.f['move_group']}.{self.Product.SKU_OBJ_ID}.{self.f['product_code']}":product_code}
    #         ]
    #         }
    #         unwind_stage.append({'$match': p_code_query })
    #     if sku:
    #         sku_query = {"$or":[
    #             {f"answers.{self.f['move_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['sku']}":sku},
    #             {f"answers.{self.f['move_group']}.{self.Product.SKU_OBJ_ID}.{self.f['sku']}":sku}
    #         ]
    #         }
    #         unwind_stage.append({'$match': sku_query })
    #     if lot_number:
    #         lot_number_query = {"$or":[
    #             {f"answers.{self.f['move_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['lot_number']}":lot_number},
    #             {f"answers.{self.f['move_group']}.{self.Product.SKU_OBJ_ID}.{self.f['lot_number']}":lot_number},
    #             {f"answers.{self.f['move_group']}.{self.f['lot_number']}":lot_number}
    #         ]
    #         }
    #         unwind_stage.append({'$match': lot_number_query })
    #     if status:
    #         match_query.update({f"answers.{self.f['stock_status']}":status})
    #     if date_from or date_to:
    #         match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
    #     project = {'$project':
    #             {'_id': 1,
    #                 'product_code':{"$ifNull":[
    #                     f"$answers.{self.f['move_group']}.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}",
    #                     f"$answers.{self.f['move_group']}.{self.Product.SKU_OBJ_ID}.{self.f['product_code']}",
    #                 ] } ,
    #                 'total': f"$answers.{self.f['move_group']}.{self.f['move_group_qty']}",
    #                 }
    #         }
    #     query= [{'$match': match_query }]
    #     query.append(unwind)
    #     if unwind_query:
    #         query.append({'$match': unwind_query })
    #     if unwind_stage:
    #         query += unwind_stage
    #     query.append(project)
    #     query += [
    #         {'$group':
    #             {'_id':
    #                 { 'product_code': '$product_code',
    #                   },
    #               'total': {'$sum': '$total'},
    #               }
    #         },
    #         {'$project':
    #             {'_id': 0,
    #             'product_code': '$_id.product_code',
    #             'total': '$total',
    #             }
    #         },
    #         {'$sort': {'product_code': 1}}
    #         ]
    #     print('2query=', simplejson.dumps(query, indent=3))
    #     res = self.cr.aggregate(query)
    #     result = {}
    #     for r in res:
    #         pcode = r.get('product_code')
    #         result[pcode] = result.get(pcode, 0)        
    #         result[pcode] += r.get('total',0)
    #     if product_code:
    #         result = result.get(product_code,0)
    #     return result 

