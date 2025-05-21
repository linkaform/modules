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
            'product_material': '66b10b87a1d4483b5369f409',
            'series_group':'66c75ca499596663582eed59',
            'num_serie': '66c75d1e601ad1dd405593fe',
        })

        self.prev_version = {}
        self.max_sets = 2
        self.sku_finds = []

    def carga_materiales(self, header, records):
        #ESTA FUNCION ES MUY PARECECIDA A STOCK_MOVE_MAY_2_ONE  SOLO QUE 
        #CAMBIA EL TIPO DEL DEL CATALGOO DENTRO GRUPO REPETITIVO
        #En este movimiento se requiere que venga de SKU
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
        existing_rows = self.answers[ self.f['move_group'] ]
        existing_sku = [x[self.Product.SKU_OBJ_ID][self.f['sku']] for x in existing_rows if x.get(self.Product.SKU_OBJ_ID,{}).get(self.f['sku'])]
        for pos_row, rec in enumerate(records):
            num_row = pos_row + 2
            product_code = rec[pos_codigo]
            sku = rec[pos_sku]
            cantidad = rec[pos_cantidad]
            if sku in existing_sku:
                msg = f'SKU {sku}: Repetido o previamente cargado, omitiendo'
                if self.answers.get(self.f['stock_move_comments']):
                    self.answers[self.f['stock_move_comments']]  += f' | {msg}'
                else:
                    self.answers[self.f['stock_move_comments']] = msg
                continue
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
            })
            
            sets_to_products.append({
                self.Product.SKU_OBJ_ID: info_product,
                self.f['lot_number']: 'LotePCI001',
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

    def create_records(self, groups):
        """
            Se acomoda el grupo repetitivo de la recpcion de ONTs
            Pone folio segun tamaño de grupo
            arma sets para continuar con proceso
        args: 
            grupos: lista registros de excel
        """
        move_group = self.answers.get( self.f['move_group'], [] )
        base_row_set = deepcopy(move_group[0])
        self.answers[self.f['move_group']] = []
        base_row_set[self.f['move_group_qty']] = 1
        series_unique = []
        total_groups = len(groups)
        base_record = deepcopy(self.current_record)
        base_record.update(self.get_complete_metadata(fields = {'voucher_id':ObjectId('6743d90d5f1c35d02395a7cf')}))
        base_record['answers'][self.f['move_group']] = []
        base_record["editable"] = False 
        series_repetidas = []
        get_folio = True
        ########
        create_new_rec = True
        for idx, records in enumerate(groups):
            new_record = deepcopy(base_record)
            new_folio = f"{self.folio}-{idx+1}/{total_groups}"
            new_record['folio'] = new_folio
            print('idx', idx)
            folio_serie_record = []
            # self.answers[self.f['move_group']] = []
            new_record['answers'][self.f['inv_adjust_status']] = 'todo'
            for idy, num_serie in enumerate(records):
                get_folio = True
            #### ONTS
                print('idy', idy)
                if get_folio:
                    folio_ont_inv = f"{self.folio}-{idy+1}/{len(records)}"
                # num_serie = row[ pos_serie ]
                num_serie = self.strip_special_characters(num_serie)
                print('num_serie', num_serie)
                if not num_serie:
                    continue
                if num_serie in series_unique:
                    series_repetidas.append(num_serie)
                    get_folio = False
                    continue
                series_unique.append(num_serie)
                row_set = deepcopy(base_row_set)
                row_set[self.f['lot_number']] = num_serie
                row_set['folio'] = folio_ont_inv
                row_set[self.f['inv_adjust_grp_status']] = 'todo'
                folio_serie_record.append({"folio":new_folio, "ont_serie": num_serie, "folio_recepcion":folio_ont_inv})
                new_record['answers'][self.f['move_group']].append(row_set)
                self.answers[self.f['move_group']].append(row_set)
                if idx+1 == len(groups):
                    create_new_rec = False
                    print('---------------------------')
            print('*************groiu************',create_new_rec)
            if create_new_rec:
                self.ejecutar_transaccion(new_record, folio_serie_record )
            else:
                self.ejecutar_transaccion({}, folio_serie_record )
            if series_repetidas:
                self.LKFException( '', dict_error= {
                    f"{self.f['lot_number']}": {
                    "msg": ['Se encontraron series repetidas en el excel: {}'.format( series_repetidas )], 
                    "label": "Serie Repetida", "error": []}}
                )
        return True

    def ejecutar_transaccion(self, new_record, folio_serie_record):
        # Inicia una sesión
        if self.get_enviroment() == 'prod':
            with self.client.start_session() as session:
                # Define el bloque de transacción
                #HACE TRANSACCIONES ACIDAS
                def write_records(sess):
                    if new_record.get('answers'):
                        # self.direct_move_in(new_record)
                        self.records_cr.insert_one(new_record, session=sess)
                    try:
                        if folio_serie_record:
                            self.ont_cr.insert_many(folio_serie_record, session=sess)
                    except Exception as e:
                        print(f"Error durante la transacción: {e}")
                        self.LKFException( '', dict_error= {
                            f"Error": {
                            "msg": [f'Error en la creacion de las onts. Existen Series previamente Cargadas. {e}'], 
                            "label": "Serie Repetida", "error": []}}
                            )
            
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
            try:
                if folio_serie_record and False:
                    res = self.ont_cr.insert_many(folio_serie_record)
            except Exception as e:
                self.LKFException( '', dict_error= {
                        f"Error": {
                        "msg": [f'Error en la creacion de las onts. Existen Series previamente Cargadas '], 
                        "label": "Serie Repetida", "error": []}}
                        )
            # try:
            if True:
                if new_record.get('answers'):
                    print('rwreew [[[[[[[[[[[[[[[[ new records]]]]]]]]]]]]]]]]', simplejson.dumps(new_record['answers'], indent=3))
                    response = stock_obj.make_direct_stock_move(move_type='in')
                    print('res',response)
                    res = self.records_cr.insert_one(new_record)
                    #self.direct_move_in(new_record)
            # except Exception as e:
            #     print('error: ', e)
            #     series = [s['ont_serie'] for s in folio_serie_record]
            #     res = self.ont_cr.delete_many({'ont_serie':{'$in':series}})
            #     print('delete_many: ',series)
            #     self.LKFException( '', dict_error= {
            #             f"{self.f['lot_number']}": {
            #             "msg": [f'INTENTA NUEVAMENTE:  Se encontraron series repetidas en el excel. '], 
            #             "label": "Serie Repetida", "error": []}}
            #             )

    def read_series_ONTs(self):
        """
        Se revisa que no haya numeros de serie repetidas
        """
        series_unique = []
        series_repeated = []

        """
        Preparo una lista de tuplas con la info del producto MODEM / ONT y la cantidad de series que se requieren para cada producto
        """
        move_group = self.answers.get( self.f['move_group'], [] )[:]
        for idx, set_product in enumerate(move_group):


            productTipoMaterial = set_product.get( self.mf['product_material'] )
            TipoMaterial = [self.unlist(productTipoMaterial)] if type(productTipoMaterial) == list else [productTipoMaterial]

            set_product[self.mf['product_material']] = TipoMaterial

            capture_num_serie = set_product.get( self.Product.SKU_OBJ_ID, {} ).get( self.mf['capture_num_serie'] )
            if capture_num_serie and type(capture_num_serie[0]) == list:
                set_product[ self.Product.SKU_OBJ_ID ][ self.mf['capture_num_serie'] ] = [ self.unlist(capture_num_serie) ]

            capture_num_serie = True #self.unlist( set_product.get( self.Product.SKU_OBJ_ID, {} ).get( self.mf['capture_num_serie'] ) ) == 'Si'
            cantidad_solicitada = set_product.get( self.f['move_group_qty'], 0 )
            if capture_num_serie and cantidad_solicitada:
                if cantidad_solicitada != len( self.answers.get( self.mf['series_group'], [] ) ):
                    self.LKFException( {
                        'msg': f"La cantidad de series requeridas {cantidad_solicitada} no corresponde con las capturadas {len( self.answers.get( self.mf['series_group'], [] ) )}"
                    } )
                new_set = self.answers[ self.f['move_group'] ].pop(idx)
                for idx_serie, serie_set in enumerate(self.answers.get(self.mf['series_group'], [])):
                    num_serie = serie_set.get( self.mf['num_serie'] )
                    if num_serie not in series_unique:
                        series_unique.append(num_serie)
                    else:
                        series_repeated.append( (idx_serie + 1, num_serie) )
                    row_set = deepcopy(new_set)
                    row_set[ self.f['move_group_qty'] ] = 1
                    row_set[ self.f['lot_number'] ] = serie_set.get( self.mf['num_serie'] )
                    self.answers[ self.f['move_group'] ].insert( idx, row_set )
        
        if series_repeated:
            self.LKFException( self.list_to_str( [f'Codigo {i[1]} esta repetido en el Set {i[0]}' for i in series_repeated] ) )
        self.answers[self.mf['series_group']] = []
        return True

    def get_product_sku(self, all_codes):
        search_sku = []
        for sku, product_code in all_codes.items():
            if sku not in self.all_sku:
                self.all_sku.append(sku.upper())
                search_sku.append(sku.upper())
        skus = {}

        if search_sku:
            mango_query = self.product_sku_query(search_sku)
            sku_finds = self.lkf_api.search_catalog(self.Product.SKU_ID, mango_query)
            self.sku_finds += sku_finds
        else:
            sku_finds = self.sku_finds
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

    def set_mongo_connections(self):
        self.client = self.get_mongo_client()
        dbname = 'infosync_answers_client_{}'.format(self.account_id)
        db = self.client[dbname]
        self.records_cr = db["form_answer"]
        self.ont_cr = db["serie_onts"]
        return True


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    folio = None
    if hasattr(stock_obj,'folio'):
        folio = stock_obj.folio
    if not folio:
        today = stock_obj.get_today_format()
        # folio = f"REC{datetime.strftime(today, '%y%m%d')}"
        folio = "REC"
        next_folio = stock_obj.get_record_folio(stock_obj.STOCK_IN_ONE_MANY_ONE, folio)
        folio = f"{folio}-{next_folio}"


    stock_obj.folio = folio
    stock_obj.current_record['folio'] = folio
    stock_obj.answers[stock_obj.f['folio_recepcion']] = folio
    stock_obj.current_record['answers'] = stock_obj.answers
    stock_obj.set_mongo_connections()
    header, records = stock_obj.read_xls_file()
    if stock_obj.proceso_onts:
        groups = stock_obj.do_groups(header, records)
        print('groups', groups)
        stock_obj.create_records(groups)
    else:
        stock_obj.current_record['answers'] = stock_obj.answers
        response = stock_obj.make_direct_stock_move(move_type='in', )
    # stock_obj.read_series_ONTs()
    # se tiene que mover el direct move in despues de la injeccion de datos

    print('TODO: revisar si un create no estuvo bien y ponerlo en error o algo')
    stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        "metadata":{"folio":stock_obj.current_record['folio']}
        }))
