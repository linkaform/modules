# -*- coding: utf-8 -*-
import sys, simplejson, copy

from stock_utils import Stock

from account_settings import *

class Stock(Stock):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.mf.update({
            'xls_file': '66c797955cfca4851db2c3b8',
            'xls_onts': '66e0cd760cc8e3fb75f23803',
            'product_material': '66b10b87a1d4483b5369f409',
            'series_group':'66c75ca499596663582eed59',
            'num_serie': '66c75d1e601ad1dd405593fe',
            'capture_num_serie': '66c75e0c0810217b0b5593ca'
        })
        self.prev_version = {}

    def get_skus_records(self):
        """
        Se revisa en el catalogo de productos que exista el codigo y sku para obtener la informacion en los readonly
        """
        records_catalog = stock_obj.lkf_api.search_catalog( stock_obj.SKU_ID )
        dict_skus = {}
        for r in records_catalog:
            productCode = r.get( self.f['product_code'] )
            productSku = r.get( self.f['sku'] )
            productName = r.get( self.f['product_name'] )
            productTipoMaterial = r.get( self.mf['product_material'] )
            if not productCode or not productSku:
                continue
            dict_skus[ f'{productCode}_{productSku}' ] = {
                self.f['product_name']: [productName,],
                self.mf['product_material']: [productTipoMaterial,],
            }
        return dict_skus

    def read_xls(self, id_field_xls):
        file_url_xls = self.answers.get( id_field_xls )
        if not file_url_xls:
            print(f'no hay excel de carga {id_field_xls}')
            return False
        file_url_xls = file_url_xls[0].get('file_url')
        if not self.prev_version:
            if self.folio: 
                if self.current_record.get('other_versions'):
                    # print('entra al other_versions')
                    self.prev_version = stock_obj.get_prev_version(self.current_record['other_versions'], select_columns=[ 'answers.{}'.format(self.mf['xls_file']), 'answers.{}'.format(self.mf['xls_onts']) ])
                else:
                    print('Ya tiene folio pero aun no hay mas versiones... revisando el current_record en la BD')
                    self.prev_version = stock_obj.get_record_from_db(self.form_id, self.folio, select_columns=[ 'answers.{}'.format(self.mf['xls_file']), 'answers.{}'.format(self.mf['xls_onts']) ])
                print('prev_version=',self.prev_version)
        if self.prev_version.get('answers', {}).get( id_field_xls ):
            print( 'ya hay un excel previamente cargado... se ignora en esta ejecucion =',self.prev_version.get('answers', {}).get( id_field_xls ) )
            return False
        header, records = stock_obj.read_file( file_url_xls )
        return {'header': header, 'records': records}

    def read_xls_file(self):

        # header, records = stock_obj.read_file( file_url_xls )
        data_xls = self.read_xls( self.mf['xls_file'] )
        if not data_xls:
            return False
        header = data_xls.get('header')
        records = data_xls.get('records')
        header_dict = stock_obj.make_header_dict(header)
        
        """
        # Se revisa que el excel tenga todas las columnas que se requieren para el proceso
        """
        cols_required = ['codigo_de_producto', 'sku', 'cantidad']
        cols_not_found = stock_obj.check_keys_and_missing(cols_required, header_dict)
        if cols_not_found:
            cols_not_found = [ c.replace('_', ' ').title() for c in cols_not_found ]
            self.LKFException( f'Se requieren las columnas: {stock_obj.list_to_str(cols_not_found)}' )

        """
        # Se revisan los renglones del excel para verificar que los codigos y skus existan en el catalogo
        """
        dict_products_skus = self.get_skus_records()

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
        for pos_row, rec in enumerate(records):
            num_row = pos_row + 2
            product_code = rec[pos_codigo]
            sku = rec[pos_sku]
            cantidad = rec[pos_cantidad]
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
                self.SKU_OBJ_ID: info_product,
                self.f['lot_number'] : "LotePCI001",
                self.f['inv_adjust_grp_status'] : "todo",
                self.f['move_group_qty'] : cantidad,
            })
        if error_rows:
            self.LKFException( stock_obj.list_to_str(error_rows) )
        if self.answers.get( self.f['move_group'] ):
            self.answers[ self.f['move_group'] ] += sets_to_products
        else:
            self.answers[ self.f['move_group'] ] = sets_to_products

    def read_xls_onts(self):

        # header, records = stock_obj.read_file( file_url_xls )
        data_xls = self.read_xls( self.mf['xls_onts'] )
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
        error_rows = []
        move_group = self.answers.get( self.f['move_group'], [] )
        if move_group:
            capture_num_serie = stock_obj.unlist( move_group[0].get( self.SKU_OBJ_ID, {} ).get( self.mf['capture_num_serie'] ) ) == 'Si'
            cantidad_solicitada = move_group[0].get( self.f['move_group_qty'], 0 )
            if capture_num_serie and cantidad_solicitada:
                if cantidad_solicitada != len( records ):
                    self.LKFException( {
                        'msg': f"La cantidad de series requeridas {cantidad_solicitada} no corresponde con las capturadas {len( records )}"
                    } )
                series_unique = []
                series_repeated = []
                sets_to_series = []
                for row in records:
                    num_serie = row[ pos_serie ]
                    if num_serie not in series_unique:
                        series_unique.append(num_serie)
                    else:
                        series_repeated.append( num_serie )
                    sets_to_series.append({ 
                        self.mf['num_serie'] : num_serie
                    })
                if series_repeated:
                    self.LKFException( 'Se encontraron series repetidas en el excel: {}'.format( stock_obj.list_to_str(series_repeated) ) )
                self.answers[ self.mf['series_group'] ] = sets_to_series

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

            capture_num_serie = set_product.get( self.SKU_OBJ_ID, {} ).get( self.mf['capture_num_serie'] )
            if capture_num_serie and type(capture_num_serie[0]) == list:
                set_product[ self.SKU_OBJ_ID ][ self.mf['capture_num_serie'] ] = [ stock_obj.unlist(capture_num_serie) ]

            capture_num_serie = stock_obj.unlist( set_product.get( self.SKU_OBJ_ID, {} ).get( self.mf['capture_num_serie'] ) ) == 'Si'
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
                    row_set = copy.deepcopy(new_set)
                    row_set[ self.f['move_group_qty'] ] = 1
                    row_set[ self.f['lot_number'] ] = serie_set.get( self.mf['num_serie'] )
                    self.answers[ self.f['move_group'] ].insert( idx, row_set )
        
        if series_repeated:
            self.LKFException( stock_obj.list_to_str( [f'Codigo {i[1]} esta repetido en el Set {i[0]}' for i in series_repeated] ) )
        self.answers[self.mf['series_group']] = []
        return True

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    response = stock_obj.move_in()
    print('TODO: revisar si un create no estuvo bien y ponerlo en error o algo')
    stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'
    print('asi termina', stock_obj.answers)
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        }))
