# -*- coding: utf-8 -*-
import sys, simplejson

from stock_utils import Stock

from account_settings import *

class Stock(Stock):

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
            productTipoMaterial = r.get( self.f['product_material'] )
            if not productCode or not productSku:
                continue
            dict_skus[ f'{productCode}_{productSku}' ] = {
                self.f['product_name']: [productName,],
                self.f['product_material']: [productTipoMaterial,],
            }
        return dict_skus

    def show_error_app(self, field_id, label, msg):
        raise Exception( simplejson.dumps({
            field_id: { 'msg': [msg], 'label': label, 'error': [] }
        }) )

    def read_xls_file(self):
        # return False
        self.f.update({
            'xls_file': '66c797955cfca4851db2c3b8',
            'product_material': '66b10b87a1d4483b5369f409'
        })

        file_url_xls = self.answers.get( self.f['xls_file'] )
        if not file_url_xls:
            print('no hay excel de carga masiva')
            return False
        
        file_url_xls = file_url_xls[0].get('file_url')

        """
        Para evitar que se carguen los renglones cada que editan el registro se revisa
        si ya tiene folio, entonces es una edicion y se debe revisar si en la version previa ya existia un excel
        si ya existia un excel entonces se ignora
        """
        if self.folio and self.current_record.get('other_versions'):
            # print('entra al other_versions')
            prev_version = stock_obj.get_prev_version(self.current_record['other_versions'], select_columns=[ 'answers.{}'.format( self.f['xls_file'] ) ])
            print('prev_version=',prev_version)
            if prev_version.get('answers', {}).get( self.f['xls_file'] ):
                print( 'ya hay un excel previamente cargado... se ignora en esta ejecucion =',prev_version.get('answers', {}).get( self.f['xls_file'] ) )
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
            self.show_error_app( self.f['xls_file'], 'Excel de carga masiva', f'Se requieren las columnas: {stock_obj.list_to_str(cols_not_found)}' )

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
            self.show_error_app( self.f['xls_file'], 'Excel de carga masiva', stock_obj.list_to_str(error_rows) )
        if self.answers.get( self.f['move_group'] ):
            self.answers[ self.f['move_group'] ] += sets_to_products
        else:
            self.answers[ self.f['move_group'] ] = sets_to_products
        # self.show_error_app( 'folio', 'Folio', 'En Pruebas!' )

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    stock_obj.read_xls_file()

    response = stock_obj.move_in()
    print('TODO: revisar si un create no estuvo bien y ponerlo en error o algo')
    stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'
    print('asi termina', stock_obj.answers)
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        }))
