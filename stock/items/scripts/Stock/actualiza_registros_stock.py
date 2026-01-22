# -*- coding: utf-8 -*-
import sys, simplejson, copy
import time

from stock_utils import Stock

from account_settings import *

"""
python actualiza_registros_stock.py '{}' '{"jwt": "jwt eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InNpZ2FfbWF0ZXJpYWxlcyIsInVzZXJfaWQiOjE3ODYwLCJwYXJlbnRfaWQiOjE3ODYwLCJpc19tb2JpbGUiOmZhbHNlLCJleHAiOjE3NjIzODA2NzgsImRldmljZV9vcyI6IndlYiIsImVtYWlsIjoiYWRtaW5wY2xpbmtAb3BlcmFjaW9ucGNpLmNvbS5teCJ9.phkrrRhpfyls4cPMqFSwDLc7OPGfXDvSOVkiAK77f4zqh07wGUfhQ81PUqJv7ShQMmPlmcHJ2lwUrBuGcapMMphTRiroy-Ae147Ssy3uUQ4rkbjXkwhrek4XvDYStJ82ukxs4gYvQuTQqZVma8By0OJRw3FtbnSbjd-XqeBq3Zb_qg1n2kS3jvXTTTr8iZ0-4HUFqrPpuOAittJZNC-bRbGbMquFlb0drzc8EeXbTYrWAxWVB1eHhH5X17omI2lVITVioK3UGGAQPd-sdY9bYfUBJrYsLQ9khID1OEKz02hEANWokFs4okZO_qOPUK7giqfJVl4gq4XUVMDnorI7RK5WZ02yNigE3k81QZIlo4whBbWNq883fVDyD-0RhqklM2nr8veLWhwVtb01VYfsTT4psQoa9L_oBMBWT48T4UoSOjOcoHqbqCWW2B-CO98PQ9CrfPr4aid1dQZqsMgh4M4PS1V3QUy3RU4q63I75MqaXO726-biEaVI3TtZptRvK4JNRrYUE1a71bX8Ed1DaTwIQ536EGk78uFZz_f7n6IuN8jGAsFzFU2R2xloG9kxBYQd8aN_z8HYpM-AHL5PbAqkuz-Wcrwkvg3hlv0l37i92i4Tm06_79ici0EJfxMXRcP2euD6niMvI7Bpx9TmBrFbl_dJSbBVwPZZZibTyJw"}'
"""


class Stock(Stock):


    def get_stock_rec_by_lot_number(self, lot_numbers, product_code, sku):
        #use to be get_record_greenhouse_inventory
        query = {
            'form_id':self.FORM_INVENTORY_ID, 
            'deleted_at': {'$exists': False},
            f"answers.{self.Product.SKU_OBJ_ID}.{self.f['product_code']}": product_code,
            f"answers.{self.Product.SKU_OBJ_ID}.{self.f['sku']}": sku,
            f"answers.{self.f['product_lot']}": {"$in":lot_numbers},
        }
        print('query', query)
        records = self.cr.find(query, 
            {'folio': 1, 'answers': 1, 'form_id': 1, 'user_id': 1,'created_at':1}).sort('created_at')
        record = [x for x in records]
        return record

    def get_folios(self):

        answer_file = self.answers[self.mf['xls_file']]
        file_url = answer_file[0]['file_url'] if type(answer_file) == list else answer_file['file_url']
        print('********************** file_url=',file_url)
        header, records = self.upfile.read_file(file_url=file_url)

        print('header', header )
        print('records', records )
        rec = [r[0] for r in records]
        return rec
        # file_path = '/tmp/folios.txt'
        # with open(file_path, 'r') as file:
        #     folios = file.read().splitlines()
        # batch_size = 512
        # for i in range(0, len(folios), batch_size):
        #     batch = folios[i:i + batch_size]
        #     self.update_stock_by_folios(batch)
        # return folios

    def update_stock_by_folios(self,folios):
        print('Cantidad de folios=', len(folios))
        answer = {self.f['product_grading_pending']:'pendiente'}
        timein = time.time()
        res = self.lkf_api.patch_multi_record(answer, self.FORM_INVENTORY_ID,  folios=folios, threading=True)
        print(f'{len(folios)} folios en time=', time.time() - timein)

    def update_stock_record(self):
        """
        Se busca en la collecion de Stock el cache para hacer update
        """
        list_lotes = [
            
        ]
        query = {
            'form_id':self.FORM_INVENTORY_ID, 
            'deleted_at': {'$exists': False},
            # f"answers.{self.Product.SKU_OBJ_ID}.{self.Product.f['product_code']}":'1468270',
            # f"answers.{self.f['folio_recepcion']}":"REC-86",
            # # f"answers.{self.f['actuals']}":{'$exists':False},

            "answers.6824e62e7c8af42c04a73d28.65ac6fbc070b93e656bd7fbe": "PCI Puebla",
            # "answers.6824e62e7c8af42c04a73d28.6442e4831198daf81456f274": "PRESEA",
            f"answers.{self.f['lot_number']}": {'$in': list_lotes},
            "answers.620ad6247a217dbcb888d175": "done" # "done" / "active"

            # "answers.644c36f1d20db114694a495a": "auditado",
            # 'answers.679bd3e1e213a8e4c124c05c': 'REC-71',
        }
        print('query=', simplejson.dumps(query, indent=3))
        records = self.cr.find(query,{'folio':1, 'answers':1})#.limit(1000)

        #self.cr.update_many(query, {'$unset': {'connection_record_id': ''}})

        print('pasa recors.....')
        update_ids = []
        for rec in records:
            answers = rec.get('answers',{})
            if not answers:
                continue
            rec_id = rec.get('_id')
            # lot_number = answers.get(self.f['lot_number'])
            # query = {
            # 'form_id':self.FORM_INVENTORY_ID,
            # 'deleted_at': {'$exists': False},
            # '_id': {'$ne':rec_id},
            # f"answers.{self.Product.SKU_OBJ_ID}.{self.Product.f['product_code']}":'1468270',
            # f"answers.{self.f['lot_number']}":lot_number,
            #  }
            # old_rec = self.cr.find(query,{'folio':1})
            # for x in old_rec:
            #     update_ids.append(str(x.get('_id')))
            print('rec', rec_id)
            update_ids.append(str(rec_id))
        print('Size of updated ids:' , len(update_ids))
        if update_ids:
            answer = {self.f['product_grading_pending']:'pendiente'}
            # answer = {'6442e2fbc0dd855fe856fddd': 0} # Le pongo cuarentena en cero nomas para editar
            print('Doing patch_multi_record', update_ids)
            res = self.lkf_api.patch_multi_record(answer, self.FORM_INVENTORY_ID,  record_id=update_ids, threading=True)
            print('\n +++ res =',res)
                

        return True

    def update_actuals(self, stock):
        res = {}
        res[self.f['product_lot_produced']] = stock['production']
        res[self.f['move_in']] = stock['move_in']
        res[self.f['move_out']] = stock['move_out']
        res[self.f['product_lot_sales']] = stock['sales']
        res[self.f['cuarentin']] = stock['cuarentin']
        res[self.f['product_lot_scrapped']] = stock['scrapped']
        res[self.f['adjustments']] = stock['adjustments']
        res[self.f['actuals']] = stock['actuals']
        if stock['actuals'] <= 0:
            res[self.f['status']] = 'done'
        else:
            res[self.f['status']] = 'active'
        return res

    def update_stock_lot_number(self, lot_numbers):
        """
        Args:
        lot_numbers (list): Lista con numeros de lote:
        Acutaliza los registros de stock por numero de lote...
        1. Obtiene todos los regisros de stock de ese numeros de lotes
        1 Primero busca en que almacenes se encuentra el lote
        2 Solicita o calcula el stock del lote o registro
        3 Si es disitnto al actual, lo actualiza directo a base de datos
        """
        product_code = '1468270'
        sku = 'F1468270'
        stock_records = self.get_stock_rec_by_lot_number(lot_numbers, product_code, sku)
        print(f'Evaluando: {len(stock_records)} ONTs')
        for record in stock_records:
            rec  = self._labels(record['answers'])
            actuals = rec['actual_eaches_on_hand']
            warehouse = rec['warehouse']
            location = rec['warehouse_location']
            product_code = rec['product_code']
            sku = rec['sku']
            product_lot = rec['product_lot']
            stock = self.get_product_stock(product_code, sku, product_lot, warehouse, location)
            if actuals != stock.get('actuals',0):
                print(f'ONT: {product_lot} warehouse: {warehouse}, location: {location}, ACTUALS: {actuals} / Stock: {stock["actuals"]}' )
                update_stock = self.update_actuals(stock)
                record['answers'].update(update_stock)
                update_res = self.cr.update_one({'_id':record['_id']}, {'$set':{'answers':record['answers']}})

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    #stock_obj.update_stock_record()
    try:
        stock_obj.update_status_record('procesando')
        lot_numbers = stock_obj.get_folios()
        print('lot_numbers, ', lot_numbers)
        stock_obj.update_stock_lot_number(lot_numbers)
        stock_obj.update_status_record('realizado')
    except:
        stock_obj.update_status_record('error')
