# -*- coding: utf-8 -*-
import sys, simplejson, copy
import time

from stock_utils import Stock

from account_settings import *

"""
python actualiza_registros_stock.py '{}' '{"jwt": "jwt eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InNpZ2FfbWF0ZXJpYWxlcyIsInVzZXJfaWQiOjE3ODYwLCJwYXJlbnRfaWQiOjE3ODYwLCJpc19tb2JpbGUiOmZhbHNlLCJleHAiOjE3NjIzODA2NzgsImRldmljZV9vcyI6IndlYiIsImVtYWlsIjoiYWRtaW5wY2xpbmtAb3BlcmFjaW9ucGNpLmNvbS5teCJ9.phkrrRhpfyls4cPMqFSwDLc7OPGfXDvSOVkiAK77f4zqh07wGUfhQ81PUqJv7ShQMmPlmcHJ2lwUrBuGcapMMphTRiroy-Ae147Ssy3uUQ4rkbjXkwhrek4XvDYStJ82ukxs4gYvQuTQqZVma8By0OJRw3FtbnSbjd-XqeBq3Zb_qg1n2kS3jvXTTTr8iZ0-4HUFqrPpuOAittJZNC-bRbGbMquFlb0drzc8EeXbTYrWAxWVB1eHhH5X17omI2lVITVioK3UGGAQPd-sdY9bYfUBJrYsLQ9khID1OEKz02hEANWokFs4okZO_qOPUK7giqfJVl4gq4XUVMDnorI7RK5WZ02yNigE3k81QZIlo4whBbWNq883fVDyD-0RhqklM2nr8veLWhwVtb01VYfsTT4psQoa9L_oBMBWT48T4UoSOjOcoHqbqCWW2B-CO98PQ9CrfPr4aid1dQZqsMgh4M4PS1V3QUy3RU4q63I75MqaXO726-biEaVI3TtZptRvK4JNRrYUE1a71bX8Ed1DaTwIQ536EGk78uFZz_f7n6IuN8jGAsFzFU2R2xloG9kxBYQd8aN_z8HYpM-AHL5PbAqkuz-Wcrwkvg3hlv0l37i92i4Tm06_79ici0EJfxMXRcP2euD6niMvI7Bpx9TmBrFbl_dJSbBVwPZZZibTyJw"}'
"""


class Stock(Stock):

    def get_folios(self):
        file_path = '/tmp/folios.txt'
        with open(file_path, 'r') as file:
            folios = file.read().splitlines()
        batch_size = 36
        for i in range(0, len(folios), batch_size):
            batch = folios[i:i + batch_size]
            self.update_stock_by_folios(batch)
        return folios

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


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    #stock_obj.update_stock_record()
    stock_obj.get_folios()
