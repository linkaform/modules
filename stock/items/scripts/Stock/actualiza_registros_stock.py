# -*- coding: utf-8 -*-
import sys, simplejson, copy

from stock_utils import Stock

from account_settings import *

"""
python actualiza_registros_stock.py '{}' '{"jwt": "jwt eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InNpZ2FfbWF0ZXJpYWxlcyIsInVzZXJfaWQiOjE3ODYwLCJwYXJlbnRfaWQiOjE3ODYwLCJpc19tb2JpbGUiOmZhbHNlLCJleHAiOjE3NTk5NjkxMjIsImRldmljZV9vcyI6IndlYiIsImVtYWlsIjoiYWRtaW5wY2xpbmtAb3BlcmFjaW9ucGNpLmNvbS5teCJ9.HFRkcflClT769b4mZqf2ACxZqJhAG6DM0Fu7-upOf6FYRyKdE-OydLBVx8uowkWEc3oMnH74iUQDsYCDIQFHQJqXw7yPKZQkld4TVOgrDe4hFNaj-YLs_hEvOd27ycTd7qOZOINTbjGUiwO9-BP5AMzTKDDOhM4bzWc-iHADUiLjxp5qBmdeOTn9kPneZyS4l7rlePuv9Xq0Z9OWNzE2E6x7K_1FEaqG4DFo8_3uAVcq6ZyecBuOTDIVK4Bevp5JZ-j_wYWiyxDfoFuJ_lTSxtaJV3AfdJyMPCRRuerz_xugFMYG1aMl1FochRqEnRLBDK9U21NLxfqD8I2Ygqzancvk8A2YNPuP_beUrnufDz_UPd7H1qQapn6rb4ZYhpi9PGDVCaUf-tJU-xcP-rWLmACmIjDAAvwqErNTT0el7Q9Yvn9Fh17bnAsQK5zI7RkpB-kHGZuGYbMmxX9VN5pkpBtEPJ2Iq8tnk_WOMY4g3N5TTZiAUNfmNt0fpjPaSnjHcpPcNwnBEGfZs0V-8MBmCZMdJibpVfJoV_aPVMYmcCFyVeCGzke-o0vvRQva-Bf6Dwr_I9DfbuwfXMcIQ7mLUqdnF_7hC8Y9KL2jhugmF6VIsfE1e2XFeqxQSrtbznc9BrP7kepH52xgyiB3TjQ4NtZpYNqdKHgF0_-PG_lJV7Y"}'
"""


class Stock(Stock):


    def update_stock_record(self):
        """
        Se busca en la collecion de Stock el cache para hacer update
        """
        list_lotes = [
            "ZTEG25036B6E", "ZTEG25036E18", 
        ]
        query = {
            'form_id':self.FORM_INVENTORY_ID, 
            'deleted_at': {'$exists': False},
            # f"answers.{self.Product.SKU_OBJ_ID}.{self.Product.f['product_code']}":'1468270',
            # f"answers.{self.f['folio_recepcion']}":"REC-86",
            # # f"answers.{self.f['actuals']}":{'$exists':False},

            # "answers.6824e62e7c8af42c04a73d28.65ac6fbc070b93e656bd7fbe": "PCI Puebla",
            "answers.6824e62e7c8af42c04a73d28.6442e4831198daf81456f274": "PRESEA",
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
    stock_obj.update_stock_record()
