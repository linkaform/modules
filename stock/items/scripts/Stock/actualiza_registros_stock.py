# -*- coding: utf-8 -*-
import sys, simplejson, copy

from stock_utils import Stock

from account_settings import *

"""
python actualiza_registros_stock.py '{}' '{"jwt": "jwt eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InNpZ2FfbWF0ZXJpYWxlcyIsInVzZXJfaWQiOjE3ODYwLCJwYXJlbnRfaWQiOjE3ODYwLCJpc19tb2JpbGUiOmZhbHNlLCJleHAiOjE3NTgzMDIwMzQsImRldmljZV9vcyI6IndlYiIsImVtYWlsIjoiYWRtaW5wY2xpbmtAb3BlcmFjaW9ucGNpLmNvbS5teCJ9.VXK04oP2KmgjLxMdLnh2iUVpl-VrGJREQF5lg1IZVwwWzvHDwsRxZw0fWow-Yq2hnOim1S9YV3Sj9Y7-KCA-7rdapcaIW-dQ5meliIvVh9YACIFucEYk7ZSoZpzARZXJvwpuiFYjKY82dq2ga8j98iI0-Fse5gqUzcllK4lPHweJ1Q-6k_vAzOQxLnNpUYNWxYzfMqR2OeL8odM77Y5-iV_C8ix5cgHYLoPgNSpL12ZBcr9m23PHXiMm1Qv0Oa-gzT35MxwlbcpiDdhljyYWGbuBpBjGZn8lYd9Oqi-ZEjQpwgQfacMyM-lPPBQMwCYpQi-m3pOvJFVZBeltIp4FI5B0HtGrk6EBDiZtBLYbj5IuIHAS2381hfrecDWmlI1nx749cqgP9KUtvcyu0PBL-8GihqH2EpQ-hFRZDjdpKjp6yS2iL3M1dzGwg43QqBYnSM9f8986L1zb3Tq0FywI5PN1fROQXm4pgEPP2MOnmYknNkCZ7Z8REWCFIVOwyZEG2J5CdCUpWEl7CBfjR94UXcRw1sdv4Blm9jLsa5o06K-1S11pAz2gOFpLiwSfFh13PWhtjtk69JTauBtRzLKI9K_nkMRNK9EmQeOg5yt6J8zLivtSGbRmc4tU5Bpb9VXroEMbsKzbY8Lu4rlFqfbnPmIyFJOuEu9R1fUF35Hoa-4"}'
"""


class Stock(Stock):


    def update_stock_record(self):
        """
        Se busca en la collecion de Stock el cache para hacer update
        """
        list_lotes = [
            "ALCLFE0917E9", "ALCLFE091655", "ALCLFE08F23A", "ALCLFE0921C6", "ALCLFE08F9C0", "ALCLFE0913B8", "ALCLFE08F976", "ALCLFE08E939", 
            "ALCLFE090A91", "ALCLFE08FDE5", "ALCLFE09122C", "ALCLFE08EB82", "ALCLFE08F1D1", "ALCLFE08E9DC", "ALCLFE090056", "ALCLFE08FABB", 
            "ALCLFE08F3CE", "ALCLFE0905A0", "ALCLFE08EBE4", "ALCLFE08F4CB", "ALCLFE08F732", "ALCLFE08F4D8", "ALCLFE09092E", "ALCLFE08EEB3", 
            "ALCLFE09160B", "ALCLFE09009C", "ALCLFE091A09", "ALCLFE0913FE", "ALCLFE091623", "ALCLFE08F317", "ALCLFE08FE1B", "ALCLFE0916FB", 
            "ALCLFE0916F2", "ALCLFE0922C8", "ALCLFE08F94F", "ALCLFE092322", "ALCLFE0918EF", "ALCLFE08F1EA", "ALCLFE091E82", "ALCLFE09103B", 
            "ALCLFE08F316", "ALCLFE08E984", "ALCLFE08F8F3", "ALCLFE08EEEE", "ALCLFE08EAE1", "ALCLFE08EB74", "ALCLFE08F420", "ALCLFE0911B0", 
            "ALCLFE0915EF", "ALCLFE08F2D6", "ALCLFE08F74A", "ALCLFE0900A8", "ALCLFE08EBBC", "ALCLFE0922F7", "ALCLFE08F9EE", "ALCLFE0915F9", 
            "ALCLFE08EF11", "ALCLFE08F8A8", "ALCLFE091647", "ALCLFE08FAE3", "ALCLFE09163F", "ALCLFE08FCC1", "ALCLFE092222", "ALCLFE08F3A1", 
            "ALCLFE08F715", "ALCLFE0916F4", "ALCLFE08FAB8", "ALCLFE08EB79", "ALCLFE08F4C7", "ALCLFE09124F", "ALCLFE090092", "ALCLFE08ECBD", 
            "ALCLFE08E9FB", "ALCLFE08F10B", "ALCLFE091881", "ALCLFE08FAC7", "ALCLFE08E8EC", "ALCLFE08F9FB", "ALCLFE091691", "ALCLFE08F71A", 
            "ALCLFE08E860", "ALCLFE09005C", "ALCLFE0922C7", "ALCLFE08EAAA", "ALCLFE08FA45", "ALCLFE08EAA2", "ALCLFE08F8D4", "ALCLFE08F716", 
            "ALCLFE08F495", "ALCLFE08FA67"
        ]
        query = {
            'form_id':self.FORM_INVENTORY_ID, 
            'deleted_at': {'$exists': False},
            # f"answers.{self.Product.SKU_OBJ_ID}.{self.Product.f['product_code']}":'1468270',
            # f"answers.{self.f['folio_recepcion']}":"REC-86",
            # # f"answers.{self.f['actuals']}":{'$exists':False},

            "answers.6824e62e7c8af42c04a73d28.65ac6fbc070b93e656bd7fbe": "PCI Puebla",
            f"answers.{self.f['lot_number']}": {'$in': list_lotes}

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
