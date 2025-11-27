# coding: utf-8
from copy import deepcopy
import sys, simplejson, time, datetime
from linkaform_api import settings
from account_settings import *

from oracle import Oracle

class Oracle(Oracle):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.class_cr = self.get_db_cr('Oracle')
        #use self.lkm.catalog_id() to get catalog id
        self.name =  __class__.__name__
        self.settings = settings
        self.QUERY_FORM_ID = 143654
        self.m = {
            'fecha':'691f4838444e45f9f6e2e648',
            'variable':'691f4838444e45f9f6e2e649',
            'resultado':'691f4838444e45f9f6e2e64a',
            'query':'69278e5a49292b0e75a2f285'
        }

    def get_form_model(self):
        """
        Obtiene el modelo de la forma
        """
        metadata = self.lkf_api.get_metadata(self.QUERY_FORM_ID)
        properties = {
                "device_properties":{
                    "system": "Script",
                    "process": "Sync Catalogs", 
                    "accion": 'sync_catalogs', 
                    "archive": "oracle/sync_catalogs.py",
                },
            }
        metadata.update({
            'properties': properties,
            'answers': {}
            },
        )
        return metadata

    def run_query(self, variable):
        db_name = 'PRODUCCION.VW_LinkAForm_Hora'
        query = "SELECT * FROM PRODUCCION.VW_LinkAForm_Hora "
        query += " WHERE "
        query += " VARIA='{}'".format(variable)
        query += """ AND TRUNC(DATA) + (TO_DATE(HORA, 'HH24:MI') - TRUNC(TO_DATE(HORA, 'HH24:MI')))
                >= TO_DATE('2025-11-20 00:00', 'YYYY-MM-DD HH24:MI')
        """
        form_model = self.get_form_model()
        print('query: ', query)
        answers = {}
        answers[self.m['query']] = query
        for x in range(2):
            header, data = self.query_view(db_name, query=query, date_format=True)
            res = ''
            for d in data:
                res +='row = DATA: {} , Hora: {}, Valor: {}'.format(d['DATA'],d['HORA'],d['VALOR'])
                res += "\n"
            answers[self.m['fecha']] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            answers[self.m['variable']] = variable
            answers[self.m['resultado']] = res
            form_model['answers'] = answers
            res = self.lkf_api.post_forms_answers_list(form_model)
            time.sleep(1)



if __name__ == "__main__":
    """
    Formato last_update_date = '2025-08-25 15:00'
    """
    module_obj = Oracle(settings, sys_argv=sys.argv)
    module_obj.console_run()
    module_obj.db_updated_at = time.time()
    variable = module_obj.data.get("variable",)
    module_obj.run_query(variable)