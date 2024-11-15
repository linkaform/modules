import sys
from linkaform_api import settings, base
from account_settings import *


def get_tableau_collection():
    get_tableau = lkf.net.get_collections(collection='Tableau')
    return get_tableau


def get_tableau_items():
    tableau_collection = get_tableau_collection()
    tableau_find = tableau_collection.find()
    tableau_items = list(tableau_find)
    
    for item in tableau_items:
        if '_id' in item:
            del item['_id']  # Eliminar el campo _id
    
    return tableau_items
    
    
def process_report_and_send_data():
    form_id_email = 125483
    metadata = lkf.lkf_api.get_metadata(form_id_email)
    tableau_items = get_tableau_items()
    if tableau_items:
        header = list(tableau_items[0].keys())
        records = [list(item.values()) for item in tableau_items]
        
        resp_xls = lkf.lkf_api.make_excel_file(header, records, form_id_email, '67351ca2a02e74e7321196e2', upload_name='Excel de pruebas')
        metadata['answers'] = {
            '67169f72c736cc47794404f8': 'victor@katusak.com',
            '67169f72c736cc47794404f9': 'correo de prueba',
            '670d2e32756833542954716b': 'Gabriel',
            '670d2e32756833542954716c': 'victor_gabriel.bueno.castro@outlook.com',
            '670d2d9d0337e410e4353550': 'correo de prueba',
            '6716a1067f394110d24404eb': 'mensaje enviado desde script',
            '67337e9c6ff7bc277a99bc09': [resp_xls.pop('67351ca2a02e74e7321196e2')],
        }
        
        resp_create = lkf.lkf_api.post_forms_answers(metadata)
    
          
if __name__ == '__main__':
    lkf = base.LKF_Base(settings, sys_argv=sys.argv, use_api=True)
    process_report_and_send_data()