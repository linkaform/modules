# -*- coding: utf-8 -*-
import sys, simplejson, time
# from linkaform_api import settings, base
from bson import ObjectId
from datetime import datetime
from custom_utils import Custom
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    # def get_form_name(self):
    #     form_data = lkf_obj.lkf_api.get_item(lkf_obj.form_id, 'form', jwt_settings_key='JWT_KEY')
    #     if not form_data:
    #         print('[ERROR] no se pudo obtener la forma')
    #         return ''

    #     objects_form = form_data.get('json', {}).get('objects', [])
    #     if not objects_form:
    #         print('[ERROR] no se encontraron los datos =',form_data)
    #         return ''

    #     return objects_form[0].get('name', '')

    # def get_str_created_at(self):
    #     created_at = lkf_obj.current_record.get('created_at')
    #     print('... created_at =',created_at)
    #     if isinstance(created_at, dict):
    #         created_at = datetime.fromtimestamp( created_at.get('$date')/1000 )
    #     return created_at.strftime('%Y-%m-%d')

    def update_current_record(self):
        # form_name = self.get_form_name()
        # str_created_at = self.get_str_created_at()
        # nombre_estacion = lkf_obj.current_record['answers'].get('67b60531f2e5f0e87a807dbc', {}).get('67b60531f2e5f0e87a807dbe', '')
        
        # print('form_name =',form_name)
        # print('str_created_at =',str_created_at)
        # print('nombre_estacion =',nombre_estacion)

        # nombre_pdf = f'{form_name} - {nombre_estacion} - {str_created_at}.pdf'

        nombre_pdf = f'{self.folio}.pdf'
        
        


        time.sleep(60)
        print('... intentando descargar el pdf desde script')
        current_record = lkf_obj.lkf_api.drop_fields_for_patch(lkf_obj.current_record)
        
        try:
            pdf_obtenido = lkf_obj.lkf_api.get_pdf_record(lkf_obj.record_id, template_id=629, send_url=True, jwt_settings_key='JWT_KEY')
            print('pdf_obtenido=',pdf_obtenido)
            pdf_to_record = [{
                'file_name': nombre_pdf,
                'file_url': pdf_obtenido.get('json', {}).get('download_url', '')
            }]
            current_record['answers']['68ee96df829dcc7b749a04d4'] = pdf_to_record
        except Exception as e:
            print('[ERROR generando pdf] =',e)
        time.sleep(60)

        # Reviso el grading antes de enviar el patch
        cr_grading = lkf_obj.net.get_collections(collection='grading')
        rec_grading = cr_grading.find_one( {'record_id': ObjectId( lkf_obj.record_id )}, {'pages.points_obtained': 1,'points_obtained': 1} )
        print('\n    ... ... rec_grading = ',rec_grading)

        # Se integra la Geolocalizacion
        geolocalization = lkf_obj.current_record.get('geolocalization',[])
        print('--- geolocalization =',geolocalization)
        current_record['geolocalization'] = geolocalization
        
        # Campo para enviar el email como "Si" y patch al current_record
        current_record['answers']['68a8943e7589aafccbe966e5'] = 'sí'
        resp_patch = lkf_obj.lkf_api.patch_record(current_record, lkf_obj.record_id)
        print('+++ resp_patch =',resp_patch)
        return

if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    answers = lkf_obj.answers

    lkf_obj.update_current_record()