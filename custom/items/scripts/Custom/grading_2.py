# -*- coding: utf-8 -*-
import sys, simplejson
from custom_utils import Custom
from bson import ObjectId
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def get_fields_ponderables(self):
        """
        Genera un diccionario de páginas y los campos que tienen configurada una ponderacion
        """
        form_fields = lkf_obj.lkf_api.get_form_id_fields(lkf_obj.form_id)

        pages_fields_ponderables = {}
        for page in form_fields[0].get('form_pages', []):
            page_name = page['page_name']
            for f in page.get('page_fields', []):
                if f.get('field_type') == 'radio': # and f.get('grading_criteria'):
                    options = {option['value'] for option in f.get('options', [])}
                    if {"cumple", "no_cumple"}.issubset(options):
                        pages_fields_ponderables.setdefault( page_name, [] ).append( f['field_id'] )


        return pages_fields_ponderables

    def make_grading_2(self):
        print('... ... Calculando el grading 2 ... ...')

        valor_porcentual_base = self.data.get('valor_porcentual_base')
        if not valor_porcentual_base:
            print('[ERROR] no se encontro el parametro valor_porcentual_base')
            return

        try:
            valor_porcentual_base = int( valor_porcentual_base.strip() )
        except:
            print('[ERROR] el parametro valor_porcentual_base debe ser un entero')
            return
        
        # Se obtienen las paginas y campos que son ponderables
        fields_ponderables_by_page = self.get_fields_ponderables()
        
        """
        Cada form tiene un Valor porcentual base, y la ponderacion base se obtiene dividiendo éste valor entre 
        el total de páginas ponderables.
        El resultado es la Ponderacion base X y eso es lo que vale cada sección. 
        """
        total_paginas_ponderables = len( fields_ponderables_by_page )
        ponderacion_base_x = round( valor_porcentual_base / total_paginas_ponderables, 2 )

        ponderacion_base_100 = round( 100 / total_paginas_ponderables, 2 )

        print(f"valor_porcentual_base = {valor_porcentual_base} | total_paginas_ponderables = {total_paginas_ponderables} | ponderacion_base_x = {ponderacion_base_x}")

        """
        Al contestas el registro. Si al menos una pregunta tiene el valor "No Cumple" se asigna cero a toda la sección, de lo contrario la seccion vale 
        lo que se obtuvo de Ponderacion base X
        """
        ponderaciones_by_page = {}
        for name_page, fields_ponderables in fields_ponderables_by_page.items():
            # Grading 1
            ponderaciones_by_page[name_page] = {'grading_1': ponderacion_base_x, 'grading_2': ponderacion_base_100}
            for field_ponderable in fields_ponderables:
                if answers.get(field_ponderable) in ('no_cumple', 'no'):
                    ponderaciones_by_page[name_page] = {'grading_1': 0, 'grading_2': 0}
                    break

        print(f"\n - ponderaciones_by_page = {ponderaciones_by_page}")

        """
        Se consulta el grading del registro para editarlo y meter los que se calcularon
        """
        query_grading = {'record_id': ObjectId( self.record_id )}
        cr_grading = self.net.get_collections(collection='grading')
        rec_grading = cr_grading.find_one( query_grading )
        print(f'\n\n - grading = {rec_grading}')
        if not rec_grading:
            print(f'[ERROR] grading no encontrado para el registro = {self.record_id}')
            return

        for page in rec_grading.get('pages', []):
            ponderaciones = ponderaciones_by_page.get( page['name'], {} )
            if not ponderaciones:
                print(f"Pagina {page['name']} no esta en las ponderaciones")
                continue

            # Se agrega el Grading 1
            page['points_available'] = ponderacion_base_x
            page['points_obtained'] = ponderaciones['grading_1']

            # Se agrega el Grading 2
            page['points_available_2'] = ponderacion_base_100
            page['points_obtained_2'] = ponderaciones['grading_2']

        # Se guarda el grading con las páginas ya modificadas
        resp_update_grading = cr_grading.update_one( query_grading, {'$set': {'pages': rec_grading['pages']}} )
        print(f"\nMatched count: {resp_update_grading.matched_count} Modified count: {resp_update_grading.matched_count}", )

if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    answers = lkf_obj.answers

    lkf_obj.make_grading_2()