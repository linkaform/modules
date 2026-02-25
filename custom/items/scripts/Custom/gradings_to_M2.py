# -*- coding: utf-8 -*-
import sys, simplejson
from custom_utils import Custom
from bson import ObjectId
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def make_gradings_to_M2(self):
        print('... ... Calculando el grading para M2 ... ...')

        valor_porcentual_base = self.get_valor_porcentual_base_form()
        if valor_porcentual_base is None:
            return
        
        print('\n - valor_porcentual_base =',valor_porcentual_base)

        # Se obtienen las paginas y campos que son ponderables
        fields_ponderables_by_page = self.get_fields_ponderables()
        print('\n - fields_ponderables_by_page =',fields_ponderables_by_page)
        
        """
        Cada form tiene un Valor porcentual base, y la ponderacion base se obtiene dividiendo éste valor entre 
        el total de páginas ponderables.
        El resultado es la Ponderacion base X y eso es lo que vale cada sección. 
        """
        # total_paginas_ponderables = len( fields_ponderables_by_page )
        # ponderacion_base_x = round( valor_porcentual_base / total_paginas_ponderables, 2 )

        # ponderacion_base_100 = round( 100 / total_paginas_ponderables, 2 )

        # print(f"valor_porcentual_base = {valor_porcentual_base} | total_paginas_ponderables = {total_paginas_ponderables} | ponderacion_base_x = {ponderacion_base_x}")

        
        """
        Al contestas el registro. Si al menos una pregunta tiene el valor "No Cumple" se asigna cero a toda la sección, de lo contrario la seccion vale 
        lo que se obtuvo de Ponderacion base X
        """
        ponderaciones_by_page = {}
        for name_page, fields_ponderables in fields_ponderables_by_page.items():
            ponderaciones_by_page[name_page] = {
                # 'grading_1': ponderacion_base_x, 
                # 'grading_2': ponderacion_base_100, 
                'fields_cumplen': [], 
                'fields_considerados': []
            }

            for field_ponderable in fields_ponderables:

                value_field_ponderable = answers.get(field_ponderable)
                if not value_field_ponderable:
                    continue
                
                ponderaciones_by_page[name_page]['fields_considerados'].append( field_ponderable )

                if value_field_ponderable == 'cumple':
                    ponderaciones_by_page[name_page]['fields_cumplen'].append( field_ponderable )

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

        total_points_available, total_points_obtained = 0, 0
        total_points_available_2, total_points_obtained_2 = 0, 0
        for page in rec_grading.get('pages', []):
            ponderaciones = ponderaciones_by_page.get( page['name'], {} )
            if not ponderaciones:
                print(f"Pagina {page['name']} no esta en las ponderaciones")
                continue
            
            # Se agrega el grading de los campos
            fields_considerados = ponderaciones['fields_considerados']
            fields_cumplen = ponderaciones['fields_cumplen']
            
            total_fields_considerados = len( fields_considerados )
            grading_1_field = round(valor_porcentual_base / total_fields_considerados, 2)
            grading_2_field = round(100 / total_fields_considerados, 2)

            page_points_available, page_points_obtained = 0, 0
            page_points_available_2, page_points_obtained_2 = 0, 0
            
            for field_page in page.get('fields', []):
                field_id = str( field_page['field_id'] )
                
                val_grad_1, val_grad_2 = 0, 0
                field_page['points_available'] = 0
                field_page['points_available_2'] = 0

                if field_id in fields_considerados:
                    if field_id in fields_cumplen:
                        val_grad_1 = grading_1_field
                        val_grad_2 = grading_2_field
                
                    field_page['points_available'] = grading_1_field
                    field_page['points_available_2'] = grading_2_field
                    page_points_available += grading_1_field
                    page_points_available_2 += grading_2_field
                
                field_page['points_obtained'] = val_grad_1
                field_page['points_obtained_2'] = val_grad_2
                page_points_obtained += val_grad_1
                page_points_obtained_2 += val_grad_2

            # Se agrega el Grading 1
            page['points_available'] = page_points_available
            page['points_obtained'] = page_points_obtained

            # Se agrega el Grading 2
            page['points_available_2'] = page_points_available_2
            page['points_obtained_2'] = page_points_obtained_2

            total_points_available += page_points_available
            total_points_obtained += page_points_obtained
            total_points_available_2 += page_points_available_2
            total_points_obtained_2 += page_points_obtained_2

        # Se guarda el grading con las páginas ya modificadas
        resp_update_grading = cr_grading.update_one( query_grading, {'$set': {
            'pages': rec_grading['pages'],
            'points_available': total_points_available,
            'points_obtained': total_points_obtained,
            'points_available_2': total_points_available_2,
            'points_obtained_2': total_points_obtained_2,
        }} )
        print(f"\nMatched count: {resp_update_grading.matched_count} Modified count: {resp_update_grading.matched_count}", )

if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    answers = lkf_obj.answers

    lkf_obj.make_gradings_to_M2()