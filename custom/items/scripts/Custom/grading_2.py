# -*- coding: utf-8 -*-
import sys, simplejson
from custom_utils import Custom
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.valor_porcentual_by_form = {
            145465: 25
        }

    def get_fields_ponderables(self):
        """
        Genera un diccionario de páginas y los campos que tienen configurada una ponderacion
        """
        form_fields = lkf_obj.lkf_api.get_form_id_fields(lkf_obj.form_id)

        pages_fields_ponderables = {}
        for page in form_fields[0].get('form_pages', []):
            page_name = page['page_name']
            for f in page.get('page_fields', []):
                if f.get('field_type') == 'radio' and f.get('grading_criteria'):
                    pages_fields_ponderables.setdefault( page_name, [] ).append( f['field_id'] )

        return pages_fields_ponderables

    def make_grading_2(self):
        print('... ... Calculando el grading 2 ... ...')
        
        # Se obtienen las paginas y campos que son ponderables
        fields_ponderables_by_page = self.get_fields_ponderables()
        
        """
        Cada form tiene un Valor porcentual base, y la ponderacion base se obtiene dividiendo éste valor entre 
        el total de páginas ponderables.
        El resultado es la Ponderacion base X y eso es lo que vale cada sección. 
        """
        valor_porcentual_base = self.valor_porcentual_by_form.get( self.form_id, 0 )
        total_paginas_ponderables = len( fields_ponderables_by_page )
        ponderacion_base_x = round( valor_porcentual_base / total_paginas_ponderables, 2 )

        ponderacion_base_100 = round( 100 / total_paginas_ponderables )

        print(f"valor_porcentual_base = {valor_porcentual_base} | total_paginas_ponderables = {total_paginas_ponderables} | ponderacion_base_x = {ponderacion_base_x}")

        """
        Al contestas el registro. Si al menos una pregunta tiene el valor "No Cumple" se asigna cero a toda la sección, de lo contrario la seccion vale 
        lo que se obtuvo de Ponderacion base X
        """
        ponderaciones_by_page = {}
        for name_page, fields_ponderables in fields_ponderables_by_page.items():
            ponderaciones_by_page[name_page] = ponderacion_base_x
            for field_ponderable in fields_ponderables:
                if answers.get(field_ponderable) in ('no_cumple', 'no'):
                    ponderaciones_by_page[name_page] = 0
                    break

        print(f"\n - ponderaciones_by_page = {ponderaciones_by_page}")

if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    answers = lkf_obj.answers

    lkf_obj.make_grading_2()