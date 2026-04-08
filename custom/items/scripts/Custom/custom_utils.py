# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.custom.app import Custom


class Custom(Custom):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
    
    def get_fields_ponderables(self, with_comentarios_field=False):
        """
        Genera un diccionario de páginas y los campos que tienen configurada una ponderacion
        """
        form_fields = self.lkf_api.get_form_id_fields(self.form_id)

        pages_fields_ponderables = {}
        for page in form_fields[0].get('form_pages', []):
            page_name = page['page_name']
            for n, f in enumerate( page.get('page_fields', []) ):
                if f.get('field_type') == 'radio': # and f.get('grading_criteria'):
                    options = {option['value'] for option in f.get('options', [])}
                    if {"cumple", "no_cumple"}.issubset(options):
                        if with_comentarios_field:
                            pages_fields_ponderables[ f['field_id'] ] = {
                                'label_field': f['label'],
                                'field_comentarios': page['page_fields'][ n + 1 ]['field_id']
                            }
                            continue
                        pages_fields_ponderables.setdefault( page_name, [] ).append( f['field_id'] )

        return pages_fields_ponderables

    def get_valor_porcentual_base_form(self):
        valor_porcentual_base = self.data.get('valor_porcentual_base')
        if not valor_porcentual_base:
            print('[ERROR] no se encontro el parametro valor_porcentual_base')
            return None

        try:
            valor_porcentual_base = int( valor_porcentual_base.strip() )
        except:
            print('[ERROR] el parametro valor_porcentual_base debe ser un entero')
            return None

        return valor_porcentual_base