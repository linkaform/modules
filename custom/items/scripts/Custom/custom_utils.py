# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.custom.app import Custom


class Custom(Custom):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.FORM_ID_PROGRAMACION = 150324
        self.FORM_ID_CONVERSION = 145748
        self.FORM_ID_MOLINOS = 148658

        self.field_anio = "69dfc83706c1197cec034b39"
        self.field_mes = "69d82f9651b77ef10d63b785"
        self.field_semana = "69d82edb3203a903fb3fd9fd"
        self.field_grupo_areas = "69dfc84f6748944372b3d533"
        
        self.obj_plantas_areas = "696133f0829d117f5e819e8c"
        self.field_planta = "696130ce57ba2b8308adef4d"
        self.field_area = "696133f1829d117f5e819e8d"
        
        self.obj_usuarios = "696517d545ba5981006be647"
        self.field_responsable = "638a9a7767c332f5d459fc81"
        self.field_email = "638a9a7767c332f5d459fc82"
        self.field_username = "6759e4a7a9a6e13c7b26da33"
        
    
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