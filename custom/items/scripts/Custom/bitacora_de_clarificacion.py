# -*- coding: utf-8 -*-
import sys, simplejson, re
from custom_utils import Custom
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
    
    def bitacora_de_clarificacion(self):
        fields_to_group, fields_to_catalog, fields_generals = self.get_fields_variables()
        print('\n === fields_to_group =',simplejson.dumps(fields_to_group, indent=4))
        print('\n === fields_to_catalog =',simplejson.dumps(fields_to_catalog, indent=4))
        print('\n === fields_generals =',simplejson.dumps(fields_generals, indent=4))

    def get_fields_variables(self):
        """
        Se consulta la estructura de la forma y se obtienen los campos que seran necesarios para 
        el grupo de variables
        """
        form_fields = lkf_obj.lkf_api.get_form_id_fields(self.FORM_BITACORA)
        fields = form_fields[0]['fields']

        # Se procesan los campos y se obtienen los que empiecen por aaaaa
        fields_to_group, fields_to_catalog, fields_generals = {}, {}, {}

        for f in fields:
            field_id = f.get('field_id', '')
            if re.match(r"^(aaaaa|ccaaa)", field_id):

                data_field = {
                    'label': f['label'], 
                    'field_type': f['field_type'],
                    'help_text': f['help_text']
                }

                catalog_field_id = f['catalog'].get('catalog_field_id') if f.get('catalog') else None
                group_id = f['group'].get('group_id') if f.get('group') else None

                if group_id:
                    fields_to_group.setdefault(group_id, {})[field_id] = data_field
                elif catalog_field_id:
                    fields_to_catalog.setdefault(catalog_field_id, {})[field_id] = data_field
                else:
                    fields_generals[field_id] = data_field
        
        # fields_variables = {
        #     f['field_id']: {
        #         'label': f['label'], 
        #         'field_type': f['field_type'],
        #         'help_text': f['help_text'],
        #         'catalog_field_id': f['catalog'].get('catalog_field_id') if f.get('catalog') else None,
        #         'group_id': f['group'].get('group_id') if f.get('group') else None
        #     }
        #     for f in fields
        #     if re.match(r"^(aaaaa|ccaaa)", f.get('field_id', ''))
        # }
        return fields_to_group, fields_to_catalog, fields_generals


if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    lkf_obj.bitacora_de_clarificacion()