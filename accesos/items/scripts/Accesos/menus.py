# coding: utf-8
import dis
import re
import unicodedata
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.MENUS_CATALOG = self.lkm.catalog_id('elementos_menu')
        self.MENUS_CATALOG_ID = self.MENUS_CATALOG.get('id')
        self.MENUS_CATALOG_OBJ_ID = self.MENUS_CATALOG.get('obj_id')
        self.MENUS_FORM = self.lkm.form_id('configuracion_menus','id')

        self.menu_form_fields = {
            "username": "6759e4a7a9a6e13c7b26da33",
            "usuario_id": "638a9a99616398d2e392a9f5",
            "grupo_nombre": "638a9ab3616398d2e392a9fa",
            "grupo_id": "639b65dfaf316bacfc551ba2",
            "elementos": "69efaf4c4a59aa2591074f45",
            "menu": "69efaf883bcb25ed1458465d",
            "seccion": "69efaf883bcb25ed1458465e",
            "elemento": "69efaf883bcb25ed1458465f",
            "column": "69efb3dcfc8545da78179bfb",
            "order": "69efb3dcfc8545da78179bfa",
            "href": "69efb3dcfc8545da78179bf8",
            "type": "69efb3dcfc8545da78179bf9",
            "key": "69efb57c4a59aa2591074f4e"
        }

        self.menu_catalog_fields = {
            "catalog_menu": "69efaf883bcb25ed1458465d",
            "catalog_seccion": "69efaf883bcb25ed1458465e",
            "catalog_elemento": "69efaf883bcb25ed1458465f",
            "catalog_key": "69efb57c4a59aa2591074f4e",
            "catalog_href": "69efb3dcfc8545da78179bf8",
            "catalog_type": "69efb3dcfc8545da78179bf9",
            "catalog_order": "69efb3dcfc8545da78179bfa",
            "catalog_column": "69efb3dcfc8545da78179bfb"
        }

    def format_default_menus(self, data):
        """
        Formatea los datos de los registros obtenidos en el catalogo de ELEMENTOS MENU
        para obtener los menus.
        """
        format_data = []
        for item in data:
            format_item = {
                "menu": item.get(self.menu_catalog_fields['catalog_menu']),
                "seccion": item.get(self.menu_catalog_fields['catalog_seccion']),
                "elemento": item.get(self.menu_catalog_fields['catalog_elemento']),
                "column": item.get(self.menu_catalog_fields['catalog_column']),
                "order": item.get(self.menu_catalog_fields['catalog_order']),
                "key": item.get(self.menu_catalog_fields['catalog_key']),
                "href": item.get(self.menu_catalog_fields['catalog_href']),
                "type": item.get(self.menu_catalog_fields['catalog_type']),
            }
            format_data.append(format_item)
        return format_data
    
    def format_user_menus(self, data, default=False):
        """
        Formatea los datos de los registros obtenidos en la forma de CONFIGURACION MENUS
        para obtener los menus personalizados del usuario.
        """
        if default:
            return self.get_structured_menu(data)
        
        format_data = []
        data = self.unlist(data).get('elementos', [])
        for item in data:
            format_item = {
                "menu": item.get('menu', ''),
                "seccion": item.get('seccion', ''),
                "elemento": item.get('elemento', ''),
                "column": self.unlist(item.get('column', [])),
                "order": self.unlist(item.get('order', [])),
                "key": self.unlist(item.get('key', [])),
                "href": self.unlist(item.get('href', [])),
                "type": self.unlist(item.get('type', [])),
            }
            format_data.append(format_item)
        
        return self.get_structured_menu(format_data)

    def get_default_user_menus(self):
        """
        Obtiene los menus por default del catalogo de ELEMENTOS MENU.
        """
        mango_query = {
            "selector": {},
            "limit": 10000,
        }
        data = self.format_default_menus(self.lkf_api.search_catalog( self.MENUS_CATALOG_ID, mango_query))
        return data

    def get_user_menus(self):
        """
        Obtiene los menus personalizados para el usuario 
        actual desde los registros de la forma de CONFIGURACION MENUS.
        """
        query = [
            {"$match": {
                "form_id": self.MENUS_FORM,
                "deleted_at": {"$exists": False},
                f"answers.{self.EMPLOYEE_OBJ_ID}.{self.menu_form_fields['username']}": self.user.get('username')
            }},
            {"$project": {
                "_id": 0,
                "answers": 1
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query), ids_label_dct=self.menu_form_fields)
        default = False
        if not data:
            data = self.get_default_user_menus()
            default = True

        modules = self.format_user_menus(data, default=default)
        return modules

    def get_structured_menu(self, data):
        """
        Agrupa una lista plana de items de menú en la estructura 
        jerárquica módulo > sección > items.
        """
        modules_dict = {}

        for item in data:
            menu_label   = item.get('menu', '')
            seccion_label = item.get('seccion', '')

            if menu_label not in modules_dict:
                modules_dict[menu_label] = {
                    'id':       self.slugify(menu_label),
                    'key':      self.slugify(menu_label, '_'),
                    'label':    menu_label,
                    'icon':     None,
                    'order':    len(modules_dict) + 1,
                    'columns':  item.get('column'),
                    'sections': {}
                }

            sections = modules_dict[menu_label]['sections']

            if seccion_label not in sections:
                sections[seccion_label] = {
                    'id':     self.slugify(seccion_label),
                    'key':    self.slugify(seccion_label, '_'),
                    'label':  seccion_label,
                    'href':   item.get('href', ''),
                    'order':  len(sections) + 1,
                    'column': item.get('column') or len(sections) + 1,
                    'items':  []
                }

            sections[seccion_label]['items'].append({
                'key':   item.get('key') or self.slugify(item.get('elemento', ''), '_'),
                'label': item.get('elemento', ''),
                'type':  item.get('type', 'link'),
                'href':  item.get('href', ''),
                'order': item.get('order', len(sections[seccion_label]['items']) + 1)
            })

        modules = []
        for module in modules_dict.values():
            sections = sorted(module['sections'].values(), key=lambda s: s['order'])
            for s in sections:
                s['items'] = sorted(s['items'], key=lambda i: i['order'])
            modules.append({**module, 'sections': sections})

        return {'modules': sorted(modules, key=lambda m: m['order'])}

    def slugify(self, text, sep='-'):
        """
        Convierte un texto en un slug, 
        reemplazando espacios y caracteres especiales.
        """
        text = unicodedata.normalize('NFKD', str(text)).encode('ascii', 'ignore').decode('ascii')
        text = re.sub(r'[^\w\s-]', '', text.lower().strip())
        return re.sub(r'[\s_-]+', sep, text)

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv)
    script_obj.console_run()
    data = script_obj.data.get('data', {})
    option = data.get("option", '')

    dispatcher = {
        "get_menus": lambda: script_obj.get_user_menus(),
    }

    action = dispatcher.get(option)
    if action:
        response = action()
    else:
        response = {"error": "Opción no válida"}

    script_obj.HttpResponse({"data": response})