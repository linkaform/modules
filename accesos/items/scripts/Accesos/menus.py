# coding: utf-8
import dis
import re
import unicodedata
import sys, simplejson, json
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
            "grupo_asignado": "638a9ab3616398d2e392a9fa",
            "grupo_id": "639b65dfaf316bacfc551ba2",
            "elementos": "69efaf4c4a59aa2591074f45",
            "menu": "69efaf883bcb25ed1458465d",
            "seccion": "69efaf883bcb25ed1458465e",
            "elemento": "69efaf883bcb25ed1458465f",
            "key": "69efb57c4a59aa2591074f4e",
            "plataforms": "69f27e8cdf4d7acc80f2e9b0"
        }

        self.menu_catalog_fields = {
            "catalog_menu_key": "69f28216c76fd3bed14949a2",
            "catalog_menu": "69efaf883bcb25ed1458465d",
            "catalog_menu_order": "69f27e8cdf4d7acc80f2e9a8",
            "catalog_menu_icon": "69f27e8cdf4d7acc80f2e9a9",
            "catalog_menu_columns": "69f27e8cdf4d7acc80f2e9aa",
            "catalog_seccion_key": "69f28216c76fd3bed14949a3",
            "catalog_seccion": "69efaf883bcb25ed1458465e",
            "catalog_seccion_order": "69f27e8cdf4d7acc80f2e9ab",
            "catalog_seccion_column": "69f27e8cdf4d7acc80f2e9ac",
            "catalog_seccion_href": "6a036ef020c6e62e1c3fdee6",
            "catalog_seccion_icon": "69f27e8cdf4d7acc80f2e9ad",
            "catalog_seccion_icon_color": "69f27e8cdf4d7acc80f2e9ae",
            "catalog_elemento": "69efaf883bcb25ed1458465f",
            "catalog_key": "69efb57c4a59aa2591074f4e",
            "catalog_type": "69efb3dcfc8545da78179bf9",
            "catalog_item_order": "69efb3dcfc8545da78179bfa",
            "catalog_href_web": "69efb3dcfc8545da78179bf8",
            "catalog_route_mobile": "69f27e8cdf4d7acc80f2e9af",
            "catalog_plataforms": "69f27e8cdf4d7acc80f2e9b0"
        }

        self.f.update({
            'menus':'6722472f162366c38ebe1c64'
        })

        self.ARTICULOS_CONSECIONADOS = self.lkm.script_id('articulos_consecionados','id')
        self.ARTICULOS_PERDIDOS = self.lkm.script_id('articulos_perdidos','id')
        self.FALLAS = self.lkm.script_id('fallas','id')
        self.GET_STATS = self.lkm.script_id('get_stats','id')
        self.GAFETES_LOCKERS = self.lkm.script_id('gafetes_lockers','id')
        self.NOTAS = self.lkm.script_id('notes','id')
        self.PAQUETERIA = self.lkm.script_id('paqueteria','id')
        self.SCRIPT_TURNOS = self.lkm.script_id('script_turnos','id')
        self.SCRIPT_PASE_ACCESO = self.lkm.script_id('pase_de_acceso','id')
        self.SCRIPT_PASE_ACCESO_API = self.lkm.script_id('pase_de_acceso_use_api','id')
        self.SCRIPT_GOOGLE_WALLET = self.lkm.script_id('create_pass_google_wallet','id')
        self.SCRIPT_RONDINES = self.lkm.script_id('rondines','id')
        self.OFFLINE_SERVICES = self.lkm.script_id('offline_services','id')
        self.OCR_DOCS = self.lkm.script_id('ocr_docs','id')
        self.SCRIPT_MENUS = self.lkm.script_id('menus','id')
        self.FILTERS = self.lkm.script_id('filters','id')
        self.SCRIPT_INCIDENCIAS = self.lkm.script_id('incidencias','id')

        self.module_permits = {
            'always':{
                'forms':[],
                'catalogs':[
                    self.ACTIVOS_FIJOS_CAT_ID,
                    self.AREAS_DE_LAS_UBICACIONES_CAT_ID,
                    self.CATEGORIAS_INCIDENCIAS_ID,
                    self.CONFIGURACION_RECORRIDOS_ID,
                    self.CONF_AREA_EMPLEADOS_AP_CAT_ID,
                    self.CONF_AREA_EMPLEADOS_CAT_ID,
                    self.ESTADO_ID,
                    self.LISTA_FALLAS_CAT_ID,
                    self.LISTA_INCIDENCIAS_CAT_ID,
                    self.LOCKERS_CAT_ID,
                    self.PASE_ENTRADA_ID,
                    self.PROVEEDORES_CAT_ID,
                    self.SUB_CATEGORIAS_INCIDENCIAS_ID,
                    self.TIPO_ARTICULOS_PERDIDOS_CAT_ID,
                    self.TIPO_DE_EQUIPO_ID,
                    self.UBICACIONES_CAT_ID,
                    self.USUARIOS_ID,
                    self.VISITA_AUTORIZADA_CAT_ID,
                    self.MENUS_CATALOG_ID,
                    self.OCR_DOCS
                ],
                'scripts':[self.OFFLINE_SERVICES, self.SCRIPT_MENUS, self.FILTERS]
            },
            'accesos':{
                'forms':[self.CHECKIN_CASETAS, self.REGISTRO_ASISTENCIA, self.BITACORA_GAFETES_LOCKERS, self.CHECK_UBICACIONES, self.BITACORA_ACCESOS],
                'catalogs':[],
                'scripts':[]
            },
            'seguridad':{
                'forms':[self.CONFIGURACION_RECORRIDOS_FORM, self.BITACORA_RONDINES, self.BITACORA_FALLAS, self.BITACORA_INCIDENCIAS],
                'catalogs':[],
                'scripts':[self.SCRIPT_RONDINES, self.FALLAS, self.SCRIPT_INCIDENCIAS]
            },
            'activos':{
                'forms':[self.CONCESSIONED_ARTICULOS, self.BITACORA_OBJETOS_PERDIDOS],
                'catalogs':[self.ACTIVOS_FIJOS_CAT_ID, ],
                'scripts':[self.PAQUETERIA, self.GET_STATS, self.GAFETES_LOCKERS, self.FALLAS, self.ARTICULOS_PERDIDOS, self.ARTICULOS_CONSECIONADOS]
            },
            'notas':{
                'forms':[self.ACCESOS_NOTAS],
                'catalogs':[],
                'scripts':[self.NOTAS]
            },
            'pases_de_entrada':{
                'forms':[self.PASE_ENTRADA],
                'catalogs':[],
                'scripts':[self.SCRIPT_PASE_ACCESO, self.GET_STATS, self.SCRIPT_PASE_ACCESO_API]
            },
            'caseta':{
                'forms':[self.CHECKIN_CASETAS, self.REGISTRO_ASISTENCIA, self.FORMATO_VACACIONES],
                'catalogs':[],
                'scripts':[self.SCRIPT_TURNOS]
            },
        }

    def format_menus(self, data):
        """
        Formatea los datos de los registros obtenidos en el catalogo de ELEMENTOS MENU
        para obtener los menus.
        """
        if not data:
            return []

        f = self.menu_catalog_fields
        format_data = []
        for item in data:
            format_data.append({
                # Módulo
                "menu_key":           item.get(f['catalog_menu_key']),
                "menu":               item.get(f['catalog_menu']),
                "menu_order":         item.get(f['catalog_menu_order']),
                "menu_icon":          item.get(f['catalog_menu_icon']),
                "menu_columns":       item.get(f['catalog_menu_columns']),
                # Sección
                "seccion_key":        item.get(f['catalog_seccion_key']),
                "seccion":            item.get(f['catalog_seccion']),
                "seccion_order":      item.get(f['catalog_seccion_order']),
                "seccion_column":     item.get(f['catalog_seccion_column']),
                "seccion_href":       item.get(f['catalog_seccion_href']),
                "seccion_icon":       item.get(f['catalog_seccion_icon']),
                "seccion_icon_color": item.get(f['catalog_seccion_icon_color']),
                # Item
                "elemento":           item.get(f['catalog_elemento']),
                "key":                item.get(f['catalog_key']),
                "type":               item.get(f['catalog_type']),
                "item_order":         item.get(f['catalog_item_order']),
                "href_web":           item.get(f['catalog_href_web']),
                "route_mobile":       item.get(f['catalog_route_mobile']),
                "plataforms":         item.get(f['catalog_plataforms']),
            })
        return format_data

    def get_format_user_menus(self, filter_keys=None):
        """
        Obtiene los menus por default del catalogo de ELEMENTOS MENU.
        """
        selector = {}
        if filter_keys:
            selector = {f"answers.{self.menu_catalog_fields['catalog_key']}": {"$in": filter_keys}}

        mango_query = {
            "selector": selector,
            "limit": 10000,
        }
        data = self.format_menus(self.lkf_api.search_catalog( self.MENUS_CATALOG_ID, mango_query))
        return data

    def get_structured_mobile_menu(self, data):
        """
        Agrupa una lista plana de items de menú en la estructura
        jerárquica para móvil: módulo > submodulos > items.
        """
        modules_dict = {}

        for item in data:
            if item.get('plataforms') == 'web':
                continue

            menu_key    = item.get('menu_key') or self.slugify(item.get('menu', ''), '_')
            seccion_key = item.get('seccion_key') or self.slugify(item.get('seccion', ''), '_')

            if menu_key not in modules_dict:
                modules_dict[menu_key] = {
                    'id':         menu_key.replace('_', '-'),
                    'key':        menu_key,
                    'label':      item.get('menu', ''),
                    'icon':       item.get('menu_icon'),
                    'order':      item.get('menu_order') or len(modules_dict) + 1,
                    'submodules': {}
                }

            submodules = modules_dict[menu_key]['submodules']

            if seccion_key not in submodules:
                submodules[seccion_key] = {
                    'id':         seccion_key.replace('_', '-'),
                    'key':        seccion_key,
                    'label':      item.get('seccion', ''),
                    'order':      item.get('seccion_order') or len(submodules) + 1,
                    'icon':       item.get('seccion_icon'),
                    'iconBgColor': item.get('seccion_icon_color'),
                    'items':      {}
                }

            item_key = item.get('key') or self.slugify(item.get('elemento', ''), '_')
            if item_key not in submodules[seccion_key]['items']:
                item_data = {
                    'key':   item_key,
                    'label': item.get('elemento', ''),
                    'type':  item.get('type', 'link'),
                    'order': item.get('item_order') or len(submodules[seccion_key]['items']) + 1,
                }
                item_route = item.get('route_mobile')
                if item_route:
                    item_data['route'] = item_route
                submodules[seccion_key]['items'][item_key] = item_data

        modules = []
        for module in modules_dict.values():
            submodules = sorted(module['submodules'].values(), key=lambda s: s['order'])
            for s in submodules:
                s['items'] = sorted(s['items'].values(), key=lambda i: i['order'])
            modules.append({**module, 'submodules': submodules})

        return {'menu': sorted(modules, key=lambda m: m['order'])}

    def get_structured_web_menu(self, data):
        """
        Agrupa una lista plana de items de menú en la estructura
        jerárquica módulo > sección > items.
        """
        modules_dict = {}

        for item in data:
            if item.get('plataforms') in ['Mobile', 'mobile']:
                continue

            menu_key    = item.get('menu_key') or self.slugify(item.get('menu', ''), '_')
            seccion_key = item.get('seccion_key') or self.slugify(item.get('seccion', ''), '_')

            if menu_key not in modules_dict:
                modules_dict[menu_key] = {
                    'id':       menu_key.replace('_', '-'),
                    'key':      menu_key,
                    'label':    item.get('menu', ''),
                    'icon':     item.get('menu_icon'),
                    'order':    item.get('menu_order') or len(modules_dict) + 1,
                    'columns':  item.get('menu_columns'),
                    'sections': {}
                }

            sections = modules_dict[menu_key]['sections']

            if seccion_key not in sections:
                seccion_href = item.get('seccion_href')
                seccion_data = {
                    'id':     seccion_key.replace('_', '-'),
                    'key':    seccion_key,
                    'label':  item.get('seccion', ''),
                    'order':  item.get('seccion_order') or len(sections) + 1,
                    'column': item.get('seccion_column') or len(sections) + 1,
                    'items':  {}
                }
                if seccion_href:
                    seccion_data['href'] = seccion_href
                sections[seccion_key] = seccion_data

            item_href = item.get('href_web')
            item_key = item.get('key') or self.slugify(item.get('elemento', ''), '_')
            if item_key not in sections[seccion_key]['items']:
                item_data = {
                    'key':   item_key,
                    'label': item.get('elemento', ''),
                    'type':  item.get('type', 'link'),
                    'order': item.get('item_order') or len(sections[seccion_key]['items']) + 1,
                }
                if item_href:
                    item_data['href'] = item_href
                sections[seccion_key]['items'][item_key] = item_data

        modules = []
        for module in modules_dict.values():
            sections = sorted(module['sections'].values(), key=lambda s: s['order'])
            for s in sections:
                s['items'] = sorted(s['items'].values(), key=lambda i: i['order'])
            modules.append({**module, 'sections': sections})

        return {'modules': sorted(modules, key=lambda m: m['order'])}

    def get_user_menus(self, platform=''):
        """
        Obtiene los menus personalizados para el usuario 
        actual desde los registros de la forma de CONFIGURACION MENUS.
        """
        query = [
            {"$match": {
                "form_id": self.MENUS_FORM,
                "deleted_at": {"$exists": False},
                f"answers.{self.USUARIOS_OBJ_ID}.{self.menu_form_fields['usuario_id']}": self.user.get('user_id')
            }},
            {"$project": {
                "_id": 0,
                "elementos": f"$answers.{self.menu_form_fields['elementos']}"
            }},
            {"$unwind": "$elementos"},
            {"$project": {
                "menu_key": f"$elementos.{self.MENUS_CATALOG_OBJ_ID}.{self.menu_form_fields['key']}",
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query), ids_label_dct=self.menu_form_fields)
        if data:
            menu_keys = [self.unlist(item['menu_key']) for item in data if item.get('menu_key')]
            data = self.get_format_user_menus(filter_keys=menu_keys)
        else:
            data = self.get_format_user_menus()

        if platform == 'mobile':
            return self.get_structured_mobile_menu(data)

        modules = self.get_structured_web_menu(data)
        return modules

    def set_item_permits(self, user_id, item_needed, item_type):
        """
        Comparte los items necesarios para el usuario
        """
        permissions = 'can_read_item'
        share_data = {
            "owner": f"/api/infosync/user/{user_id}/",
            "perm": permissions
        }
        if item_type == 'form':
            user_item = self.lkf_api.get_user_forms(user_id)
        elif item_type == 'catalog':
            user_item = self.lkf_api.get_user_catalog(user_id)
        elif item_type == 'script':
            user_item = self.lkf_api.get_user_scripts(user_id)
        else:
            self.LKFException('Item type not found: ', item_type)
        res = {}
        user_item = user_item.get('data',[])
        if isinstance(user_item, dict):
            user_item = []
        user_item = [item for item in user_item ]
        unshare_items = []
        for item in user_item:
            if item['id'] not in item_needed:
                unshare_items.append(item)

        if unshare_items:
            unshare_items_data = []
            for item in unshare_items:
                unshare_data = {'group_id':None, 'filter_name':None}
                unshare_data['uri'] = f"/api/infosync/file_shared/{item['shared_id']}/"
                unshare_data['item_id'] = item['id']
                unshare_items_data.append(unshare_data)
            if unshare_items_data:
                res = self.lkf_api.share_form(unshare_items_data, unshare=True)

        user_item_ids = {item['id'] for item in user_item}
        items_to_share = item_needed - user_item_ids
        for item_id in items_to_share:
            share_data["file_shared"]=  f"/api/infosync/item/{item_id}/"
            res = self.lkf_api.share_form(share_data)
            if res['status_code'] != 201:
                self.LKFException(f'Error al compartir scritp: {share_data}')
        return res

    def set_user_permissions(self):
        """
        Comparte las Formas, Catalogos y Scripts necesarios para el usuario tomando en cuenta
        sus menus asignados.
        """
        data = self._labels(self.answers, ids_label_dct=self.menu_form_fields)
        user_id = data.get('usuario_id')
        if user_id and isinstance(user_id, list):
            user_id = user_id[0]
        permissions = 'can_read_item'
        share_data = {
            "owner": f"/api/infosync/user/{user_id}/",
            "perm": permissions
        }
        forms_needed = set()
        catalogs_needed = set()
        scripts_needed = set()
        menus = {i.get('menu', '').lower().replace(' ', '_') for i in data.get('elementos', [])}
        menus = ['always'] + list(menus)
        for menu in menus:
            config = self.module_permits.get(menu, {})
            if not config:
                continue
            forms_needed.update([x for x in config.get('forms',[]) if x])
            catalogs_needed.update([x for x in config.get('catalogs') if x])
            scripts_needed.update([x for x in config.get('scripts') if x])

        response_forms = self.set_item_permits(user_id, forms_needed, item_type='form')
        response_catalog = self.set_item_permits(user_id, catalogs_needed, item_type='catalog')
        response_scripts = self.set_item_permits(user_id, scripts_needed,  item_type='script')

        return True

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
    data_raw = json.loads(sys.argv[2])
    option = data.get("option", '')
    workflow_option = data_raw.get('option', '')
    platform = data.get("platform", '')

    dispatcher = {
        "get_menus": lambda: script_obj.get_user_menus(platform=platform),
        "set_permissions": lambda: script_obj.set_user_permissions(),
    }

    action = dispatcher.get(option) or dispatcher.get(workflow_option)
    if action:
        response = action()
    else:
        response = {"error": "Opción no válida"}

    # print(simplejson.dumps(response, indent=4))
    script_obj.HttpResponse({"data": response})