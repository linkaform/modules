# coding: utf-8
import sys, simplejson

from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):


    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
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

        #TODO SHARE FORMS AND CATALOGGS
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
                    ],
                'scripts':[self.OFFLINE_SERVICES]
            },
            'bitacoras':{
                'forms':[self.BITACORA_ACCESOS],
                'catalogs':[],
                'scripts':[]
            },
            'accesos':{
                'forms':[self.CHECKIN_CASETAS, self.REGISTRO_ASISTENCIA, self.BITACORA_GAFETES_LOCKERS, self.CHECK_UBICACIONES],
                'catalogs':[],
                'scripts':[]
            },
            'rondines':{
                'forms':[self.CONFIGURACION_RECORRIDOS_FORM, self.BITACORA_RONDINES],
                'catalogs':[],
                'scripts':[self.SCRIPT_RONDINES]
            },
            'articulos':{
                'forms':[self.CONCESSIONED_ARTICULOS, self.BITACORA_OBJETOS_PERDIDOS, self.PAQUETERIA],
                'catalogs':[self.ACTIVOS_FIJOS_CAT_ID, ],
                'scripts':[self.PAQUETERIA, self.GET_STATS, self.GAFETES_LOCKERS, self.FALLAS, self.ARTICULOS_PERDIDOS, self.ARTICULOS_CONSECIONADOS]
            },
            'incidencias':{
                'forms':[self.BITACORA_FALLAS, self.BITACORA_INCIDENCIAS],
                'catalogs':[],
                'scripts':[self.FALLAS]
            },
            'notas':{
                'forms':[self.ACCESOS_NOTAS],
                'catalogs':[],
                'scripts':[self.NOTAS]
            },
            'pases':{
                'forms':[self.PASE_ENTRADA],
                'catalogs':[],
                'scripts':[self.SCRIPT_PASE_ACCESO, self.GET_STATS, self.SCRIPT_PASE_ACCESO_API]
            },
            'turnos':{
                'forms':[self.CHECKIN_CASETAS, self.REGISTRO_ASISTENCIA, self.FORMATO_VACACIONES,self.SCRIPT_TURNOS],
                'catalogs':[],
                'scripts':[]
            },
        }




    def set_config(self):
        data = self._labels(self.answers)
        user_id = data.get('id_usuario')
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
        menus = ['always'] + data.get('menus', [])
        for menu in menus:
            config = self.module_permits.get(menu, {})
            if not config:
                continue
            forms_needed.update(config.get('forms',[]))
            catalogs_needed.update(config.get('catalogs'))
            scripts_needed.update(config.get('scripts'))

        response_forms = self.set_item_permits(user_id, forms_needed, item_type='form')
        response_catalog = self.set_item_permits(user_id, catalogs_needed, item_type='catalog')
        response_scripts = self.set_item_permits(user_id, scripts_needed,  item_type='script')
            
        return True

    def set_item_permits(self, user_id, item_needed, item_type):
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
                #TODO UN SHARE...
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


if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    data = acceso_obj.data.get('data',{})
    option = data.get("option",'')
    print('option', option)
    if option == 'get_user_menu':
        response = acceso_obj.get_config_accesos()
    elif option == 'set_config' or True:
        response = acceso_obj.set_config()

