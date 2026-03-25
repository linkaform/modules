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
                        self.LISTA_INCIDENCIAS_CAT_ID,
                        self.SUB_CATEGORIAS_INCIDENCIAS_ID,
                        self.CATEGORIAS_INCIDENCIAS_ID,
                        self.AREAS_DE_LAS_UBICACIONES_CAT_ID,
                        self.UBICACIONES_CAT_ID,
                        self.CONFIGURACION_RECORRIDOS_ID,
                        self.USUARIOS_ID,
                        self.CONF_AREA_EMPLEADOS_CAT_ID,
                        self.TIPO_DE_EQUIPO_ID,
                        self.LISTA_FALLAS_CAT_ID,
                        self.CONF_AREA_EMPLEADOS_AP_CAT_ID,
                        self.VISITA_AUTORIZADA_CAT_ID,
                        self.ESTADO_ID,
                        self.PROVEEDORES_CAT_ID,
                        self.LOCKERS_CAT_ID,
                        self.TIPO_ARTICULOS_PERDIDOS_CAT_ID,
                        self.PASE_ENTRADA_ID,
                        self.ACTIVOS_FIJOS_CAT_ID,
                ],
                'scripts':[self.OFFLINE_SERVICES]
            },
            'bitacoras':{
                'forms':[],
                'catalogs':[],
                'scripts':[]
            },
            'accesos':{
                'forms':[self.CHECKIN_CASETAS, self.REGISTRO_ASISTENCIA],
                'catalogs':[],
                'scripts':[]
            },
            'rondines':{
                'forms':[],
                'catalogs':[],
                'scripts':[self.SCRIPT_RONDINES]
            },
            'articulos':{
                'forms':[],
                'catalogs':[self.ACTIVOS_FIJOS_CAT],
                'scripts':[self.PAQUETERIA, self.GET_STATS, self.GAFETES_LOCKERS, self.FALLAS,self.ARTICULOS_PERDIDOS, self.ARTICULOS_CONSECIONADOS]
            },
            'incidencias':{
                'forms':[],
                'catalogs':[],
                'scripts':[]
            },
            'notas':{
                'forms':[],
                'catalogs':[],
                'scripts':[self.NOTAS]
            },
            'pases':{
                'forms':[],
                'catalogs':[],
                'scripts':[self.SCRIPT_PASE_ACCESO, self.GET_STATS]
            },
            'turnos':{
                'forms':[],
                'catalogs':[],
                'scripts':[]
            },
        }

    def set_config(self):
        data = self._labels(self.answers)
        user_id = data.get('id_usuario')
        if user_id and isinstance(user_id, list):
            user_id = user_id[0]
        print('user_id=', user_id)
        permissions = 'can_read_item'
        share_data = {
            "owner": f"/api/infosync/user/{user_id}/",
            "perm": permissions
        }
        for menu in data['menus']:
            forms_needed = self.module_permits.get(menu,{}).get('forms')
            catalogs_needed = self.module_permits.get(menu,{}).get('catalogs')
            scripts_needed = self.module_permits.get(menu,{}).get('scripts')
            user_scripts = self.lkf_api.get_user_scripts(user_id)
            print('user_scripts', user_scripts)
            user_scripts = user_scripts.get('data',[])
            unshare_scripts = [script_item for script_item in user_scripts if script_item['id'] not in scripts_needed]
            if unshare_scripts:
                unshare_data = {'group_id':None, 'filter_name':None}
                for script in unshare_scripts:
                    #TODO UN SHARE...
                    unshare_data['uri'] = f"/api/infosync/file_shared/{script['share_id']}/"
                    unshare_data['item_id'] = f"/api/infosync/file_shared/{script['id']}/"
                    res = self.lkf_api.share_form(unshare_data, unshare=True)
            for script_id in scripts_needed:
                share_data["file_shared"]=  f"/api/infosync/item/{script_id}/"
                res = self.lkf_api.share_form(share_data)
                print('script_id', script_id)
                if res['status_code'] != 201:
                    self.LKFException(f'Error al compartir scritp: {share_data}')
                print('res=',res)
        return True

    


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

