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

    def set_config(self):
        """
        Ya no comparte permisos directamente (eso ahora vive solo en la forma
        CONFIGURACION_MENUS, via set_user_permissions/apply_user_menu_permissions
        en lkf_addons/addons/base/app.py). En su lugar, crea o actualiza el
        registro equivalente en CONFIGURACION_MENUS siguiendo el mismo mapeo que
        migrate_legacy_menus.py, y es el workflow nativo de esa forma el que
        dispara el compartir de permisos.
        """
        data = self._labels(self.answers)
        user_id = data.get('id_usuario')
        if user_id and isinstance(user_id, list):
            user_id = user_id[0]
        username = self.answers.get(self.EMPLOYEE_OBJ_ID, {}).get(self.mf['username'])
        menus = data.get('menus', [])

        new_menus = []
        for menu in menus:
            new_menus.extend(self.PERMISSION_MODULE_MAP.get(menu, []))
        details_menus = self.get_format_user_menus(filter_keys=new_menus)

        clear_menus = []
        for menu in details_menus:
            clear_menus.append({
                f"{self.MENUS_CATALOG_OBJ_ID}": {
                    self.menu_form_fields['menu']: menu['menu'],
                    self.menu_form_fields['seccion']: menu['seccion'],
                    self.menu_form_fields['elemento']: menu['elemento'],
                    self.menu_form_fields['key']: [menu['key']],
                    self.menu_form_fields['plataforms']: [menu['plataforms']],
                }
            })

        answers = {
            self.USUARIOS_OBJ_ID: {
                self.mf['username']: username,
                self.mf['id_usuario']: [user_id],
            },
            self.menu_form_fields['elementos']: clear_menus,
        }

        existing_id = self._existing_menu_record_id(user_id)
        metadata = self.lkf_api.get_metadata(form_id=self.MENUS_FORM)
        metadata.update({'answers': answers})
        if existing_id:
            metadata['_id'] = existing_id
            res = self.net.patch_forms_answers(metadata)
        else:
            metadata.pop('_id', None)
            res = self.lkf_api.post_forms_answers(metadata)
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

