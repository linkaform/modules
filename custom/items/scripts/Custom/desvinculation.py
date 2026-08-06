# -*- coding: utf-8 -*-
import sys, simplejson
from custom_utils import Custom
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def get_usernames_ids(self):
        """
        Consulta los usuarios de la cuenta y devuelve un mapeo del nombre de usuario y id
        """
        all_users = self.lkf_api.get_all_users()

        return {
            u['username']: u['id'] 
            for u in all_users
            if u.get('username') and u.get('id')
        }

    def desvincular_usuario(self):
        """
        Desvincula el dispositivo de un usuario
        """
        map_user_id = self.get_usernames_ids()
        # print('map_user_id =',map_user_id)

        # usuario que se va a desvincular
        usuario = self.answers.get('000000000000000000000001')
        print('\nDesvinculando usuario =',usuario)

        user_id = map_user_id.get(usuario)
        if not user_id:
            print('[ERROR] Usuario no encontrado. Favor de revisar que exista')
            return

        # Ejecutando la Desvinculacion del dispositivo
        print('ID del usuario =',user_id)
        resp_desvinculation = self.lkf_api.unlink_device(user_id)
        print('\n\nresp_desvinculation =',resp_desvinculation)

if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()
    lkf_obj.desvincular_usuario()