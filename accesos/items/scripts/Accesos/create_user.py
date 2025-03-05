# coding: utf-8
#####
# Script para crear un usuario
# Forma: Usuarios
#####
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.load(module='Base', **self.kwargs)

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    user_data = acceso_obj.answers

    response = acceso_obj.Base.create_user_account(user_data=user_data)

    sys.stdout.write(simplejson.dumps({
        'response': response
    }))