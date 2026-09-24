# coding: utf-8
#####
# Script para compartir el script de menus con el usuario del registro
# Forma: Usuarios
#####
import sys, simplejson, json

from base_utils import Base
from account_settings import *

class Base(Base):

    def __init__(self, settings, sys_argv=None, use_api=True):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

if __name__ == "__main__":
    base_obj = Base(settings, sys_argv=sys.argv)
    base_obj.console_run()
    userId = base_obj.answers.get(base_obj.menu_form_fields.get('usuario_id'))

    response = base_obj.share_menus_script(user_id=userId)
    status_code = response.get('status_code', 400)

    sys.stdout.write(simplejson.dumps({
        'status': status_code,
    }))
