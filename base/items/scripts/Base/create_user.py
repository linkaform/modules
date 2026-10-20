# coding: utf-8
#####
# Script para crear un usuario
# Forma: Usuarios
#####
import sys, simplejson, json

from base_utils import Base
from account_settings import *


class Base(Base):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

if __name__ == "__main__":
    base_obj = Base(settings, sys_argv=sys.argv)
    base_obj.console_run()
    user_data = base_obj.answers

    # Dos fases con el mismo script (workflows del form Usuarios):
    #  - 'crear'  (runtime before): da de alta el usuario y deja el registro en 'creado'.
    #  - 'activado' (runtime after, cuando el usuario fija su contraseña y
    #    set_user_password en Sanic pasa el registro a 'activado'): crea/liga el
    #    empleado y su Configuracion Accesos.
    if user_data.get(base_obj.f['new_user_status']) == 'activado':
        base_obj.setup_user_employee(user_data)
        sys.stdout.write(simplejson.dumps({'status': 200}))
    else:
        response = base_obj.create_user_account(user_data=user_data)

        sys.stdout.write(simplejson.dumps({
            'status': 101,
            'replace_ans': base_obj.answers,
            'metadata': {
                'id': base_obj.record_id
            }
        }))