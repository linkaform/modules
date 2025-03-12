# coding: utf-8
#####
# Script para crear un usuario
# Forma: Usuarios
#####
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from base_utils import Base

class Base(Base):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

if __name__ == "__main__":
    base_obj = Base(settings, sys_argv=sys.argv)
    base_obj.console_run()
    user_data = base_obj.answers

    response = base_obj.create_user_account(user_data=user_data)

    sys.stdout.write(simplejson.dumps({
        'response': response
    }))