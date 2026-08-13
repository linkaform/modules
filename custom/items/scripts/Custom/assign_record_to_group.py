# -*- coding: utf-8 -*-
import sys, simplejson
from custom_utils import Custom
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def assign_record_to_group(self):
        usuario_asignado = self.answers.get('6a26fffffdf9dcdb9755f8b6', {}).get('69df18efff8ef34560975100')
        if not usuario_asignado:
            print("[ERROR] No se encontro usuario asignado en el campo")
            return

        # consultar los grupos para saber donde esta como supervisor

if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    lkf_obj.assign_record_to_group()