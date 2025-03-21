# coding: utf-8
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from mantenimiento_utils import Mantenimiento

class Mantenimiento(Mantenimiento):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

if __name__ == "__main__":
    mantenimiento_obj = Mantenimiento(settings, sys_argv=sys.argv)
    mantenimiento_obj.console_run()

    