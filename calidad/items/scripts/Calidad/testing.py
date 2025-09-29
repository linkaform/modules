# coding: utf-8
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from calidad_utils import Calidad

class Calidad(Calidad):
    pass

if __name__ == "__main__":
    script_obj = Calidad(settings, sys_argv=sys.argv)
    script_obj.console_run()