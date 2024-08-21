# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from app import ModuleName

class ModuleName(ModuleName):
    pass


if __name__ == "__main__":
    module_obj = ModuleName(settings, sys_argv=sys.argv)
    module_obj.console_run()
    data = module_obj.data.get('data',{})
    option = data.get("option",'')