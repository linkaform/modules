# coding: utf-8
import dis
import re
import unicodedata
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from base_utils import Base

class Base(Base):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

if __name__ == "__main__":
    script_obj = Base(settings, sys_argv=sys.argv)
    script_obj.console_run()
    data = script_obj.data.get('data', {})
    data_raw = json.loads(sys.argv[2])
    option = data.get("option", '')
    workflow_option = data_raw.get('option', '')
    platform = data.get("platform", '')
    dispatcher = {
        "get_menus": lambda: script_obj.get_user_menus(platform=platform),
        "set_permissions": lambda: script_obj.set_user_permissions(),
    }

    action = dispatcher.get(option) or dispatcher.get(workflow_option)
    if action:
        response = action()
    else:
        response = {"error": "Opción no válida"}

    # print(simplejson.dumps(response, indent=4))
    script_obj.HttpResponse({"data": response})