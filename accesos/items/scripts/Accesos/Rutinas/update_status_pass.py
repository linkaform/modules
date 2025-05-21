# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

sys.path.append('/srv/scripts/addons/modules/accesos/items/scripts/Accesos')
from accesos_utils import Accesos

class Accesos(Accesos):
    pass
    
if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    jwt = acceso_obj.lkf_api.get_jwt(user='seguridad@linkaform.com', api_key='58c62328de6b38d6d039122a9f0f7577f6f70ce2')
    settings.config['JWT_KEY'] = jwt
    acceso_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    acceso_obj.console_run()

    res = acceso_obj.update_pass_status()
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': acceso_obj.answers,
        'matched_documents': res
    }))
    
