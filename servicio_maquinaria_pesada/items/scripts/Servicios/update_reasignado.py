import sys, simplejson
from linkaform_api import settings
from account_settings import *

from servicios_utils import ServicioMaquinariaPesada

if __name__ == '__main__':
    lkf_obj = ServicioMaquinariaPesada(settings, sys_argv=sys.argv, use_api=True)
    lkf_obj.update_reasignado()
