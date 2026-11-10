import sys, simplejson
from linkaform_api import settings
from account_settings import *

from servicios_utils import ServicioMaquinariaPesada

if __name__ == '__main__':
    lkf_obj = ServicioMaquinariaPesada(settings, sys_argv=sys.argv)
    lkf_obj.add_fecha_field()
