# -*- coding: utf-8 -*-
import sys, simplejson
from linkaform_api import settings
from account_settings import *


from lkf_addons.addons.base.app import CargaUniversal


if __name__ == '__main__':
    class_obj = CargaUniversal(settings=settings, sys_argv=sys.argv, use_api=True)
    class_obj.console_run()
    resp_cu = class_obj.carga_doctos_records(new_records, pos_field_dict, files_dir, nueva_ruta, id_forma_seleccionada, dict_catalogs, group_records )
