# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

print('Arranca JIT - Report en modulo JIT....')
from lkf_addons.addons.jit.report import Reports

#Se agrega path para que obtenga el archivo de Stock de este modulo
# sys.path.append('/srv/scripts/addons/modules/JIT/items/scripts/JIT')

today = date.today()
year_week = int(today.strftime('%Y%W'))


class Reports(Reports):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        #base.LKF_Base.__init__(self, settings, sys_argv=sys_argv, use_api=use_api)
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
