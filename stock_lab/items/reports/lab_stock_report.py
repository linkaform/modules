# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date

from lkf_addons.addons.stock_greenhouse.stock_reports import Reports





sys.path.append('../')
from scripts.Lab.lab_stock_utils import Stock

from account_settings import *

class Reports(Reports):

    def sart(self):
        print('start')

    # def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
    #     print('folio_solicitud', folio_solicitud)
    #     super().__init__(settings, folio_solicitud=folio_solicitud, sys_argv=sys_argv, use_api=use_api)
    #     print('esteeeeee......')


