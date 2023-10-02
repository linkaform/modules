# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime

from linkaform_api import settings, network, utils

from account_settings import *
#from account_utils import get_plant_recipe, select_S4_recipe, set_lot_ready_week
#from stock_utils import *
from lkf_addons.addons.stock.stock_utils import Stock


# lkf = self.lkf.LKFModules()

if __name__ == '__main__':
    print(sys.argv)
    stock_obj = Stock(settings, sys_argv=sys.argv)
    folio = stock_obj.current_record.get('folio')
    stock_obj.inventory_adjustment(folio, stock_obj.current_record)
