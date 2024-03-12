# -*- coding: utf-8 -*-
import sys, simplejson, math

from linkaform_api import settings
from account_settings import *

from lkf_addons.addons.stock_greenhouse.stock_reports import Reports

class Reports(Reports):
    "Inherit stock reports"
