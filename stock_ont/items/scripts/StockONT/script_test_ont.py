# -*- coding: utf-8 -*-
import sys, simplejson
from stock_ont_utils import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.numero = 1
        self.testing_stock_ont()

    def funcion_prueba(self):
        print('ya estoy usando el script de pruebas')

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    stock_obj.funcion_prueba()