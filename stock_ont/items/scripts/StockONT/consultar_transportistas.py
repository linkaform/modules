# -*- coding: utf-8 -*-
import sys
from stock_ont_utils import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def get_nombres_transportistas(self):
        """
        Consulta el catalogo de Transportistas (CATALOG_ID_TRANSPORTISTAS = 164728)
        y regresa la lista de nombres encontrados en el campo Nombre del transportista.
        """
        records_transportistas = self.lkf_api.search_catalog(self.CATALOG_ID_TRANSPORTISTAS)
        return [
            self.unlist(record.get(self.f['field_nombre_transportista']))
            for record in records_transportistas
            if record.get(self.f['field_nombre_transportista'])
        ]

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    nombres_transportistas = stock_obj.get_nombres_transportistas()
    print('+++ nombres_transportistas =', nombres_transportistas)

    if not stock_obj.current_record:
        stock_obj.HttpResponse({"data": nombres_transportistas})
