# -*- coding: utf-8 -*-
import sys
from stock_ont_utils import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def get_locations_proveedores(self):
        """
        Consulta el catalogo de WH Locations (CATALOG_ID_WH_LOCATIONS = 133014)
        filtrando por Warehouse Name = "Proveedores" y regresa la lista de
        valores encontrados en el campo Location.
        """
        mango_query = {
            "selector": {
                "answers": {
                    self.f['field_wh_name_transportista']: {"$eq": "Proveedores"},
                },
            },
            "limit": 10000,
            "skip": 0,
        }
        records_wh_locations = self.lkf_api.search_catalog(self.CATALOG_ID_WH_LOCATIONS, mango_query)
        return [
            self.unlist(record.get(self.f['field_location_transportista']))
            for record in records_wh_locations
            if record.get(self.f['field_location_transportista'])
        ]

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    locations_proveedores = stock_obj.get_locations_proveedores()
    print('+++ locations_proveedores =', locations_proveedores)

    if not stock_obj.current_record:
        stock_obj.HttpResponse({"data": locations_proveedores})
