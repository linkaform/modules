# -*- coding: utf-8 -*-
import sys
from consultar_salida_de_materiales import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def _get_origin_warehouse(self, answers):
        """
        Regresa el almacen de origen de una Salida de Material.

        Args:
            answers (dict): `answers` del registro.

        Returns:
            str | None
        """
        origen = answers.get(self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID) or {}
        return self.unlist(origen.get(self.stk.WH.f['warehouse_location']))

    def listar_salida_materiales(self):
        """
        Consulta en mongo todos los registros de la forma de Salida de
        Material (FORM_ID_SALIDAS) cuyo stage sea distinto de "entregado"
        (delivered), y regresa el mismo formato {individual, grouped_by_sku}
        que generar_salidas_por_contratista.py (ver
        build_individual_and_grouped_response() en salida_de_materiales.py).

        Returns:
            dict: {individual: [...], grouped_by_sku: [...]}.
        """
        registros_db = self.get_records(form_id=self.FORM_ID_SALIDAS, query_answers={
            self.f['field_status_transferencia']: {"$ne": "entregado"}
        })

        registros = []
        for registro_db in registros_db:
            answers = registro_db.get('answers') or {}
            stage = self.inv_map_salida_stages.get(self.unlist(answers.get(self.f['field_status_transferencia'])))

            registros.append({
                'folio': registro_db.get('folio'),
                'originWarehouse': self._get_origin_warehouse(answers),
                'recipient': self._unbuild_recipient(answers),
                'items': self._unbuild_items(answers),
                'stage': stage,
            })

        return self.build_individual_and_grouped_response(registros)

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    resultado_lista = stock_obj.listar_salida_materiales()
    # print('+++ resultado_lista =', resultado_lista)

    if not stock_obj.current_record:
        stock_obj.HttpResponse({"data": resultado_lista})
