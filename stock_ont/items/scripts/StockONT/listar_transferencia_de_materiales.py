# -*- coding: utf-8 -*-
import sys
from transferencia_de_materiales import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.inv_map_transfer_stages = {v: k for k, v in self.map_transfer_stages.items()}

    def _get_origin_warehouse(self, answers):
        """
        Regresa el almacen de origen de una Transferencia de Materiales.

        Args:
            answers (dict): `answers` del registro.

        Returns:
            str | None
        """
        origen = answers.get(self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID) or {}
        return self.unlist(origen.get(self.stk.WH.f['warehouse_location']))

    def _get_destination_warehouse(self, answers):
        """
        Regresa el almacen de destino de una Transferencia de Materiales.

        Args:
            answers (dict): `answers` del registro.

        Returns:
            str | None
        """
        destino = answers.get(self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID) or {}
        return self.unlist(destino.get(self.stk.WH.f['warehouse_location_dest']))

    def listar_transferencia_materiales(self):
        """
        Consulta en mongo todos los registros de la forma de Transferencia de
        Materiales (FORM_ID_TRANSFERENCIAS) y regresa, por cada registro
        encontrado, un resumen con folio, originWarehouse,
        destinationWarehouse, items (cantidad de materiales en el desglose),
        stage, startDate y endDate.

        Returns:
            list[dict]: un elemento por registro encontrado.
        """
        registros = self.get_records(form_id=self.FORM_ID_TRANSFERENCIAS)

        resultado = []
        for registro in registros:
            answers = registro.get('answers') or {}
            materiales_rows = answers.get(self.bitacora_transportista_fields['grupo_desglose_empaque']) or []

            resultado.append({
                'folio': registro.get('folio'),
                'originWarehouse': self._get_origin_warehouse(answers),
                'destinationWarehouse': self._get_destination_warehouse(answers),
                'items': len(materiales_rows),
                'stage': self.inv_map_transfer_stages.get(self.unlist(answers.get(self.f['field_status_transferencia']))),
                'startDate': self.unlist(answers.get(self.f['field_transfer_date_from'])),
                'endDate': self.unlist(answers.get(self.f['field_transfer_date_to'])),
            })

        return resultado

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    resultado_lista = stock_obj.listar_transferencia_materiales()
    print('+++ resultado_lista =', resultado_lista)

    if not stock_obj.current_record:
        stock_obj.HttpResponse({"data": resultado_lista})
