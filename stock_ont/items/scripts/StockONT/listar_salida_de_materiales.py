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
        registros_db = list(self.get_records(form_id=self.FORM_ID_SALIDAS, query_answers={
            f"answers.{self.f['field_status_transferencia']}": {"$ne": "entregado"}
        }))

        # Se recorren los renglones de todos los registros una vez para consultar
        # el catalogo de productos (nsNeed) en una sola peticion y tomar el
        # product_code guardado en el registro, en vez de buscarlo por sku.
        product_code_by_sku = {}
        for registro_db in registros_db:
            answers = registro_db.get('answers') or {}
            for row_material in answers.get(self.bitacora_transportista_fields['grupo_desglose_empaque']) or []:
                producto = row_material.get(self.f['obj_products']) or {}
                sku = self.unlist(producto.get(self.f['field_sku']))
                if sku and not product_code_by_sku.get(sku):
                    product_code_by_sku[sku] = self.unlist(producto.get(self.f['field_product_code']))

        ns_need_by_sku = self._get_ns_need_by_sku(product_code_by_sku.keys())

        registros = []
        for registro_db in registros_db:
            answers = registro_db.get('answers') or {}
            stage = self.inv_map_salida_stages.get(self.unlist(answers.get(self.f['field_status_transferencia'])))

            registros.append({
                'folio': registro_db.get('folio'),
                'createdAt': self.format_created_at(registro_db.get('created_at')),
                'originWarehouse': self._get_origin_warehouse(answers),
                'recipient': self._unbuild_recipient(answers),
                'items': self._unbuild_items(answers, ns_need_by_sku),
                'stage': stage,
            })

        return self.build_individual_and_grouped_response(registros, product_code_by_sku)

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    resultado_lista = stock_obj.listar_salida_materiales()
    # print('+++ resultado_lista =', resultado_lista)

    if not stock_obj.current_record:
        stock_obj.HttpResponse({"data": resultado_lista})
