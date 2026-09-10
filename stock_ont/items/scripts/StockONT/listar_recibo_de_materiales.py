# -*- coding: utf-8 -*-
import sys
from recibo_de_materiales import Stock
from account_settings import *

# Mismos field_id hardcodeados que usa get_damage_reports() en stock_ont_utils.py
# (no viven en self.f porque tampoco viven ahi en el script que los crea).
FIELD_DAMAGE_QUANTITY = '6a8f105cf579313536b1984d'
FIELD_DAMAGE_NOTE = '6a8f10acae2709fa995fc6be'

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def _get_provider_name(self, answers):
        """
        Regresa el nombre del proveedor (almacen de origen) de un registro
        de Bitacora de Transportistas.

        Args:
            answers (dict): `answers` del registro (self.f/self.f_bitacora como llaves).

        Returns:
            str | None
        """
        origen = answers.get(self.f['obj_wh_locations']) or {}
        return self.unlist(origen.get(self.f['field_location_transportista']))

    def _get_delivery_date(self, answers):
        """
        Regresa solo la fecha (sin hora) de `fecha_hora_ingreso`.

        Args:
            answers (dict): `answers` del registro.

        Returns:
            str | None
        """
        delivery_date = self.unlist(answers.get(self.f_bitacora['fecha_hora_ingreso']))
        if delivery_date and ' ' in delivery_date:
            delivery_date = delivery_date.split(' ', 1)[0]
        return delivery_date

    def _get_expected_quantity_and_damage_count(self, answers):
        """
        Recorre el desglose de materiales (`grupo_desglose_empaque`) del
        registro y regresa la suma de `cantidad_desglose` (expectedQuantity)
        y el numero de materiales con un daño reportado (damageReports).

        Nota: get_damage_reports() (stock_ont_utils.py) ya combina todos los
        damageReports originales de un material en un solo dato agregado, por
        lo que aqui solo se puede contar materiales con daño, no reportes
        individuales.

        Args:
            answers (dict): `answers` del registro.

        Returns:
            tuple: (expected_quantity: int, damage_reports_count: int)
        """
        materiales_rows = answers.get(self.f_bitacora['grupo_desglose_empaque']) or []

        expected_quantity = 0
        damage_reports_count = 0
        for row_material in materiales_rows:
            expected_quantity += row_material.get(self.f_bitacora['cantidad_desglose']) or 0
            damage_reports_count += row_material.get(FIELD_DAMAGE_QUANTITY) or 0

        return expected_quantity, damage_reports_count

    def listar_recibo_materiales(self):
        """
        Consulta en mongo todos los registros de la forma de Bitacora de
        Transportistas (FORM_BITACORA_TRANSPORTISTA_ID) y regresa, por cada
        registro encontrado, un resumen con folio, stage, deliveryDate,
        providerName, expectedQuantity (suma de cantidades esperadas de sus
        materiales) y damageReports (cantidad de materiales con daño
        reportado).

        Returns:
            list[dict]: un elemento por registro encontrado.
        """
        registros = self.get_records(form_id=self.FORM_BITACORA_TRANSPORTISTA_ID, query_answers={
            f"{self.f['field_grp_stages']}.{self.f['field_grp_stages']}": {"$nin": ["revisión_final"]}
        })

        resultado = []
        for registro in registros:
            answers = registro.get('answers') or {}
            expected_quantity, damage_reports_count = self._get_expected_quantity_and_damage_count(answers)

            resultado.append({
                'folio': registro.get('folio'),
                'stage': self.unlist(answers.get(self.f['field_status_transferencia'])),
                'deliveryDate': self._get_delivery_date(answers),
                'providerName': self._get_provider_name(answers),
                'expectedQuantity': expected_quantity,
                'damageReports': damage_reports_count,
            })

        return resultado

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    resultado_lista = stock_obj.listar_recibo_materiales()
    print('+++ resultado_lista =', resultado_lista)

    if not stock_obj.current_record:
        stock_obj.HttpResponse({"data": resultado_lista})
