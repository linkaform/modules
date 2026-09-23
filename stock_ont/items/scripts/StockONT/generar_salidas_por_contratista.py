# -*- coding: utf-8 -*-
import sys, time
from datetime import datetime
from salida_de_materiales import Stock
from consultar_material_estimado import Stock as MaterialEstimadoStock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        # Instancia aparte (mismo patron que self.accs/self.stk en
        # stock_ont_utils.py) para reusar el calculo de material estimado sin
        # heredar dos veces de stock_ont_utils.Stock.
        self.material_estimado = MaterialEstimadoStock(self.settings, sys_argv=self.sys_argv, use_api=self.use_api)
        # Este script siempre necesita el reporte agrupado por contratista,
        # sin depender de que el payload de entrada lo haya mandado.
        self.material_estimado.data['tipo_reporte'] = 'vale_por_contratista'

    def generar_id_salida(self):
        """
        Folio unico para el vale creado (se usa despues para localizarlo via
        actualizar_salida_de_materiales.py / consultar_salida_de_materiales.py).

        TODO: confirmar si el front espera un formato de `id` especifico
        (aqui solo se genera uno unico, con el mismo prefijo "SAL-" que se
        vio en el payload de ejemplo).
        """
        return f"SAL-{datetime.now().strftime('%Y-%m-%d')}-{time.time_ns()}"

    def generar_salidas_por_contratista(self):
        """
        Calcula el material estimado agrupado por contratista (tipo_reporte
        'vale_por_contratista', ver build_vale_por_contratista() en
        consultar_material_estimado.py) y crea un registro de Salida de
        Material (FORM_ID_SALIDAS) por cada vale generado, reutilizando
        build_answers_salida()/post_salida_materiales() de
        salida_de_materiales.py.

        Solo se toman en cuenta los vales cuya creacion respondio
        status_code == 201; el resto se descarta tanto de `individual` como
        de `grouped_by_sku` (ver build_individual_and_grouped_response()).

        Returns:
            dict: {individual: [...], grouped_by_sku: [...]}, formato que
            espera el Front.
        """
        vales = self.material_estimado.consultar_material_estimado()
        if not isinstance(vales, list):
            self.LKFException(f"No se pudo calcular el material estimado por contratista: {vales}")

        # TODO: por ahora solo crea 3 vales de prueba, para no saturar la BD de Salidas de Materiales
        # quito el limite pero lo dejo comentado para pruebas locales, ya que el script puede ser llamado desde el Front y generar muchos vales de golpe.
        # vales = vales[:3]

        registros_creados = []
        for vale in vales:
            self.data = vale

            answers_salida = self.build_answers_salida()
            resp_salida = self.post_salida_materiales(answers_salida)

            if resp_salida.get('status_code') != 201:
                continue

            registros_creados.append({
                'folio': resp_salida.get('json', {}).get('folio'),
                'originWarehouse': vale.get('originWarehouse'),
                'recipient': vale.get('recipient') or {},
                'items': vale.get('items', []),
                'stage': 'created',
            })

        return self.build_individual_and_grouped_response(registros_creados)

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_generar = stock_obj.generar_salidas_por_contratista()
    stock_obj.HttpResponse({"data": resp_generar})
