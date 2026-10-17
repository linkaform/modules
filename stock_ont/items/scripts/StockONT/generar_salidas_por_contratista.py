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

        # Solo estos roles pueden generar las salidas por contratista
        self.generar_salidas_roles = ('super_admin', 'national_admin')
        # Los asistentes administrativos solo pueden generarlas desde su almacen
        self.map_wh_admin_assistant_location = {
            'wh_admin_assistant_puebla': 'PCI Puebla',
            'wh_admin_assistant_camarones': 'Camarones',
        }
        self.user_authorized, self.user_in_group = self.authorized_generar_salidas()
        print(f'--- authorized = {self.user_authorized} in_group = {self.user_in_group}')

    def authorized_generar_salidas(self):
        is_admin, type_admin = self.is_user_admin()
        if is_admin and type_admin in self.generar_salidas_roles:
            return True, type_admin
        if type_admin == 'no_user_found':
            return False, type_admin

        # Mismo almacen origen que usara consultar_material_estimado() para calcular los vales
        wh_origen = self.material_estimado.set_source_warehouse_name(self.material_estimado.data.get('almacen_origen'))
        for role, id_group in self.siga_wh_admin_assistant_groups:
            if self.current_user_id in self._get_users_group(id_group):
                return wh_origen == self.map_wh_admin_assistant_location.get(role), role

        return False, type_admin

    def generar_id_salida(self):
        """
        Folio unico para el vale creado (se usa despues para localizarlo via
        actualizar_salida_de_materiales.py / consultar_salida_de_materiales.py).

        TODO: confirmar si el front espera un formato de `id` especifico
        (aqui solo se genera uno unico, con el mismo prefijo "SAL-" que se
        vio en el payload de ejemplo).
        """
        return f"SAL-{datetime.now().strftime('%Y-%m-%d')}-{time.time_ns()}"

    def post_firmas_externas(self, firmas_externas):
        """
        Crea de golpe los registros de Firmas de Usuarios Externos
        (FORM_FIRMAS_EXTERNAS) de todos los vales creados, en lugar de uno a
        uno como en salida_de_materiales().

        Args:
            firmas_externas (list[dict]): metadatas armadas con
                build_metadata_firma_externa().

        Returns:
            respuesta de `lkf_api.post_forms_answers_list`, o None si no hay
            registros por crear.
        """
        if not firmas_externas:
            return None
        return self.lkf_api.post_forms_answers_list(firmas_externas, jwt_settings_key='APIKEY_JWT_KEY')

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
        if not self.user_authorized:
            return {
                'status_code': 403,
                'msg': 'Usuario no autorizado para generar Salidas de Materiales por contratista'
            }

        vales =self.material_estimado.consultar_material_estimado()
        if not isinstance(vales, list):
            self.LKFException(f"No se pudo calcular el material estimado por contratista: {vales}")

        # TODO: por ahora solo crea 10 vales de prueba, para no saturar la BD de Salidas de Materiales
        # quito el limite pero lo dejo comentado para pruebas locales, ya que el script puede ser llamado desde el Front y generar muchos vales de golpe.
        vales = vales[:10]

        registros_creados = []
        firmas_externas = []
        for vale in vales:
            self.data = vale

            answers_salida = self.build_answers_salida()
            resp_salida = self.post_salida_materiales(answers_salida)

            if resp_salida.get('status_code') != 201:
                continue

            # La firma externa ahora se crea en actualizar_salida_de_materiales.py con la bandera requestExternalSignature
            # metadata_firma = self.build_metadata_firma_externa(resp_salida.get('json', {}).get('folio'), 'salida', 'generar_salidas_por_contratista.py')
            # if metadata_firma:
            #     firmas_externas.append(metadata_firma)

            # created_at viene como epoch (segundos); si no viene, se usa la hora actual
            created_at_ts = resp_salida.get('json', {}).get('created_at')
            created_at = datetime.fromtimestamp(created_at_ts).astimezone() if created_at_ts else datetime.now().astimezone()

            registros_creados.append({
                'folio': resp_salida.get('json', {}).get('folio'),
                'createdAt': self.format_created_at(created_at),
                'originWarehouse': vale.get('originWarehouse'),
                'recipient': vale.get('recipient') or {},
                'items': vale.get('items', []),
                'stage': 'created',
            })

        # self.post_firmas_externas(firmas_externas)

        return self.build_individual_and_grouped_response(registros_creados)

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_generar = stock_obj.generar_salidas_por_contratista()
    stock_obj.HttpResponse({"data": resp_generar})
