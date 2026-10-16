# -*- coding: utf-8 -*-
import sys, simplejson
from copy import deepcopy
from stock_ont_utils import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.data = self.data.get('data')
        
        self.map_stages = {
            'request': 'solicitud', 
            'approval': 'aprobación', 
            'preparation': 'en_preparación', 
            'reception': 'entregado', 
            'cancellation': 'cancelado',
        }

        self.map_transfer_stages = {
            'request_created': 'solicitud_creada',
            'transfer_requested': 'traspaso_solicitado',
            'transfer_authorized': 'traspaso_autorizado',
            'ready_for_delivery': 'por_entregar',
            'in_transit': 'en_tránsito',
            'stock_available': 'stock_disponible',
            'cancelled': 'cancelado',
        }

        # Almacen de cada rol de WH; los roles sin almacen no se restringen por almacen
        self.map_wh_role_location = {
            'wh_operator_puebla': 'PCI Puebla',
            'wh_operator_camarones': 'Camarones',
            'wh_manager_puebla': 'PCI Puebla',
            'wh_manager_camarones': 'Camarones',
        }

        # Fase de la tabla de permisos de SIGA a la que pertenece cada estatus
        self.map_stage_action = {
            'request_created': 'request',
            'transfer_requested': 'request',
            'transfer_authorized': 'approval',
            'ready_for_delivery': 'preparation',
            'in_transit': 'preparation',
            'stock_available': 'reception',
            'cancelled': 'cancellation',
        }

        # super_admin y national_admin pueden pasar a cualquier estatus
        self.transfer_full_access_roles = ('super_admin', 'national_admin')
        roles_wh = ('warehouse_coordinator', 'wh_operator_puebla', 'wh_operator_camarones')
        roles_mg = ('wh_manager_puebla', 'wh_manager_camarones')
        self.map_action_roles = {
            'request': roles_wh,
            'preparation': roles_wh,
            'approval': roles_mg,
            'reception': roles_mg,
            'cancellation': (),
        }

        self.user_authorized, self.user_in_group = self.authorized_transfer()
        print(f'--- authorized = {self.user_authorized} in_group = {self.user_in_group}')

    def authorized_transfer(self):
        is_admin, type_admin = self.is_user_admin()

        if is_admin or type_admin == 'no_user_found':
            return is_admin, type_admin

        for role, id_group in self.siga_wh_operator_groups + self.siga_wh_manager_groups:
            if self.current_user_id in self._get_users_group(id_group):
                return True, role

        return False, ''

    def operator_can_edit_location(self, location):
        """
        Valida que un rol de WH (operator/manager) solo opere Transferencias
        del almacen de su grupo. Los roles sin almacen pasan sin restriccion.

        Args:
            location (str): location del almacen (origen o destino) a validar.

        Returns:
            tuple: (bool autorizado, str location validada).
        """
        location_role = self.map_wh_role_location.get(self.user_in_group)
        if not location_role:
            return True, location

        return location == location_role, location

    def validate_transfer_permission(self, answers_record=None):
        """
        Valida que el rol del usuario pueda pasar la Transferencia al estatus
        enviado (`self.data['stage']`) y que sea del almacen correspondiente:
        destino para Recibir, origen para lo demas. Se valida el almacen del
        payload y, si viene `answers_record`, tambien el guardado en la BD.

        Args:
            answers_record (dict): answers del registro guardado, al actualizar.

        Returns:
            dict | None: respuesta 403 si no tiene permiso, None si pasa.
        """
        if self.user_in_group in self.transfer_full_access_roles:
            return None

        stage = self.data.get('stage')
        action = self.map_stage_action.get(stage)
        if not action or self.user_in_group not in self.map_action_roles[action]:
            return {
                'status_code': 403,
                'msg': f"El rol '{self.user_in_group}' no puede pasar Transferencias a '{stage}'"
            }

        if action == 'reception':
            obj_almacen = self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID
            field_location = self.stk.WH.f['warehouse_location_dest']
            locations = [self.data.get('destinationWarehouse')]
        else:
            obj_almacen = self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID
            field_location = self.stk.WH.f['warehouse_location']
            locations = [self.data.get('originWarehouse')]

        if answers_record is not None:
            almacen_guardado = answers_record.get(obj_almacen) or {}
            locations.insert(0, self.unlist(almacen_guardado.get(field_location)))

        for location in locations:
            can_edit_location, location = self.operator_can_edit_location(location)
            if not can_edit_location:
                return {
                    'status_code': 403,
                    'msg': f"Usuario no autorizado para operar Transferencias del almacen '{location}'"
                }

        return None

    def post_transferencia_materiales(self, answers):
        """
        Crea el registro en la forma de Transferencia de Materiales
        (form_id FORM_ID_TRANSFERENCIAS) con las respuestas ya armadas.

        Args:
            answers (dict): respuestas {field_id: valor} a guardar.

        Returns:
            dict: respuesta de `lkf_api.post_forms_answers`.
        """
        metadata = self.lkf_api.get_metadata(self.FORM_ID_TRANSFERENCIAS, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Transferencia de Materiales",
                    "action": "Crear Transferencia de Materiales",
                    "script": "transferencia_de_materiales.py",
                    "module": "stock_ont",
                    "function": "post_transferencia_materiales",
                }
            },
            'answers': answers,
        })
        resp_transferencia = self.lkf_api.post_forms_answers(metadata)
        if resp_transferencia.get('status_code') not in (200, 201):
            return resp_transferencia

        # Se asigna el registro creado a los grupos de Administracion de SIGA y a los
        # grupos de WH de los almacenes involucrados, para que puedan verlo y editarlo.
        self.assign_record_to_group(
            resp_transferencia.get('json', {}).get('id'),
            [self.id_group_super_admin, self.id_group_national_admin, self.id_group_wh_coordinator] + self.get_wh_groups_transfer()
        )

        return resp_transferencia

    def get_wh_groups_transfer(self):
        """
        Obtiene los grupos de WH Operator y WH Manager cuyo almacen es el
        origen o el destino de la Transferencia (segun map_wh_role_location).

        Returns:
            list[int]: ids de los grupos de WH a los que se asigna el registro.
        """
        locations = {self.data.get('originWarehouse'), self.data.get('destinationWarehouse')}
        return [
            id_group
            for role, id_group in self.siga_wh_operator_groups + self.siga_wh_manager_groups
            if self.map_wh_role_location.get(role) in locations
        ]

    def _build_stage_row(self, stage, name_stage, data_stage):
        stage_at = data_stage.get('cancelledAt' if stage == 'cancellation' else 'confirmedAt')
        stage_by = data_stage.get('cancelledBy' if stage == 'cancellation' else 'confirmedBy')
        reason = data_stage.get('reason')

        if not stage_at and not stage_by:
            return None

        return {
            self.f['field_stage_name'] : name_stage,
            self.f['field_stage_at'] : self.format_fecha_evento(stage_at),
            self.f['field_stage_by'] : stage_by,
            self.f['field_stage_canceled_reason'] : reason,
        }

    def build_grp_stages(self):
        data_grp_stages = []
        for stage, name_stage in self.map_stages.items():
            data_stage = self.data.get(stage, {})

            if not data_stage:
                continue

            row_stage = self._build_stage_row(stage, name_stage, data_stage)
            if row_stage:
                data_grp_stages.append(row_stage)
        return data_grp_stages

    def build_answers_transferencia(self):
        materiales_data = self.data.get('items', [])
        grp_materiales, grp_boxes, grp_pallets, grp_series, grp_missing = self.build_grp_materiales(materiales_data, is_transfer=True)

        # actualQuantity se calcula/consulta en el momento (front o consultar_
        # transferencia_de_materiales.py con get_stock=true) y se guarda tal
        # cual llegue, para poder leerlo despues sin recalcular el stock.
        for row_material, item in zip(grp_materiales, materiales_data):
            row_material[self.f['field_actual_quantity']] = item.get('actualQuantity')

        # print("\n+++ grp_materiales =",grp_materiales)
        # print("\n+++ grp_boxes =",grp_boxes)
        # print("\n+++ grp_pallets =",grp_pallets)
        # print("\n+++ grp_series =",grp_series)

        # print('... ... .... ... originWarehouse= ', self.find_warehouse_location_catalog(self.data.get('originWarehouse')))
        # print('... ... .... ... destinationWarehouse= ', self.find_warehouse_dest_catalog(self.data.get('destinationWarehouse')))
        # stop
        delivery_data = self.data.get('delivery', {})
        answers_transferencia = {
            self.f['field_status_transferencia']: self.map_transfer_stages.get(self.data.get('stage')),
            self.f['field_transfer_date_from']: self.data.get('analysisRange', {}).get('startDate'),
            self.f['field_transfer_date_to']: self.data.get('analysisRange', {}).get('endDate'),
            self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID: self.find_warehouse_location_catalog(self.data.get('originWarehouse')),
            self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID: self.find_warehouse_dest_catalog(self.data.get('destinationWarehouse')),
            self.bitacora_transportista_fields['grupo_desglose_empaque']: grp_materiales,
            self.f['field_grp_tarimas']: self.build_grp_tarimas(grp_pallets),
            self.f['field_grp_boxes']: grp_boxes,
            self.f['field_grp_onts']: grp_series,
            self.f['field_grp_bitacora']: self.build_grp_bitacora(self.data.get('events', [])),
            self.f['field_grp_stages']: self.build_grp_stages(),
            self.f['field_grp_missing_report']: grp_missing,

            self.f['field_delivery_estimated_date']: delivery_data.get('estimatedDeliveryDate'),
            self.f['field_delivery_signature']: delivery_data.get('signatureDataUrl',[]),
            self.f['field_delivery_signature_at']: self.format_fecha_evento(delivery_data.get('signedAt')),
            self.f['field_delivery_ev_transport']: delivery_data.get('evidence', {}).get('transport', {}).get('evidence', []),
            self.f['field_delivery_ev_plates']: delivery_data.get('evidence', {}).get('plates', {}).get('evidence', []),
            self.f['field_delivery_ev_ine']: delivery_data.get('evidence', {}).get('ine', {}).get('evidence', []),
            self.f['field_delivery_ev_license']: delivery_data.get('evidence', {}).get('license', {}).get('evidence', []),
            self.f['field_delivery_ev_material']: delivery_data.get('evidence', {}).get('material', {}).get('evidence', []),
        }

        info_catalog_transportista = self.find_transportista_catalog(delivery_data.get('carrierName'))
        if info_catalog_transportista:
            answers_transferencia[ self.f['obj_ubi_transportista'] ] = info_catalog_transportista

        # print('answers_transferencia =',simplejson.dumps(answers_transferencia, indent=4))

        return answers_transferencia

    def tranferencia_de_materiales(self):
        if not self.user_authorized:
            return {
                'status_code': 403,
                'msg': 'Usuario no autorizado para crear Transferencias de Materiales'
            }

        resp_permiso = self.validate_transfer_permission()
        if resp_permiso:
            return resp_permiso

        answers_transferencia = self.build_answers_transferencia()
        resp_transferencia = self.post_transferencia_materiales(answers_transferencia)
        # La firma externa ahora se crea en actualizar_transferencia_de_materiales.py con la bandera requestExternalSignature
        # self.create_record_firma_externa((resp_transferencia.get('json') or {}).get('folio'), 'transferencia', 'transferencia_de_materiales.py')
        return resp_transferencia

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_transferencia = stock_obj.tranferencia_de_materiales()
    stock_obj.HttpResponse({"data": resp_transferencia})