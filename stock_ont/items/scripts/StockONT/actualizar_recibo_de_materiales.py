# -*- coding: utf-8 -*-
import sys
from recibo_de_materiales import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.inv_map_recibo_stages = {v: k for k, v in self.map_recibo_stages.items()}

        # Cada WH Operator solo puede editar Recepciones de su almacen destino
        self.map_wh_operator_location = {
            'wh_operator_puebla': 'PCI Puebla',
            'wh_operator_camarones': 'Camarones',
        }

        self.user_authorized, self.user_in_group = self.authorized_update_reception()
        print(f'--- authorized = {self.user_authorized} in_group = {self.user_in_group}')

    def authorized_update_reception(self):
        is_admin, type_admin = self.is_user_admin()

        if is_admin or type_admin == 'no_user_found':
            return is_admin, type_admin

        # validaciones especiales para editar la Recepcion
        # Solo pueden editar recepciones los Administradores y WH Operator
        for role, id_group in self.siga_wh_operator_groups:
            if self.current_user_id in self._get_users_group(id_group):
                return True, role
        # print('--- _users_by_group =', self._users_by_group)
        return False, ''

    def operator_can_edit_location(self, answers_record):
        """
        Valida que un WH Operator solo edite Recepciones cuyo almacen destino
        (field_location_almacen_destino) sea el de su grupo. Se compara contra
        lo guardado en el registro, no contra el payload, porque el operador no
        puede modificar `delivery`. Los administradores pasan sin restriccion.

        Args:
            answers_record (dict): answers del registro guardado en la BD.

        Returns:
            tuple: (bool autorizado, str location destino del registro).
        """
        almacen_destino = answers_record.get(self.f['obj_almacen_destino']) or {}
        location_destino = self.unlist(almacen_destino.get(self.f['field_location_almacen_destino']))

        location_operator = self.map_wh_operator_location.get(self.user_in_group)
        if not location_operator:
            return True, location_destino

        return location_destino == location_operator, location_destino

    def build_grp_stages_merge(self, existing_rows):
        """
        A diferencia de build_grp_stages() (que reconstruye las 5 fases desde
        cero a partir de self.data), aqui se conservan las fases ya guardadas
        en la forma (existing_rows, tal cual se leyeron de la BD) y solo se
        sobreescribe o agrega la(s) fase(s) que vengan en self.data en esta
        edicion. Asi, si solo llega 'finalReview', las demas que ya estaban
        guardadas no se pierden.

        Args:
            existing_rows (list[dict]): filas de field_grp_stages ya guardadas.

        Returns:
            list[dict]: filas para el campo field_grp_stages.
        """
        stages_by_key = {}
        for row_stage in existing_rows:
            stage_key = self.inv_map_recibo_stages.get(self.unlist(row_stage.get(self.f['field_stage_name'])))
            if stage_key:
                stages_by_key[stage_key] = row_stage

        for stage, name_stage in self.map_recibo_stages.items():
            data_stage = self.data.get(stage)
            if not data_stage:
                continue
            row_stage = self._build_stage_row(stage, name_stage, data_stage)
            if row_stage:
                stages_by_key[stage] = row_stage

        return [stages_by_key[stage] for stage in self.map_recibo_stages if stage in stages_by_key]

    def patch_bitacora_transportista(self, record_id, answers):
        """
        Actualiza el registro existente en la forma Bitacora de Transportistas
        (form_id FORM_BITACORA_TRANSPORTISTA_ID) con las respuestas ya armadas.

        Args:
            record_id: _id del registro a actualizar.
            answers (dict): respuestas {field_id: valor} a guardar.

        Returns:
            dict: respuesta de `lkf_api.patch_record`.
        """
        metadata = self.lkf_api.get_metadata(self.FORM_BITACORA_TRANSPORTISTA_ID, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Recibo de Materiales",
                    "action": "Actualizar Bitacora de Transportista",
                    "from_folio": self.folio,
                    "script": "actualizar_recibo_de_materiales.py",
                    "module": "stock_ont",
                    "function": "actualizar_recibo_materiales",
                }
            },
            'answers': answers,
        })
        return self.lkf_api.patch_record(metadata, record_id=record_id)

    def actualizar_recibo_materiales(self):
        """
        Recibe en self.data el `folio` del registro a editar (mas el resto del
        payload con la misma estructura que recibe_de_materiales()), y
        actualiza la Bitacora de Transportistas via patch_record. Almacenes,
        transportista, inspecciones, evidencias, materiales, firma, eventos
        y estatus se reemplazan por completo con lo que venga en self.data;
        las fases (materialDeclaration/damageReview/serialScanReview/
        finalReview/cancellation) se conservan y solo se agrega/actualiza
        la(s) que vengan en este payload (ver build_grp_stages_merge).

        Si la revision final ya fue confirmada (`self.data['finalReview']
        ['confirmedAt']`), ademas se genera la Recepcion de Materiales de
        Proveedor (ver `should_generate_recepcion_materiales`), salvo que la
        fase 'revisión_final' ya estuviera guardada en el registro: en ese
        caso la Recepcion ya se genero antes y no se duplica.
        """

        if not self.user_authorized:
            return {
                'status_code': 403,
                'msg': 'Usuario no autorizado para editar Recepciones'
            }

        folio = self.data.get('folio')
        if not folio:
            self.LKFException("No se recibio el folio del Recibo de Materiales a actualizar")

        folio_query = int(folio) if str(folio).isdigit() else folio
        record = self.get_record_by_folio(folio_query, self.FORM_BITACORA_TRANSPORTISTA_ID)
        if not record:
            self.LKFException(f"No se encontro ningun Recibo de Materiales con folio '{folio}'")

        answers_record = record.get('answers', {})

        can_edit_location, location_destino = self.operator_can_edit_location(answers_record)
        if not can_edit_location:
            return {
                'status_code': 403,
                'msg': f"Usuario no autorizado para editar Recepciones del almacen '{location_destino}'"
            }

        existing_stage_rows = answers_record.get(self.f['field_grp_stages']) or []

        # Si la revision final ya estaba guardada, la Recepcion ya se genero antes
        final_review_ya_guardada = any(
            self.unlist(row_stage.get(self.f['field_stage_name'])) == self.map_recibo_stages['finalReview']
            for row_stage in existing_stage_rows
        )

        can_update_delivery = self.user_in_group not in self.map_wh_operator_location
        answers_bitacora = self.build_answers_bitacora_transportista(can_update_delivery)
        answers_bitacora[ self.f['field_grp_stages'] ] = self.build_grp_stages_merge(existing_stage_rows)
        
        answers_record.update(answers_bitacora)

        # print(f'--- answers_record = {answers_record}')
        # stop

        resp_actualizar = self.patch_bitacora_transportista(record['_id'], answers_record)

        if resp_actualizar.get('status_code') == 202 and self.should_generate_recepcion_materiales():
            if final_review_ya_guardada:
                print('ADVERTENCIA: la revision final ya estaba confirmada, no se genera otra Recepcion')
            else:
                resp_actualizar['recepcion_materiales'] = self.create_record_recepcion_materiales_proveedor()

        return resp_actualizar

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_actualizar_recibo = stock_obj.actualizar_recibo_materiales()
    stock_obj.HttpResponse({"data": resp_actualizar_recibo})
