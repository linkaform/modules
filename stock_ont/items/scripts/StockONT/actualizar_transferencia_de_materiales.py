# -*- coding: utf-8 -*-
import sys
from transferencia_de_materiales import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.inv_map_stages = {v: k for k, v in self.map_stages.items()}

    def patch_transferencia_materiales(self, record_id, answers):
        """
        Actualiza el registro existente en la forma de Transferencia de
        Materiales (form_id FORM_ID_TRANSFERENCIAS) con las respuestas ya armadas.

        Args:
            record_id: _id del registro a actualizar.
            answers (dict): respuestas {field_id: valor} a guardar.

        Returns:
            dict: respuesta de `lkf_api.patch_record`.
        """
        metadata = self.lkf_api.get_metadata(self.FORM_ID_TRANSFERENCIAS, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Transferencia de Materiales",
                    "action": "Actualizar Transferencia de Materiales",
                    "script": "actualizar_transferencia_de_materiales.py",
                    "module": "stock_ont",
                    "function": "actualizar_transferencia_materiales",
                }
            },
            'answers': answers,
        })
        return self.lkf_api.patch_record(metadata, record_id=record_id)

    def build_grp_stages_merge(self, existing_rows):
        """
        A diferencia de build_grp_stages() (que reconstruye las 5 fases desde
        cero a partir de self.data), aqui se conservan las fases ya guardadas
        en la forma (existing_rows, tal cual se leyeron de la BD) y solo se
        sobreescribe o agrega la(s) fase(s) que vengan en self.data en esta
        edicion. Asi, si solo llega 'approval', 'request' que ya estaba
        guardado no se pierde.

        Args:
            existing_rows (list[dict]): filas de field_grp_stages ya guardadas.

        Returns:
            list[dict]: filas para el campo field_grp_stages.
        """
        stages_by_key = {}
        for row_stage in existing_rows:
            stage_key = self.inv_map_stages.get(self.unlist(row_stage.get(self.f['field_stage_name'])))
            if stage_key:
                stages_by_key[stage_key] = row_stage

        for stage, name_stage in self.map_stages.items():
            data_stage = self.data.get(stage)
            if not data_stage:
                continue
            row_stage = self._build_stage_row(stage, name_stage, data_stage)
            if row_stage:
                stages_by_key[stage] = row_stage

        return [stages_by_key[stage] for stage in self.map_stages if stage in stages_by_key]

    def actualizar_transferencia_materiales(self):
        """
        Recibe en self.data el `folio` del registro a editar (mas el resto del
        payload con la misma estructura que tranferencia_de_materiales()),
        y actualiza la forma via patch_record. `items`, `delivery`, `events`,
        `stage` y el almacen origen/destino se reemplazan por completo con lo
        que venga en self.data; las fases (request/approval/preparation/
        reception/cancellation) se conservan y solo se agrega/actualiza la(s)
        que vengan en este payload (ver build_grp_stages_merge).
        """
        folio = self.data.get('folio')
        if not folio:
            self.LKFException("No se recibio el folio de la Transferencia de Materiales a actualizar")

        folio_query = int(folio) if str(folio).isdigit() else folio
        record = self.get_record_by_folio(folio_query, self.FORM_ID_TRANSFERENCIAS)
        if not record:
            self.LKFException(f"No se encontro ninguna Transferencia de Materiales con folio '{folio}'")

        existing_stage_rows = record.get('answers', {}).get(self.f['field_grp_stages']) or []

        answers_transferencia = self.build_answers_transferencia()
        answers_transferencia[ self.f['field_grp_stages'] ] = self.build_grp_stages_merge(existing_stage_rows)

        return self.patch_transferencia_materiales(record['_id'], answers_transferencia)

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_actualizar_transferencia = stock_obj.actualizar_transferencia_materiales()
    stock_obj.HttpResponse({"data": resp_actualizar_transferencia})
