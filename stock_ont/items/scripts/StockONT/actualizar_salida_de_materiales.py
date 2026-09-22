# -*- coding: utf-8 -*-
import sys
from salida_de_materiales import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.inv_map_salida_phases = {v: k for k, v in self.map_salida_phases.items()}

    def patch_salida_materiales(self, record_id, answers):
        """
        Actualiza el registro existente en la forma de Salida de Material
        (form_id FORM_ID_SALIDAS) con las respuestas ya armadas.

        Args:
            record_id: _id del registro a actualizar.
            answers (dict): respuestas {field_id: valor} a guardar.

        Returns:
            dict: respuesta de `lkf_api.patch_record`.
        """
        metadata = self.lkf_api.get_metadata(self.FORM_ID_SALIDAS, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Salida de Material",
                    "action": "Actualizar Salida de Material",
                    "script": "actualizar_salida_de_materiales.py",
                    "module": "stock_ont",
                    "function": "actualizar_salida_materiales",
                }
            },
            'answers': answers,
        })
        return self.lkf_api.patch_record(metadata, record_id=record_id)

    def build_grp_stages_merge(self, existing_rows):
        """
        A diferencia de build_grp_stages() (que reconstruye calculation/
        authorization/preparation desde cero a partir de self.data), aqui se
        conservan las fases ya guardadas en la forma (existing_rows, tal cual
        se leyeron de la BD) y solo se sobreescribe o agrega la(s) fase(s)
        que vengan en self.data en esta edicion.

        Args:
            existing_rows (list[dict]): filas de field_grp_stages ya guardadas.

        Returns:
            list[dict]: filas para el campo field_grp_stages.
        """
        phases_by_key = {}
        for row_stage in existing_rows:
            phase_key = self.inv_map_salida_phases.get(self.unlist(row_stage.get(self.f['field_stage_name'])))
            if phase_key:
                phases_by_key[phase_key] = row_stage

        for stage, name_stage in self.map_salida_phases.items():
            data_stage = self.data.get(stage)
            if not data_stage:
                continue
            row_stage = self._build_stage_row(stage, name_stage, data_stage)
            if row_stage:
                phases_by_key[stage] = row_stage

        return [phases_by_key[stage] for stage in self.map_salida_phases if stage in phases_by_key]

    def actualizar_salida_materiales(self):
        """
        Recibe en self.data el `id` (folio) del registro a editar (mas el
        resto del payload con la misma estructura que salida_de_materiales()),
        y actualiza la forma via patch_record. `items`, `delivery`, `events`,
        `stage` y el almacen origen se reemplazan por completo con lo que
        venga en self.data; las fases (calculation/authorization/preparation)
        se conservan y solo se agrega/actualiza la(s) que vengan en este
        payload (ver build_grp_stages_merge).
        """
        folio = self.data.get('id')
        if not folio:
            self.LKFException("No se recibio el id/folio de la Salida de Material a actualizar")

        folio_query = int(folio) if str(folio).isdigit() else folio
        record = self.get_record_by_folio(folio_query, self.FORM_ID_SALIDAS)
        if not record:
            self.LKFException(f"No se encontro ninguna Salida de Material con folio '{folio}'")

        existing_stage_rows = record.get('answers', {}).get(self.f['field_grp_stages']) or []

        answers_salida = self.build_answers_salida()
        answers_salida[ self.f['field_grp_stages'] ] = self.build_grp_stages_merge(existing_stage_rows)

        return self.patch_salida_materiales(record['_id'], answers_salida)

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_actualizar_salida = stock_obj.actualizar_salida_materiales()
    stock_obj.HttpResponse({"data": resp_actualizar_salida})
