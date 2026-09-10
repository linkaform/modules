# -*- coding: utf-8 -*-
import sys, random
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

    def _get_item_serials(self, item):
        """
        Junta los numeros de serie capturados para un item de la Transferencia
        (cajas escaneadas + la unidad suelta, si aplica).

        Args:
            item (dict): un elemento de self.data['items'].

        Returns:
            list[dict]: lista de series (cada una con al menos 'value').
        """
        serials = []
        for box in item.get('boxScans') or []:
            serials.extend(box.get('serials') or [])

        loose_unit = item.get('looseUnitScan')
        if loose_unit:
            serials.extend(loose_unit.get('serials') or [])

        return serials

    def build_move_group_lines(self):
        """
        Arma el grupo repetitivo `move_group` de STOCK_ONE_MANY_ONE a partir
        de self.data['items']. Se genera una linea por numero de serie
        capturado (lot_number); si un item no trae series (material a
        granel), se genera una sola linea agregada con la cantidad recibida.

        Returns:
            list[dict]: filas para el campo self.f['move_group'].
        """
        move_group_lines = []
        for item in self.data.get('items', []):
            sku = item.get('sku')
            info_catalog_sku = self.find_material_catalog_sku(sku, field_as_select=[self.f['product_code']])
            if not info_catalog_sku:
                print(f"ADVERTENCIA: no se encontro el sku '{sku}' en el catalogo")
                continue

            product_info = {
                self.f['product_code']: self.unlist(info_catalog_sku.get(self.f['product_code'])),
                self.f['sku']: self.unlist(info_catalog_sku.get(self.f['field_sku'])),
            }

            serials = self._get_item_serials(item)
            if serials:
                for serial in serials:
                    move_group_lines.append({
                        self.CATALOG_INVENTORY_OBJ_ID: {
                            **product_info,
                            self.f['lot_number']: serial.get('value'),
                        },
                        self.f['move_group_qty']: 1,
                    })
            else:
                move_group_lines.append({
                    self.CATALOG_INVENTORY_OBJ_ID: product_info,
                    self.f['move_group_qty']: item.get('receivedQuantity') or item.get('expectedQuantity', 0),
                })

        return move_group_lines

    def build_answers_stock_one_many_one(self):
        """
        Arma las respuestas para el nuevo registro de salida en
        STOCK_ONE_MANY_ONE, reutilizando el almacen origen/destino y los
        items ya presentes en self.data de la Transferencia de Materiales.

        Returns:
            dict: respuestas {field_id: valor} para STOCK_ONE_MANY_ONE.
        """
        return {
            self.f['fecha_recepcion']: self.today_str(date_format='datetime'),
            self.f['stock_status']: 'to_do',
            self.f['stock_move_comments']: f"Traspaso autorizado - Transferencia {self.data.get('folio')}",
            self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID: self.find_warehouse_location_catalog(self.data.get('originWarehouse')),
            self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID: self.find_warehouse_dest_catalog(self.data.get('destinationWarehouse')),
            self.f['move_group']: self.build_move_group_lines(),
        }

    def post_stock_one_many_one(self, answers):
        """
        Crea el registro de salida de almacen en STOCK_ONE_MANY_ONE
        (forma 'Salida Multiple Productos a una ubicacion').

        Args:
            answers (dict): respuestas {field_id: valor} a guardar.

        Returns:
            dict: respuesta de `lkf_api.post_forms_answers`.
        """
        metadata = self.lkf_api.get_metadata(self.STOCK_ONE_MANY_ONE, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Transferencia de Materiales",
                    "action": "Generar Salida por Traspaso Autorizado",
                    "script": "actualizar_transferencia_de_materiales.py",
                    "module": "stock_ont",
                    "function": "actualizar_transferencia_materiales",
                }
            },
            'answers': answers,
        })
        metadata['folio'] = f"TRASPASO-{str(int(random.random() * 1000))}"
        return self.lkf_api.post_forms_answers(metadata)

    def generar_traspaso_stock_one_many_one(self):
        answers_stock_one_many_one = self.build_answers_stock_one_many_one()
        return self.post_stock_one_many_one(answers_stock_one_many_one)

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

        resp_actualizar = self.patch_transferencia_materiales(record['_id'], answers_transferencia)

        if answers_transferencia[ self.f['field_status_transferencia'] ] == self.map_transfer_stages['transfer_authorized']:
            resp_actualizar['stock_one_many_one'] = self.generar_traspaso_stock_one_many_one()

        return resp_actualizar

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_actualizar_transferencia = stock_obj.actualizar_transferencia_materiales()
    stock_obj.HttpResponse({"data": resp_actualizar_transferencia})
