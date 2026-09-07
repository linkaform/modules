# -*- coding: utf-8 -*-
import sys
from transferencia_de_materiales import Stock
from account_settings import *

# Mismos field_id hardcodeados que usa get_damage_reports() en stock_ont_utils.py
# (no viven en self.f porque tampoco viven ahi en el script que los crea).
FIELD_DAMAGE_QUANTITY = '6a8f105cf579313536b1984d'
FIELD_DAMAGE_NOTE = '6a8f10acae2709fa995fc6be'
FIELD_DAMAGE_EVIDENCE = '6a8f10acae2709fa995fc6bd'

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.inv_map_stages = {v: k for k, v in self.map_stages.items()}
        self.inv_map_transfer_stages = {v: k for k, v in self.map_transfer_stages.items()}

    def get_transferencia_by_folio(self, folio):
        """
        Busca en la forma de Transferencia de Materiales (FORM_ID_TRANSFERENCIAS)
        el registro cuyo folio coincida con `folio`. A diferencia de search_catalog
        (pensado para catalogos), esto es un query de mongo (self.cr, via
        get_record_by_folio) contra la propia coleccion de formas de la cuenta.

        Args:
            folio (int|str): folio del registro a consultar.

        Returns:
            dict | None: respuestas (answers) del registro encontrado, o None si no existe.
        """
        folio_query = int(folio) if str(folio).isdigit() else folio
        record = self.get_record_by_folio(folio_query, self.FORM_ID_TRANSFERENCIAS)
        return record.get('answers') if record else None

    def unformat_fecha_evento(self, val):
        """Inversa de format_fecha_evento(): "YYYY-MM-DD HH:MM:SS" -> "YYYY-MM-DDTHH:MM:SS" """
        val = self.unlist(val)
        if not val or 'T' in val or ' ' not in val:
            return val
        fecha, hora = val.split(' ', 1)
        return f"{fecha}T{hora}"

    def _untitle(self, val):
        """Inversa best-effort de `.replace('_',' ').title()` (tipo de evento / razon de ajuste)."""
        val = self.unlist(val)
        if not val:
            return val
        return val.lower().replace(' ', '_')

    def _unbool(self, val):
        """Inversa de format_bool_value(): 'sí'/'no' -> True/False."""
        val = self.unlist(val)
        if val == 'sí':
            return True
        if val == 'no':
            return False
        return None

    def _unbuild_damage_reports(self, row_material):
        """
        Inversa de get_damage_reports(): esta ultima ya suma todas las
        damageReports originales en un solo total (cantidad, notas y
        evidencias combinadas), asi que aqui solo se puede recuperar UN
        damageReport agregado, no la lista individual original.
        """
        if FIELD_DAMAGE_QUANTITY not in row_material and FIELD_DAMAGE_NOTE not in row_material:
            return []
        return [{
            'quantity': row_material.get(FIELD_DAMAGE_QUANTITY, 0),
            'note': row_material.get(FIELD_DAMAGE_NOTE, ''),
            'evidence': row_material.get(FIELD_DAMAGE_EVIDENCE, []),
        }]

    def _unbuild_adjustment(self, row_material):
        """Inversa de get_adjustment_data()."""
        keys_ajuste = (
            self.f['adjust_prev_quantity'], self.f['adjust_note'],
            self.f['adjust_at'], self.f['adjust_by'], self.f['adjust_reason'],
        )
        if not any(key in row_material for key in keys_ajuste):
            return None
        return {
            'previousQuantity': row_material.get(self.f['adjust_prev_quantity']),
            'note': row_material.get(self.f['adjust_note']),
            'adjustedAt': self.unformat_fecha_evento(row_material.get(self.f['adjust_at'])),
            'adjustedBy': row_material.get(self.f['adjust_by']),
            'reason': self._untitle(row_material.get(self.f['adjust_reason'])),
        }

    def _unbuild_missing_reports(self, missing_rows, sku):
        """Inversa de make_missing_report(), filtrando por el sku del item."""
        return [{
            'quantity': row_missing.get(self.f['field_missing_quantity']),
            'note': row_missing.get(self.f['field_missing_note']),
            'evidence': row_missing.get(self.f['field_missing_evidence'], []),
            'createdAt': self.unformat_fecha_evento(row_missing.get(self.f['field_missing_at'])),
            'createdBy': row_missing.get(self.f['field_missing_by']),
        } for row_missing in missing_rows if self.unlist(row_missing.get(self.f['field_missing_sku'])) == sku]

    def _unbuild_boxes_series(self, row):
        """
        Inversa de get_materials_scan(): arma boxScans/looseUnitScan a partir
        de field_grp_boxes y field_grp_onts. Cada caja (tenga o no series)
        guarda su propio field_box_sku, asi que se usa directamente para
        reagruparla de vuelta a su item, sin depender de sus series.
        """
        box_rows = row.get(self.f['field_grp_boxes']) or []
        serie_rows = row.get(self.f['field_grp_onts']) or []

        series_by_box = {}
        for row_serie in serie_rows:
            serial_entry = {
                'value': self.unlist(row_serie.get(self.f['field_serie_num_serie'])),
                'reacondicionado': self._unbool(row_serie.get(self.f['field_serie_reacondicionado'])),
                'source': self.unlist(row_serie.get(self.f['field_serie_source'])),
                'correctedManually': self._unbool(row_serie.get(self.f['field_serie_corrected_manually'])),
            }
            box_id = self.unlist(row_serie.get(self.f['field_serie_box_id']))
            series_by_box.setdefault(box_id, []).append(serial_entry)

        box_scans_by_sku, loose_unit_by_sku = {}, {}
        for row_box in box_rows:
            box_id = self.unlist(row_box.get(self.f['field_box_id']))
            es_suelta = self.unlist(row_box.get(self.f['field_box_bool_loose_unit'])) == 'sí'
            sku_box = self.unlist(row_box.get(self.f['field_box_sku']))
            box_entry = {
                'id': None if es_suelta else box_id,
                'groupId': self.unlist(row_box.get(self.f['field_box_id_pallet'])),
                'labelPhotos': row_box.get(self.f['field_box_evidence'], []),
                'position': self.unlist(row_box.get(self.f['field_box_position'])),
                'scannedAt': self.unformat_fecha_evento(row_box.get(self.f['field_box_at'])),
                'scannedBy': self.unlist(row_box.get(self.f['field_box_by'])),
                'serials': series_by_box.get(box_id, []),
            }
            if es_suelta:
                loose_unit_by_sku[sku_box] = box_entry
            else:
                box_scans_by_sku.setdefault(sku_box, []).append(box_entry)

        return box_scans_by_sku, loose_unit_by_sku

    def _unbuild_items(self, row):
        """Inversa de build_grp_materiales()."""
        materiales_rows = row.get(self.bitacora_transportista_fields['grupo_desglose_empaque']) or []
        missing_rows = row.get(self.f['field_grp_missing_report']) or []
        box_scans_by_sku, loose_unit_by_sku = self._unbuild_boxes_series(row)

        items = []
        for idx, row_material in enumerate(materiales_rows):
            producto = row_material.get(self.f['obj_products']) or {}
            sku = self.unlist(producto.get(self.f['field_sku']))

            item = {
                'sku': sku,
                'name': self.unlist(producto.get(self.f['field_product_name'])),
                'unit': self.unlist(producto.get(self.f['field_unidad_medida'])),
                'expectedQuantity': row_material.get(self.bitacora_transportista_fields['cantidad_desglose']),
                'suggestedQuantity': row_material.get(self.bitacora_transportista_fields['cantidad_sugerida_desglose']),
                'receivedQuantity': row_material.get(self.bitacora_transportista_fields['cantidad_acumulada_desglose']),
            }

            damage_reports = self._unbuild_damage_reports(row_material)
            if damage_reports:
                item['damageReports'] = damage_reports

            item['adjustment'] = self._unbuild_adjustment(row_material)

            item['missingReports'] = self._unbuild_missing_reports(missing_rows, sku)

            box_scans = list(box_scans_by_sku.get(sku, []))
            loose_unit_scan = loose_unit_by_sku.get(sku)
            if idx == len(materiales_rows) - 1:
                # Cajas/unidades sueltas sin sku identificable (sin serie asociado)
                # se agregan al ultimo item, a falta de un dato que las ligue a un item.
                box_scans += box_scans_by_sku.get(None, [])
                loose_unit_scan = loose_unit_scan or loose_unit_by_sku.get(None)
            if box_scans:
                item['boxScans'] = box_scans
            if loose_unit_scan:
                item['looseUnitScan'] = loose_unit_scan

            items.append(item)

        return items

    def _unbuild_bitacora(self, rows):
        """Inversa de build_grp_bitacora()."""
        return [{
            'at': self.unformat_fecha_evento(row_evento.get(self.f['field_fecha_evento'])),
            'type': self._untitle(row_evento.get(self.f['field_tipo_evento'])),
            'detail': row_evento.get(self.f['field_detalle_evento']),
        } for row_evento in rows]

    def _unbuild_stages(self, rows):
        """Inversa de build_grp_stages()."""
        stages_data = {}
        for row_stage in rows:
            stage_key = self.inv_map_stages.get(self.unlist(row_stage.get(self.f['field_stage_name'])))
            if not stage_key:
                continue
            at_key = 'cancelledAt' if stage_key == 'cancellation' else 'confirmedAt'
            by_key = 'cancelledBy' if stage_key == 'cancellation' else 'confirmedBy'
            stages_data[stage_key] = {
                at_key: self.unformat_fecha_evento(row_stage.get(self.f['field_stage_at'])),
                by_key: row_stage.get(self.f['field_stage_by']),
                'reason': row_stage.get(self.f['field_stage_canceled_reason']),
            }
        return stages_data

    def _unbuild_delivery(self, row):
        """Inversa de la seccion `delivery` armada en tranferencia_de_materiales()."""
        transportista = row.get(self.f['obj_ubi_transportista']) or {}
        return {
            'estimatedDeliveryDate': row.get(self.f['field_delivery_estimated_date']),
            'signatureDataUrl': row.get(self.f['field_delivery_signature'], []),
            'signedAt': self.unformat_fecha_evento(row.get(self.f['field_delivery_signature_at'])),
            'evidence': {
                'transport': {'evidence': row.get(self.f['field_delivery_ev_transport'], [])},
                'plates': {'evidence': row.get(self.f['field_delivery_ev_plates'], [])},
                'material': {'evidence': row.get(self.f['field_delivery_ev_material'], [])},
            },
            'carrierName': self.unlist(transportista.get(self.f['field_nombre_transportista'])),
        }

    def consultar_transferencia_materiales(self):
        """
        Recibe el `folio` en self.data, consulta la forma de Transferencia de
        Materiales y regresa la informacion respetando la misma estructura del
        payload que recibe tranferencia_de_materiales() en transferencia_de_materiales.py.
        """
        folio = self.data.get('folio')
        if not folio:
            self.LKFException("No se recibio el folio de la Transferencia de Materiales a consultar")

        row = self.get_transferencia_by_folio(folio)
        if not row:
            self.LKFException(f"No se encontro ninguna Transferencia de Materiales con folio '{folio}'")

        origen = row.get(self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID) or {}
        destino = row.get(self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID) or {}

        resultado = {
            'folio': folio,
            'stage': self.inv_map_transfer_stages.get(self.unlist(row.get(self.f['field_status_transferencia']))),
            'analysisRange': {
                'startDate': self.unlist(row.get(self.f['field_transfer_date_from'])),
                'endDate': self.unlist(row.get(self.f['field_transfer_date_to'])),
            },
            'originWarehouse': self.unlist(origen.get(self.stk.WH.f['warehouse_location'])),
            'destinationWarehouse': self.unlist(destino.get(self.stk.WH.f['warehouse_location_dest'])),
            'items': self._unbuild_items(row),
            'events': self._unbuild_bitacora(row.get(self.f['field_grp_bitacora']) or []),
            'delivery': self._unbuild_delivery(row),
        }
        resultado.update(self._unbuild_stages(row.get(self.f['field_grp_stages']) or []))

        return resultado

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    resultado_transferencia = stock_obj.consultar_transferencia_materiales()
    print('+++ resultado_transferencia =', resultado_transferencia)

    if not stock_obj.current_record:
        stock_obj.HttpResponse({"data": resultado_transferencia})
