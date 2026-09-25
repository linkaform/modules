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

    def _unbuild_pallets(self, row):
        """
        Inversa de build_grp_tarimas() (stock_ont_utils.py): agrupa las
        tarimas de field_grp_tarimas por el sku con el que se guardaron
        (field_sku_pallet_association), para poder reagruparlas de vuelta a
        su item sin depender de que ya tengan cajas escaneadas.

        Returns:
            dict: {sku: [{id, palletCount, boxesPerPallet, unitsPerBox}, ...]}.
            Las tarimas sin sku identificable quedan bajo la llave `None`.
        """
        pallets_by_sku = {}
        for row_pallet in row.get(self.f['field_grp_tarimas']) or []:
            pallet_id = self.unlist(row_pallet.get(self.f['field_pallet_id']))
            if not pallet_id:
                continue
            sku_pallet = self.unlist(row_pallet.get(self.f['field_sku_pallet_association']))
            pallets_by_sku.setdefault(sku_pallet, []).append({
                'id': pallet_id,
                'palletCount': row_pallet.get(self.f['field_pallet_count']),
                'boxesPerPallet': row_pallet.get(self.f['field_boxes_by_pallet']),
                'unitsPerBox': row_pallet.get(self.f['field_units_by_box']),
            })
        return pallets_by_sku

    def _unbuild_distribution(self, sku, row_material, pallets_by_sku, is_last):
        """
        Inversa de la seccion `distribution` de cada item (looseUnits armado
        en build_grp_materiales, palletGroups armado en build_grp_tarimas).

        Args:
            sku (str | None): sku del item, para relacionar sus tarimas.
            row_material (dict): fila de grupo_desglose_empaque del item.
            pallets_by_sku (dict): ver _unbuild_pallets().
            is_last (bool): si es el ultimo item, se le agregan tambien las
                tarimas sin sku identificable (ver _unbuild_items).
        """
        pallet_groups = list(pallets_by_sku.get(sku, []))
        if is_last:
            pallet_groups += pallets_by_sku.get(None, [])
        return {
            'palletGroups': pallet_groups,
            'looseUnits': row_material.get(self.bitacora_transportista_fields['cantidad_unidades_sueltas'], 0),
        }

    def _get_actual_quantity(self, product_code, sku, warehouse, location, cache):
        """
        Inversa/analogo de add_actual_quantity() (consultar_material_estimado.py),
        pero por (product_code, sku) contra el almacen/ubicacion de origen del
        propio registro de Transferencia (en vez del almacen fijo 'Almacen
        Distribuidor' que usa material_estimado, propio de otro flujo).
        """
        if not warehouse and not location:
            return None

        cache_key = (product_code, sku)
        if cache_key not in cache:
            stock_inventory = self.stk.get_product_stock(
                product_code, sku=sku, warehouse=warehouse, location=location
            )
            cache[cache_key] = stock_inventory.get('actuals') or 0

        return cache[cache_key]

    def _unbuild_items(self, row, warehouse_origen=None, location_origen=None, get_stock=False):
        """Inversa de build_grp_materiales()."""
        materiales_rows = row.get(self.bitacora_transportista_fields['grupo_desglose_empaque']) or []
        missing_rows = row.get(self.f['field_grp_missing_report']) or []
        box_scans_by_sku, loose_unit_by_sku = self._unbuild_boxes_series(row)
        pallets_by_sku = self._unbuild_pallets(row)
        actual_quantity_cache = {}

        items = []
        for idx, row_material in enumerate(materiales_rows):
            producto = row_material.get(self.f['obj_products']) or {}
            sku = self.unlist(producto.get(self.f['field_sku']))
            product_code = self.unlist(producto.get(self.f['field_product_code']))

            if get_stock:
                # Se pidio el stock en vivo (get_stock=true): se consulta contra
                # el almacen/ubicacion de origen en vez de leer lo ya guardado.
                actual_quantity = self._get_actual_quantity(
                    product_code, sku, warehouse_origen, location_origen, actual_quantity_cache
                )
            else:
                actual_quantity = row_material.get(self.f['field_actual_quantity'])

            item = {
                'sku': sku,
                'name': self.unlist(producto.get(self.f['field_product_name'])),
                'unit': self.unlist(producto.get(self.f['field_unidad_medida'])),
                'expectedQuantity': row_material.get(self.bitacora_transportista_fields['cantidad_desglose']),
                'suggestedQuantity': row_material.get(self.bitacora_transportista_fields['cantidad_sugerida_desglose']),
                'receivedQuantity': row_material.get(self.bitacora_transportista_fields['cantidad_acumulada_desglose']),
                'actualQuantity': actual_quantity,
            }

            damage_reports = self._unbuild_damage_reports(row_material)
            if damage_reports:
                item['damageReports'] = damage_reports

            item['adjustment'] = self._unbuild_adjustment(row_material)

            item['missingReports'] = self._unbuild_missing_reports(missing_rows, sku)

            is_last = idx == len(materiales_rows) - 1

            box_scans = list(box_scans_by_sku.get(sku, []))
            loose_unit_scan = loose_unit_by_sku.get(sku)
            if is_last:
                # Cajas/unidades sueltas sin sku identificable (sin serie asociado)
                # se agregan al ultimo item, a falta de un dato que las ligue a un item.
                box_scans += box_scans_by_sku.get(None, [])
                loose_unit_scan = loose_unit_scan or loose_unit_by_sku.get(None)
            if box_scans:
                item['boxScans'] = box_scans
            if loose_unit_scan:
                item['looseUnitScan'] = loose_unit_scan

            item['distribution'] = self._unbuild_distribution(sku, row_material, pallets_by_sku, is_last)

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
                'ine': {'evidence': row.get(self.f['field_delivery_ev_ine'], [])},
                'license': {'evidence': row.get(self.f['field_delivery_ev_license'], [])},
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

        warehouse_origen = self.unlist(origen.get(self.stk.WH.f['warehouse']))
        location_origen = self.unlist(origen.get(self.stk.WH.f['warehouse_location']))
        get_stock = bool(self.data.get('get_stock'))

        resultado = {
            'folio': folio,
            'stage': self.inv_map_transfer_stages.get(self.unlist(row.get(self.f['field_status_transferencia']))),
            'analysisRange': {
                'startDate': self.unlist(row.get(self.f['field_transfer_date_from'])),
                'endDate': self.unlist(row.get(self.f['field_transfer_date_to'])),
            },
            'originWarehouse': location_origen,
            'destinationWarehouse': self.unlist(destino.get(self.stk.WH.f['warehouse_location_dest'])),
            'items': self._unbuild_items(row, warehouse_origen, location_origen, get_stock=get_stock),
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
