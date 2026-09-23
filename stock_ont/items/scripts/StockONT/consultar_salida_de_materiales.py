# -*- coding: utf-8 -*-
import sys
from salida_de_materiales import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.inv_map_salida_phases = {v: k for k, v in self.map_salida_phases.items()}
        self.inv_map_salida_stages = {v: k for k, v in self.map_salida_stages.items()}

    def get_salida_by_folio(self, folio):
        """
        Busca en la forma de Salida de Material (FORM_ID_SALIDAS) el
        registro cuyo folio coincida con `folio`.

        Args:
            folio (int|str): folio (id del vale) del registro a consultar.

        Returns:
            dict | None: respuestas (answers) del registro encontrado, o None si no existe.
        """
        folio_query = int(folio) if str(folio).isdigit() else folio
        record = self.get_record_by_folio(folio_query, self.FORM_ID_SALIDAS)
        return record.get('answers') if record else None

    def unformat_fecha_evento(self, val):
        """Inversa de format_fecha_evento(): "YYYY-MM-DD HH:MM:SS" -> "YYYY-MM-DDTHH:MM:SS" """
        val = self.unlist(val)
        if not val or 'T' in val or ' ' not in val:
            return val
        fecha, hora = val.split(' ', 1)
        return f"{fecha}T{hora}"

    def _unformat_delivery_type(self, val):
        """Inversa de format_delivery_type()."""
        val = self.unlist(val)
        if val == 'foránea':
            return True
        if val == 'local':
            return False
        return None

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
            'reason': self.unlist(row_material.get(self.f['adjust_reason'])),
        }

    def _unbuild_boxes_series(self, row):
        """
        Inversa de get_materials_scan(): arma boxScans/looseUnitScan a partir
        de field_grp_boxes y field_grp_onts. Igual a consultar_transferencia_
        de_materiales.py, pues comparten los mismos field_id.
        """
        box_rows = row.get(self.f['field_grp_boxes']) or []
        serie_rows = row.get(self.f['field_grp_onts']) or []

        series_by_box = {}
        for row_serie in serie_rows:
            serial_entry = {
                'value': self.unlist(row_serie.get(self.f['field_serie_num_serie'])),
                'reacondicionado': self.unlist(row_serie.get(self.f['field_serie_reacondicionado'])) == 'sí',
                'source': self.unlist(row_serie.get(self.f['field_serie_source'])),
                'correctedManually': self.unlist(row_serie.get(self.f['field_serie_corrected_manually'])) == 'sí',
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
        tarimas de field_grp_tarimas por el sku con el que se guardaron.
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
        """Inversa de la seccion `distribution` de cada item."""
        pallet_groups = list(pallets_by_sku.get(sku, []))
        if is_last:
            pallet_groups += pallets_by_sku.get(None, [])
        return {
            'palletGroups': pallet_groups,
            'looseUnits': row_material.get(self.bitacora_transportista_fields['cantidad_unidades_sueltas'], 0),
        }

    def _get_ns_need_by_sku(self, skus):
        """
        Consulta de una sola vez el catalogo de productos y regresa
        {sku: bool} indicando si el sku requiere captura de numero de serie.
        El objeto de producto guardado en la Salida no incluye
        capture_num_serie, por eso se consulta el catalogo.
        """
        skus = list({sku for sku in skus if sku})
        if not skus:
            return {}

        mango_query = {
            "selector": {
                "answers": {
                    self.f['field_sku']: {"$in": skus},
                },
            },
            "limit": len(skus),
            "skip": 0,
        }
        records = self.lkf_api.search_catalog(self.CATALOG_ID_SKU, mango_query, jwt_settings_key='APIKEY_JWT_KEY') or []

        return {
            self.unlist(r.get(self.f['field_sku'])): str(self.unlist(r.get(self.f['capture_num_serie'])) or '').strip().lower() in ('si', 'sí')
            for r in records
        }

    def _unbuild_items(self, row):
        """Inversa de build_grp_materiales_salida()."""
        materiales_rows = row.get(self.bitacora_transportista_fields['grupo_desglose_empaque']) or []
        box_scans_by_sku, loose_unit_by_sku = self._unbuild_boxes_series(row)
        pallets_by_sku = self._unbuild_pallets(row)

        ns_need_by_sku = self._get_ns_need_by_sku(
            self.unlist((row_material.get(self.f['obj_products']) or {}).get(self.f['field_sku']))
            for row_material in materiales_rows
        )

        items = []
        for idx, row_material in enumerate(materiales_rows):
            producto = row_material.get(self.f['obj_products']) or {}
            sku = self.unlist(producto.get(self.f['field_sku']))

            item = {
                'sku': sku,
                'name': self.unlist(producto.get(self.f['field_product_name'])),
                'unit': self.unlist(producto.get(self.f['field_unidad_medida'])),
                'nsNeed': ns_need_by_sku.get(sku, False),
                'expectedQuantity': row_material.get(self.bitacora_transportista_fields['cantidad_desglose']),
                'suggestedQuantity': row_material.get(self.bitacora_transportista_fields['cantidad_sugerida_desglose']),
                'receivedQuantity': row_material.get(self.bitacora_transportista_fields['cantidad_acumulada_desglose']),
                'recipientStock': row_material.get(self.f['recipient_stock']),
                'recipientProduction': row_material.get(self.f['recipient_production']),
            }

            item['adjustment'] = self._unbuild_adjustment(row_material)

            is_last = idx == len(materiales_rows) - 1

            box_scans = list(box_scans_by_sku.get(sku, []))
            loose_unit_scan = loose_unit_by_sku.get(sku)
            if is_last:
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
            'type': self.unlist(row_evento.get(self.f['field_tipo_evento'], '')).lower().replace(' ', '_'),
            'detail': row_evento.get(self.f['field_detalle_evento']),
        } for row_evento in rows]

    def _unbuild_stages(self, rows):
        """Inversa de build_grp_stages()."""
        stages_data = {}
        for row_stage in rows:
            stage_key = self.inv_map_salida_phases.get(self.unlist(row_stage.get(self.f['field_stage_name'])))
            if not stage_key:
                continue

            stage_at = self.unformat_fecha_evento(row_stage.get(self.f['field_stage_at']))
            stage_by = row_stage.get(self.f['field_stage_by'])

            if stage_key == 'calculation':
                stages_data[stage_key] = {'generatedAt': stage_at, 'generatedBy': stage_by}
            else:
                stages_data[stage_key] = {'confirmedAt': stage_at, 'confirmedBy': stage_by}

            if stage_key == 'preparation':
                stages_data[stage_key]['startedAt'] = self.unformat_fecha_evento(row_stage.get(self.f['field_started_at']))
                stages_data[stage_key]['startedBy'] = row_stage.get(self.f['field_started_by'])

        return stages_data

    def _unbuild_recipient(self, row):
        """
        Inversa de build_answers_recipient(). finalContratista se guarda en
        dos catalogos independientes (obj_catalog_contratistas y el
        warehouse-dest via find_by_wh=True), cada uno solo si su busqueda
        encontro coincidencia, asi que se lee con fallback entre ambos.
        """
        contratista_1_0 = row.get(self.f['obj_catalog_contratistas']) or {}
        contratista_wh_dest = row.get(self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID) or {}

        final_contratista = self.unlist(contratista_1_0.get(self.f['field_nombre_contratista'])) \
            or self.unlist(contratista_wh_dest.get(self.stk.WH.f['warehouse_dest']))

        return {
            'type': self.unlist(row.get(self.f['recipient_type'])),
            'name': '',  # TODO: pendiente field_id de recipient.name.
            'finalContratista': final_contratista,
        }

    def _unbuild_delivery(self, row):
        """Inversa de build_answers_delivery()."""
        is_foranea = self._unformat_delivery_type(row.get(self.f['delivery_type']))

        signature = {
            'file_url': row.get(self.f['field_delivery_signature'], []),
            'capturedAt': self.unformat_fecha_evento(row.get(self.f['field_delivery_signature_at'])),
            'capturedVia': self.unlist(row.get(self.f['captured_via'])),
        }
        confirmed_at = self.unformat_fecha_evento(row.get(self.f['field_delivery_signature_at']))
        confirmed_by = self.unlist(row.get(self.f['confirmed_by']))

        delivery = {
            'isForanea': is_foranea,
            'estimatedDeliveryDate': self.unlist(row.get(self.f['field_delivery_estimated_date'])),
            'dispatchInfoSetAt': self.unformat_fecha_evento(row.get(self.f['dispatch_info_at'])),
            'dispatchInfoSetBy': self.unlist(row.get(self.f['dispatch_info_by'])),
        }

        if is_foranea is False:
            delivery['local'] = {
                'signature': signature,
                'confirmedAt': confirmed_at,
                'confirmedBy': confirmed_by,
            }
        elif is_foranea is True:
            delivery['transit'] = {
                'evidence': {
                    'material': row.get(self.f['field_delivery_ev_material'], []),
                    'vehicle': row.get(self.f['field_delivery_ev_transport'], []),
                    'plates': row.get(self.f['field_delivery_ev_plates'], []),
                    'ine': row.get(self.f['field_delivery_ev_ine'], []),
                    'license': row.get(self.f['field_delivery_ev_license'], []),
                },
                'driverSignature': signature,
                'confirmedAt': confirmed_at,
                'confirmedBy': confirmed_by,
            }
            delivery['supervisorReview'] = {
                'evidence': {
                    'securitySeal': row.get(self.f['field_supervisor_ev_security_seal'], []),
                    'wrap': row.get(self.f['field_supervisor_ev_wrap'], []),
                    'printedVoucher': row.get(self.f['field_supervisor_ev_printed_voucher'], []),
                },
                'confirmedAt': self.unformat_fecha_evento(row.get(self.f['field_supervisor_at'])),
                'confirmedBy': self.unlist(row.get(self.f['field_supervisor_by'])),
            }
            delivery['finalDelivery'] = {
                'signature': {
                    'file_url': row.get(self.f['field_final_delivery_signature'], []),
                    'capturedAt': self.unformat_fecha_evento(row.get(self.f['field_final_delivery_captured_at'])),
                    'capturedVia': self.unlist(row.get(self.f['field_final_delivery_captured_via'])),
                },
                'confirmedAt': self.unformat_fecha_evento(row.get(self.f['field_final_delivery_at'])),
                'confirmedBy': self.unlist(row.get(self.f['field_final_delivery_by'])),
            }

        return delivery

    def consultar_salida_materiales(self):
        """
        Recibe el `id` (folio) en self.data, consulta la forma de Salida de
        Material y regresa la informacion respetando la misma estructura del
        payload que recibe salida_de_materiales() en salida_de_materiales.py.
        """
        folio = self.data.get('folio')
        if not folio:
            self.LKFException("No se recibio el id/folio de la Salida de Material a consultar")

        row = self.get_salida_by_folio(folio)
        if not row:
            self.LKFException(f"No se encontro ninguna Salida de Material con folio '{folio}'")

        origen = row.get(self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID) or {}

        resultado = {
            'folio': folio,
            'runId': '',
            'stage': self.inv_map_salida_stages.get(self.unlist(row.get(self.f['field_status_transferencia']))),
            'originWarehouse': self.unlist(origen.get(self.stk.WH.f['warehouse_location'])),
            'recipient': self._unbuild_recipient(row),
            'items': self._unbuild_items(row),
            'events': self._unbuild_bitacora(row.get(self.f['field_grp_bitacora']) or []),
            'delivery': self._unbuild_delivery(row),
        }
        resultado.update(self._unbuild_stages(row.get(self.f['field_grp_stages']) or []))

        return resultado

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    resultado_salida = stock_obj.consultar_salida_materiales()
    print('+++ resultado_salida =', resultado_salida)

    if not stock_obj.current_record:
        stock_obj.HttpResponse({"data": resultado_salida})
