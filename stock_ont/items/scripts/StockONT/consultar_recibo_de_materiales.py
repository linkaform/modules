# -*- coding: utf-8 -*-
import sys
from recibo_de_materiales import Stock
from account_settings import *

# Mismos field_id hardcodeados que usa get_damage_reports() en stock_ont_utils.py
# (no viven en self.f porque tampoco viven ahi en el script que los crea).
FIELD_DAMAGE_QUANTITY = '6a8f105cf579313536b1984d'
FIELD_DAMAGE_NOTE = '6a8f10acae2709fa995fc6be'
FIELD_DAMAGE_EVIDENCE = '6a8f10acae2709fa995fc6bd'

# Inversa de map_evidence en build_grp_evidencias() (recibo_de_materiales.py)
MAP_EVIDENCE = {
    'Camión Cerrado': 'truckClosed',
    'Sello del Contenedor': 'containerSeal',
    'Candado del Contenedor': 'containerLock',
    'Camión Abierto': 'truckOpen',
    'Camión Vacío': 'truckEmpty',
}

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def get_recibo_by_folio(self, folio):
        """
        Busca en la forma de Bitacora de Transportistas (FORM_BITACORA_TRANSPORTISTA_ID)
        el registro cuyo folio coincida con `folio`.

        Args:
            folio (int|str): folio del registro a consultar.

        Returns:
            dict | None: respuestas (answers) del registro encontrado, o None si no existe.
        """
        folio_query = int(folio) if str(folio).isdigit() else folio
        record = self.get_record_by_folio(folio_query, self.FORM_BITACORA_TRANSPORTISTA_ID)
        return record.get('answers') if record else None

    def unformat_fecha_evento(self, val):
        """Inversa de format_fecha_evento(): "YYYY-MM-DD HH:MM:SS" -> "YYYY-MM-DDTHH:MM:SS" """
        val = self.unlist(val)
        if not val or 'T' in val or ' ' not in val:
            return val
        fecha, hora = val.split(' ', 1)
        return f"{fecha}T{hora}"

    def _untitle(self, val):
        """Inversa best-effort de `.replace('_',' ').title()` (tipo de evento)."""
        val = self.unlist(val)
        if not val:
            return val
        return val.lower().replace(' ', '_')

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

    def _unbool(self, val):
        """Inversa de format_bool_value(): 'sí'/'no' -> True/False."""
        val = self.unlist(val)
        if val == 'sí':
            return True
        if val == 'no':
            return False
        return None

    def _unbuild_items(self, row):
        """Inversa de build_grp_materiales() (llamado con is_transfer=True desde recibo_de_materiales.py)."""
        materiales_rows = row.get(self.f_bitacora['grupo_desglose_empaque']) or []
        box_scans_by_sku, loose_unit_by_sku = self._unbuild_boxes_series(row)

        items = []
        for idx, row_material in enumerate(materiales_rows):
            producto = row_material.get(self.f['obj_products']) or {}
            sku = self.unlist(producto.get(self.f['field_sku']))

            item = {
                'sku': sku,
                'name': self.unlist(producto.get(self.f['field_product_name'])),
                'unit': self.unlist(producto.get(self.f['field_unidad_medida'])),
                'expectedQuantity': row_material.get(self.f_bitacora['cantidad_desglose']),
                'suggestedQuantity': row_material.get(self.f_bitacora['cantidad_sugerida_desglose']),
                'receivedQuantity': row_material.get(self.f_bitacora['cantidad_acumulada_desglose']),
            }

            damage_reports = self._unbuild_damage_reports(row_material)
            if damage_reports:
                item['damageReports'] = damage_reports

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

    def _unbuild_material_documents(self, row):
        """Inversa de build_grp_inspecciones()."""
        grp_inspecciones = row.get(self.f['field_grp_inspecciones']) or [{}]
        inspecciones = grp_inspecciones[0]
        return {
            'cartaPorte': inspecciones.get(self.f['field_fotos_carta_porte'], []),
            'factura': inspecciones.get(self.f['field_fotos_factura'], []),
            'pedimento': inspecciones.get(self.f['field_fotos_pedimento'], []),
            'ordenCompra': inspecciones.get(self.f['field_fotos_orden_compra'], []),
        }, inspecciones.get(self.f['field_fotos_docs_transportista'], [])

    def _unbuild_shared_evidence(self, rows):
        """Inversa de build_grp_evidencias()."""
        shared_evidence = {}
        for row_evidencia in rows:
            tipo_documento = self.unlist(row_evidencia.get(self.f_bitacora['tipo_de_documento']))
            name_evidencia = MAP_EVIDENCE.get(tipo_documento)
            if not name_evidencia:
                continue
            shared_evidence[name_evidencia] = {'evidence': row_evidencia.get(self.f_bitacora['documento'], [])}
        return shared_evidence

    def _unbuild_delivery(self, row):
        """Inversa de los datos de entrega armados en create_record_bitacora_transportista()."""
        destino = row.get(self.f['obj_almacen_destino']) or {}
        origen = row.get(self.f['obj_wh_locations']) or {}
        transportista = row.get(self.f['obj_ubi_transportista']) or {}

        delivery_date = self.unlist(row.get(self.f_bitacora['fecha_hora_ingreso']))
        if delivery_date and ' ' in delivery_date:
            delivery_date = delivery_date.split(' ', 1)[0]

        return {
            'requestedBy': self.unlist(destino.get(self.f['field_nombre_usuario_almacen_destino'])),
            'providerName': self.unlist(origen.get(self.f['field_location_transportista'])),
            'carrierName': self.unlist(transportista.get(self.f['field_nombre_transportista'])),
            'deliveryDate': delivery_date,
        }

    def consultar_recibo_materiales(self):
        """
        Recibe el `folio` en self.data, consulta la forma de Bitacora de
        Transportistas y regresa la informacion respetando la misma estructura
        del payload que recibe recibo_de_materiales() en recibo_de_materiales.py.
        """
        folio = self.data.get('folio')
        if not folio:
            self.LKFException("No se recibio el folio del Recibo de Materiales a consultar")

        row = self.get_recibo_by_folio(folio)
        if not row:
            self.LKFException(f"No se encontro ningun Recibo de Materiales con folio '{folio}'")

        material_documents, carrier_documents = self._unbuild_material_documents(row)

        return {
            'folio': folio,
            'stage': self.unlist(row.get(self.f['field_status_transferencia'])),
            'delivery': self._unbuild_delivery(row),
            'materialDocuments': material_documents,
            'carrierDocuments': carrier_documents,
            'sharedEvidence': self._unbuild_shared_evidence(row.get(self.f_bitacora['grupo_fotos_y_documentos']) or []),
            'items': self._unbuild_items(row),
            'signature': {'signatureDataUrl': row.get(self.f_bitacora['firma_conductor'], {})},
            'events': self._unbuild_bitacora(row.get(self.f['field_grp_bitacora']) or []),
        }

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()

    resultado_recibo = stock_obj.consultar_recibo_materiales()
    print('+++ resultado_recibo =', resultado_recibo)

    if not stock_obj.current_record:
        stock_obj.HttpResponse({"data": resultado_recibo})
