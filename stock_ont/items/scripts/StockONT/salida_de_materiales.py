# -*- coding: utf-8 -*-
import sys, simplejson
from stock_ont_utils import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.data = self.data.get('data')

        # Fases que se guardan como filas genericas en field_grp_stages
        # (mismo mecanismo que map_stages en transferencia_de_materiales.py).
        self.map_salida_phases = {
            'calculation': 'cálculo',
            'authorization': 'autorización',
            'preparation': 'preparación',
        }

        self.map_salida_stages = {
            'pending_authorization': 'pendiente_de_autorización',
            'voucher_generated': 'vale_generado',
            'in_preparation': 'en_preparación',
            'ready_to_deliver': 'listo_para_entrega',
            'awaiting_local_delivery': 'por_entregar_(local)',
            'awaiting_transit': 'alistando_despacho',
            'awaiting_supervisor_review': 'en_tránsito',
            'awaiting_final_delivery': 'por_entregar',
            'delivered': 'entregado',
        }

    def post_salida_materiales(self, answers):
        """
        Crea el registro en la forma de Salida de Material
        (form_id FORM_ID_SALIDAS) con las respuestas ya armadas.

        Args:
            answers (dict): respuestas {field_id: valor} a guardar.

        Returns:
            dict: respuesta de `lkf_api.post_forms_answers`.
        """
        metadata = self.lkf_api.get_metadata(self.FORM_ID_SALIDAS, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Salida de Material",
                    "action": "Crear Salida de Material",
                    "script": "salida_de_materiales.py",
                    "module": "stock_ont",
                    "function": "post_salida_materiales",
                }
            },
            'answers': answers,
        })
        # El `id` del vale lo genera el front (formato SAL-...) y se usa como
        # folio para poder localizar el registro despues (actualizar/consultar),
        # igual que TRASPASO-<n> en post_stock_one_many_one.
        metadata['folio'] = self.data.get('id')
        return self.lkf_api.post_forms_answers(metadata)

    def format_delivery_type(self, is_foranea):
        """
        Traduce `delivery.isForanea` (bool | None) al valor de texto que
        distingue local vs foranea en el campo `delivery_type`.
        """
        if is_foranea is None:
            return None
        return 'foránea' if is_foranea else 'local'

    def _build_stage_row(self, stage, name_stage, data_stage):
        """
        Arma una fila de field_grp_stages para calculation/authorization/
        preparation. A diferencia de transferencia (siempre confirmedAt/
        confirmedBy), aqui 'calculation' usa generatedAt/generatedBy, y
        'preparation' trae ademas un segundo evento (startedAt/startedBy)
        en la misma fila.
        """
        if stage == 'calculation':
            stage_at, stage_by = data_stage.get('generatedAt'), data_stage.get('generatedBy')
        else:
            stage_at, stage_by = data_stage.get('confirmedAt'), data_stage.get('confirmedBy')

        started_at, started_by = data_stage.get('startedAt'), data_stage.get('startedBy')

        if not any([stage_at, stage_by, started_at, started_by]):
            return None

        return {
            self.f['field_stage_name']: name_stage,
            self.f['field_stage_at']: self.format_fecha_evento(stage_at),
            self.f['field_stage_by']: stage_by,
            self.f['field_started_at']: self.format_fecha_evento(started_at),
            self.f['field_started_by']: started_by,
        }

    def build_grp_stages(self):
        data_grp_stages = []
        for stage, name_stage in self.map_salida_phases.items():
            data_stage = self.data.get(stage, {})
            if not data_stage:
                continue

            row_stage = self._build_stage_row(stage, name_stage, data_stage)
            if row_stage:
                data_grp_stages.append(row_stage)
        return data_grp_stages

    def build_grp_materiales_salida(self, materiales_data):
        """
        Igual al desglose de Transferencia (build_grp_materiales con
        is_transfer=True: adjustment, distribution, boxScans/looseUnitScan),
        agregando ademas recipientStock/recipientProduction, propios de la
        Salida, a cada fila del desglose.

        Returns:
            tuple: (grp_materiales, grp_boxes, grp_pallets, grp_series)
        """
        grp_materiales, grp_boxes, grp_pallets, grp_series, _grp_missing = self.build_grp_materiales(materiales_data, is_transfer=True)

        for row_material, item in zip(grp_materiales, materiales_data):
            row_material[self.f['recipient_stock']] = item.get('recipientStock', 0)
            row_material[self.f['recipient_production']] = item.get('recipientProduction', 0)

        return grp_materiales, grp_boxes, grp_pallets, grp_series

    def build_answers_recipient(self, recipient_data):
        answers_recipient = {
            self.f['recipient_type']: recipient_data.get('type'),
            # TODO: falta el field_id para recipient.name, pendiente de definir.
        }

        final_contratista = recipient_data.get('finalContratista')
        if final_contratista:
            info_catalog_contratista = self.find_warehouse_dest_catalog(final_contratista, find_by_wh=True)
            if info_catalog_contratista:
                answers_recipient[self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID] = info_catalog_contratista

            info_contratistas_1_0 = self.find_contratista_catalog(final_contratista)
            # print(f"+++ info_contratistas_1_0 = {info_contratistas_1_0}")
            if info_contratistas_1_0:
                answers_recipient[self.f['obj_catalog_contratistas']] = info_contratistas_1_0

        return answers_recipient

    def build_answers_delivery(self, delivery_data):
        """
        Arma las respuestas de la seccion `delivery`. `local.signature` y
        `transit.driverSignature` comparten los mismos campos de firma
        (field_delivery_signature / field_delivery_signature_at /
        captured_via / confirmed_by); cual de los dos aplico se distingue
        despues con `delivery_type` (isForanea).
        """
        local_data = delivery_data.get('local') or {}
        transit_data = delivery_data.get('transit') or {}
        supervisor_data = delivery_data.get('supervisorReview') or {}
        final_data = delivery_data.get('finalDelivery') or {}

        signature_data = local_data.get('signature') or transit_data.get('driverSignature') or {}
        confirmed_at = local_data.get('confirmedAt') or transit_data.get('confirmedAt')
        confirmed_by = local_data.get('confirmedBy') or transit_data.get('confirmedBy')

        transit_evidence = transit_data.get('evidence') or {}
        supervisor_evidence = supervisor_data.get('evidence') or {}
        final_signature = final_data.get('signature') or {}

        return {
            self.f['field_delivery_estimated_date']: delivery_data.get('estimatedDeliveryDate'),
            self.f['dispatch_info_at']: self.format_fecha_evento(delivery_data.get('dispatchInfoSetAt')),
            self.f['dispatch_info_by']: delivery_data.get('dispatchInfoSetBy'),
            self.f['delivery_type']: self.format_delivery_type(delivery_data.get('isForanea')),

            self.f['field_delivery_signature']: signature_data.get('file_url', []),
            self.f['field_delivery_signature_at']: self.format_fecha_evento(signature_data.get('capturedAt')),
            self.f['captured_via']: signature_data.get('capturedVia'),
            self.f['confirmed_by']: confirmed_by,
            # No hay un campo propio para el confirmedAt de local/transit: se
            # asume el mismo momento que field_delivery_signature_at.

            self.f['field_delivery_ev_transport']: transit_evidence.get('vehicle', []),
            self.f['field_delivery_ev_plates']: transit_evidence.get('plates', []),
            self.f['field_delivery_ev_material']: transit_evidence.get('material', []),
            self.f['field_delivery_ev_ine']: transit_evidence.get('ine', []),
            self.f['field_delivery_ev_license']: transit_evidence.get('license', []),

            self.f['field_supervisor_at']: self.format_fecha_evento(supervisor_data.get('confirmedAt')),
            self.f['field_supervisor_by']: supervisor_data.get('confirmedBy'),
            self.f['field_supervisor_ev_security_seal']: supervisor_evidence.get('securitySeal', []),
            self.f['field_supervisor_ev_wrap']: supervisor_evidence.get('wrap', []),
            self.f['field_supervisor_ev_printed_voucher']: supervisor_evidence.get('printedVoucher', []),

            self.f['field_final_delivery_at']: self.format_fecha_evento(final_data.get('confirmedAt')),
            self.f['field_final_delivery_by']: final_data.get('confirmedBy'),
            self.f['field_final_delivery_signature']: final_signature.get('file_url', []),
            self.f['field_final_delivery_captured_at']: self.format_fecha_evento(final_signature.get('capturedAt')),
            self.f['field_final_delivery_captured_via']: final_signature.get('capturedVia'),
        }

    def build_answers_salida(self):
        materiales_data = self.data.get('items', [])
        grp_materiales, grp_boxes, grp_pallets, grp_series = self.build_grp_materiales_salida(materiales_data)

        answers_salida = {
            self.f['field_status_transferencia']: self.map_salida_stages.get(self.data.get('stage')),
            self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID: self.find_warehouse_location_catalog(self.data.get('originWarehouse')),
            self.bitacora_transportista_fields['grupo_desglose_empaque']: grp_materiales,
            self.f['field_grp_tarimas']: self.build_grp_tarimas(grp_pallets),
            self.f['field_grp_boxes']: grp_boxes,
            self.f['field_grp_onts']: grp_series,
            self.f['field_grp_bitacora']: self.build_grp_bitacora(self.data.get('events', [])),
            self.f['field_grp_stages']: self.build_grp_stages(),
        }
        answers_salida.update(self.build_answers_recipient(self.data.get('recipient', {}) or {}))
        answers_salida.update(self.build_answers_delivery(self.data.get('delivery', {}) or {}))

        # print('answers_salida =', simplejson.dumps(answers_salida, indent=4))

        return answers_salida

    def salida_de_materiales(self):
        answers_salida = self.build_answers_salida()
        return self.post_salida_materiales(answers_salida)

    def _get_disponible_quantity(self, sku, warehouse, location, cache):
        """
        Existencia actual del sku en el almacen/ubicacion de origen (mismo
        mecanismo que _get_actual_quantity en consultar_transferencia_de_
        materiales.py), cacheada por (sku, warehouse, location) para no
        repetir la consulta entre registros que comparten almacen de origen.
        """
        cache_key = (sku, warehouse, location)
        if cache_key not in cache:
            info_catalog_sku = self.find_material_catalog_sku(sku, field_as_select=[self.f['product_code']])
            product_code = self.unlist(info_catalog_sku.get(self.f['product_code'])) if info_catalog_sku else None

            stock_inventory = self.stk.get_product_stock(product_code, sku=sku, warehouse=warehouse, location=location)
            cache[cache_key] = stock_inventory.get('actuals') or 0

        return cache[cache_key]

    def build_individual_and_grouped_response(self, registros):
        """
        Arma el formato {individual, grouped_by_sku} que espera el Front, a
        partir de una lista de registros ya armados: {folio, originWarehouse,
        recipient: {type, finalContratista}, items: [{sku, name,
        suggestedQuantity}], stage}. Usado por generar_salidas_por_
        contratista.py (recien creados) y listar_salida_de_materiales.py (ya
        existentes en la BD).

        Returns:
            dict: {individual: [...], grouped_by_sku: [...]}.
        """
        resultados_individual = []
        grupos_by_sku = {}
        disponible_cache = {}

        for registro in registros:
            folio = registro.get('folio')
            recipient_data = registro.get('recipient') or {}
            recipient_type = recipient_data.get('type')
            final_contratista = recipient_data.get('finalContratista')
            origin_warehouse = registro.get('originWarehouse')

            resultados_individual.append({
                'folio': folio,
                'originWarehouse': origin_warehouse,
                'recipient': {
                    'recipientType': recipient_type,
                    'finalContratista': final_contratista,
                },
                'totalItems': len(registro.get('items', [])),
                'stage': registro.get('stage'),
            })

            info_wh_origen = self.find_warehouse_location_catalog(origin_warehouse) or {}
            warehouse_origen = self.unlist(info_wh_origen.get(self.stk.WH.f['warehouse']))
            location_origen = self.unlist(info_wh_origen.get(self.stk.WH.f['warehouse_location']))

            for item in registro.get('items', []):
                sku = item.get('sku')
                quantity = item.get('suggestedQuantity', 0)

                grupo_sku = grupos_by_sku.setdefault(sku, {
                    'sku': sku,
                    'name': item.get('name'),
                    'totalQuantity': 0,
                    'disponibleQuantity': self._get_disponible_quantity(sku, warehouse_origen, location_origen, disponible_cache),
                    'recipients': [],
                })
                grupo_sku['totalQuantity'] += quantity
                grupo_sku['recipients'].append({
                    'folio': folio,
                    'quantity': quantity,
                    'recipientType': recipient_type,
                    'finalContratista': final_contratista,
                })

        return {
            'individual': resultados_individual,
            'grouped_by_sku': list(grupos_by_sku.values()),
        }

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_salida = stock_obj.salida_de_materiales()
    stock_obj.HttpResponse({"data": resp_salida})
