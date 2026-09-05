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
        return self.lkf_api.post_forms_answers(metadata)

    def find_warehouse_location_catalog(self, location):
        """
        Busca en el catalogo de Warehouse Locations
        (self.stk.WH.WAREHOUSE_LOCATION_ID) el registro cuyo campo Location
        coincida con `location`.

        Args:
            location (str): valor a buscar en el campo Location del catalogo.

        Returns:
            dict | None: {location, warehouse}, o None si no se encontro.
        """
        field_map = {
            'location': self.stk.WH.f['warehouse_location'],
            'warehouse': self.stk.WH.f['warehouse'],
        }
        return self._find_catalog_record(
            self.stk.WH.WAREHOUSE_LOCATION_ID,
            self.stk.WH.f['warehouse_location'],
            location,
            field_map,
            rdOnly_fields=False
        )

    def find_warehouse_dest_catalog(self, location):
        """
        Busca en el catalogo de Warehouse Dest (self.stk.WH.WAREHOUSE_LOCATION_DEST_ID)
        el registro cuyo campo Location coincida con `location`.

        Args:
            location (str): valor a buscar en el campo Location del catalogo.

        Returns:
            dict | None: {location}, o None si no se encontro.
        """
        field_map = {
            'location': self.stk.WH.f['warehouse_location_dest'],
            'warehouse': self.stk.WH.f['warehouse_dest'],
        }
        return self._find_catalog_record(
            self.stk.WH.WAREHOUSE_LOCATION_DEST_ID,
            self.stk.WH.f['warehouse_location_dest'],
            location,
            field_map,
            rdOnly_fields=False
        )

    def build_grp_stages(self):
        data_grp_stages = []
        for stage, name_stage in self.map_stages.items():
            data_stage = self.data.get(stage, {})
            
            if not data_stage:
                continue
            
            stage_at = data_stage.get('cancelledAt' if stage == 'cancellation' else 'confirmedAt')
            stage_by = data_stage.get('cancelledBy' if stage == 'cancellation' else 'confirmedBy')
            reason = data_stage.get('reason')

            if not stage_at and not stage_by:
                continue

            data_grp_stages.append({
                self.f['field_stage_name'] : name_stage,
                self.f['field_stage_at'] : self.format_fecha_evento(stage_at),
                self.f['field_stage_by'] : stage_by,
                self.f['field_stage_canceled_reason'] : reason,
            })
        return data_grp_stages

    def tranferencia_de_materiales(self):
        materiales_data = self.data.get('items', [])
        grp_materiales, grp_boxes, grp_pallets, grp_series = self.build_grp_materiales(materiales_data, is_transfer=True)

        # print("\n+++ grp_materiales =",grp_materiales)
        # print("\n+++ grp_boxes =",grp_boxes)
        # print("\n+++ grp_pallets =",grp_pallets)
        # print("\n+++ grp_series =",grp_series)

        # print('... ... .... ... originWarehouse= ', self.find_warehouse_location_catalog(self.data.get('originWarehouse')))
        # print('... ... .... ... destinationWarehouse= ', self.find_warehouse_dest_catalog(self.data.get('destinationWarehouse')))
        # stop

        answers_transferencia = {
            self.f['field_status_transferencia']: self.map_transfer_stages.get(self.data.get('stage')),
            self.f['field_transfer_date_from']: self.data.get('analysisRange', {}).get('startDate'),
            self.f['field_transfer_date_to']: self.data.get('analysisRange', {}).get('endDate'),
            self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID: self.find_warehouse_location_catalog(self.data.get('originWarehouse')),
            self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID: self.find_warehouse_dest_catalog(self.data.get('destinationWarehouse')),
            self.bitacora_transportista_fields['grupo_desglose_empaque']: grp_materiales,
            self.f['field_grp_tarimas']: [{self.f['field_pallet_id']: pall} for pall in grp_pallets],
            self.f['field_grp_boxes']: grp_boxes,
            self.f['field_grp_onts']: grp_series,
            self.f['field_grp_bitacora']: self.build_grp_bitacora(self.data.get('events', [])),
            self.f['field_grp_stages']: self.build_grp_stages(),
        }

        # print('answers_transferencia =',simplejson.dumps(answers_transferencia, indent=4))

        return self.post_transferencia_materiales(answers_transferencia)

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_transferencia = stock_obj.tranferencia_de_materiales()
    stock_obj.HttpResponse({"data": resp_transferencia})