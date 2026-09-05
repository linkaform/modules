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
        self.provider_name = "Telmex" # Por ahora fijo el nombre del Proveedor
        self.f_bitacora = self.bitacora_transportista_fields
        self.FORM_BITACORA_TRANSPORTISTA_ID = 165688

    def find_warehouse_catalog_users(self, nombre_usuario):
        """
        Busca en CATALOG_ID_USUARIOS_ALMACEN al usuario de almacen destino
        por su nombre, para obtener su id, warehouse y location.

        Args:
            nombre_usuario (str): nombre del usuario a buscar.

        Returns:
            dict | None: {id_usuario, nombre_usuario, location, wh_name},
            o None si no se encontro el usuario.
        """
        field_map = {
            'id_usuario': self.f['field_id_usuario_almacen_destino'],
            'nombre_usuario': self.f['field_nombre_usuario_almacen_destino'],
            'location': self.f['field_location_almacen_destino'],
            'wh_name': self.f['field_wh_name_almacen_destino'],
        }
        return self._find_catalog_record(
            self.CATALOG_ID_USUARIOS_ALMACEN,
            self.f['field_nombre_usuario_almacen_destino'],
            nombre_usuario,
            field_map,
        )

    def find_proveedores_catalog_users(self, nombre_proveedor):
        """
        Busca en CATALOG_ID_WH_LOCATIONS el almacen de origen (proveedor)
        por su nombre, para obtener su warehouse y location.

        Args:
            nombre_proveedor (str): nombre del proveedor a buscar.

        Returns:
            dict | None: {location, wh_name},
            o None si no se encontro el proveedor.
        """
        field_map = {
            'location': self.f['field_location_transportista'],
            'wh_name': self.f['field_wh_name_transportista'],
        }
        return self._find_catalog_record(
            self.CATALOG_ID_WH_LOCATIONS,
            self.f['field_location_transportista'],
            nombre_proveedor,
            field_map,
            rdOnly_fields=False
        )

    def find_transportista_catalog(self, nombre_transportista):
        field_map = {
            'nombre_transportista': self.f['field_nombre_transportista']
        }
        return self._find_catalog_record(
            self.CATALOG_ID_TRANSPORTISTAS, self.f['field_nombre_transportista'],
            nombre_transportista, field_map
        )

    def find_catalogs_bitacora_transportista(self, delivery_data):
        """
        Busca en catalogo el almacen destino, el almacen origen y el
        transportista a partir de los datos de entrega. Si alguno no
        se encuentra, imprime una advertencia pero no interrumpe el flujo.

        Args:
            delivery_data (dict): seccion `delivery` del payload recibido.

        Returns:
            tuple: (info_catalog_almacen_destino, info_catalog_almacen_origen,
            info_catalog_transportista), cada uno dict | None.
        """
        info_catalog_almacen_destino = self.find_warehouse_catalog_users(delivery_data.get('requestedBy'))
        info_catalog_almacen_origen = self.find_proveedores_catalog_users(delivery_data.get('providerName'))
        info_catalog_transportista = self.find_transportista_catalog(delivery_data.get('carrierName'))

        if not info_catalog_almacen_destino:
            print(f"ADVERTENCIA: no se encontro almacen destino para requestedBy='{delivery_data.get('requestedBy')}'")
        if not info_catalog_almacen_origen:
            print(f"ADVERTENCIA: no se encontro almacen origen para providerName='{delivery_data.get('providerName')}'")
        if not info_catalog_transportista:
            print(f"ADVERTENCIA: no se encontro transportista para carrierName='{delivery_data.get('carrierName')}'")

        return info_catalog_almacen_destino, info_catalog_almacen_origen, info_catalog_transportista

    def validate_delivery_date(self, delivery_data):
        """
        Valida que venga la fecha de entrega. Si falta, lanza LKFException
        con un mensaje de error asociado al campo `fecha_hora_ingreso`.

        Args:
            delivery_data (dict): seccion `delivery` del payload recibido.

        Returns:
            str: valor de `deliveryDate`.
        """
        delivery_date = delivery_data.get('deliveryDate')
        if not delivery_date:
            self.LKFException(simplejson.dumps({
                self.f_bitacora['fecha_hora_ingreso']: {
                    "msg": ["No se recibio la fecha de entrega (delivery.deliveryDate)"],
                    "label": "Fecha de entrega",
                    "error": [],
                }
            }))
        return delivery_date

    def build_grp_inspecciones(self, documents_data):
        """
        Arma el grupo de fotos/documentos de inspeccion (carta porte,
        factura, pedimento, orden de compra y documentos del transportista).

        Args:
            documents_data (dict): seccion `materialDocuments` del payload.

        Returns:
            list[dict]: lista de un solo elemento con el grupo armado, en
            el formato que espera el campo `field_grp_inspecciones`.
        """
        return [{
            self.f['field_fotos_carta_porte'] : documents_data.get('cartaPorte', []),
            self.f['field_fotos_factura'] : documents_data.get('factura', []),
            self.f['field_fotos_pedimento'] : documents_data.get('pedimento', []),
            self.f['field_fotos_orden_compra'] : documents_data.get('ordenCompra', []),
            self.f['field_fotos_docs_transportista'] : self.data.get('carrierDocuments', []),
        }]

    def build_grp_evidencias(self, evidence_data):
        """
        Arma el grupo repetitivo de fotos y documentos de evidencia
        (camion cerrado/abierto/vacio, sello y candado del contenedor).
        Los tipos de evidencia sin mapeo o sin archivo adjunto se omiten,
        imprimiendo una advertencia en el primer caso.

        Args:
            evidence_data (dict): seccion `sharedEvidence` del payload,
            con la forma {nombre_evidencia: {'evidence': [...]}}.

        Returns:
            list[dict]: filas para el campo `grupo_fotos_y_documentos`.
        """
        map_evidence = {
            'truckClosed': 'Camión Cerrado',
            'containerSeal': 'Sello del Contenedor',
            'containerLock': 'Candado del Contenedor',
            'truckOpen': 'Camión Abierto',
            'truckEmpty': 'Camión Vacío',
        }

        grp_evidencias = []
        for name_evidencia, data_evidencia in evidence_data.items():
            if not data_evidencia.get('evidence'):
                continue
            tipo_documento = map_evidence.get(name_evidencia)
            if not tipo_documento:
                print(f"ADVERTENCIA: tipo de evidencia desconocido '{name_evidencia}', se omite")
                continue
            grp_evidencias.append({
                self.f_bitacora['tipo_de_documento']: tipo_documento,
                self.f_bitacora['documento']: data_evidencia['evidence']
            })
        return grp_evidencias

    def build_move_group_recepcion(self, materiales_data):
        """
        Arma el grupo repetitivo de materiales para la forma Recepcion de
        Materiales de Proveedor, resolviendo cada SKU contra el catalogo de
        productos. Si un SKU no se encuentra, imprime una advertencia y deja
        el producto como None.

        Args:
            materiales_data (list[dict]): seccion `items` del payload, cada
            uno con `sku` y `receivedQuantity`.

        Returns:
            list[dict]: filas para el campo `move_group`.
        """
        move_group = []
        field_as_select = [ self.f['field_product_code'] ]
        for data_material in materiales_data:
            series = data_material.get('looseUnitScan', {}).get('serials', [])

            info_catalog_sku = self.find_material_catalog_sku( data_material.get('sku'), field_as_select=field_as_select )
            if not info_catalog_sku:
                print(f"ADVERTENCIA: no se encontro el sku '{data_material.get('sku')}' en el catalogo")
            
            data_set_material = {
                self.f['obj_products']: info_catalog_sku,
                self.f['lot_number']: 'LotePCI001',
                self.f['move_group_qty']: data_material.get('receivedQuantity', 0),
                self.f['inv_adjust_grp_status']: 'todo',
            }

            if series:
                for serie in series:
                    serie_set_material = deepcopy(data_set_material)
                    serie_set_material[ self.f['lot_number'] ] = serie
                    serie_set_material[ self.f['move_group_qty'] ] = 1
                    move_group.append(serie_set_material)
            else:
                move_group.append(data_set_material)

        return move_group

    def post_recepcion_materiales_proveedor(self, answers):
        """
        Crea el registro en la forma Recepcion de Materiales de Proveedor
        (form_id STOCK_IN_ONE_MANY_ONE) con las respuestas ya armadas.

        Args:
            answers (dict): respuestas {field_id: valor} a guardar.

        Returns:
            dict: respuesta de `lkf_api.post_forms_answers`.
        """
        metadata = self.lkf_api.get_metadata(self.STOCK_IN_ONE_MANY_ONE, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Recibo de Materiales",
                    "action": "Crear Recepcion de Materiales de Proveedor",
                    "from_folio": self.folio,
                    "script": "recibo_de_materiales.py",
                    "module": "stock_ont",
                    "function": "create_record_recepcion_materiales_proveedor",
                }
            },
            'answers': answers,
        })
        return self.lkf_api.post_forms_answers(metadata)

    def create_record_recepcion_materiales_proveedor(self):
        """
        Arma las respuestas de la Recepcion de Materiales de Proveedor a
        partir de `self.data` (almacenes y materiales recibidos) y crea el
        registro en LKF.

        Returns:
            dict: respuesta de `lkf_api.post_forms_answers`.
        """
        delivery_data = self.data.get('delivery', {})
        materiales_data = self.data.get('items', [])

        info_catalog_almacen_destino, info_catalog_almacen_origen, _ = \
            self.find_catalogs_bitacora_transportista(delivery_data)
        delivery_date = self.validate_delivery_date(delivery_data)

        # print("--- self.f['obj_almacen_destino'] =",self.f['obj_almacen_destino'])
        # print("--- self.f['obj_wh_locations'] =",self.f['obj_wh_locations'])

        answers = {
            self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID: {
                self.f['field_location_almacen_destino']: self.unlist(
                    info_catalog_almacen_destino.get( self.f['field_location_almacen_destino'] )
                ),
                self.f['field_wh_name_almacen_destino']: self.unlist(
                    info_catalog_almacen_destino.get( self.f['field_wh_name_almacen_destino'] )
                ),
            },
            # self.f['obj_almacen_destino'] : info_catalog_almacen_destino,
            self.f['obj_wh_locations'] : info_catalog_almacen_origen,
            self.f['move_group'] : self.build_move_group_recepcion(materiales_data),
            self.f['fecha_recepcion'] : f"{delivery_date} 00:00:00",
            self.f['stock_status'] : 'to_do',
            self.f['stock_move_comments'] : 'Esto es una prueba',
            self.f['evidencia'] : [{
                "file_name":"cartaPorte.jpg",
                "file_url":"https://f001.backblazeb2.com/file/slimey-linkaform/public-client-17860/165688/6a4589aea6675c48f34bb270/6a9089b0d5fce2b5eac47a7f.png"
            }]
        }

        # print('answers recepcion =', simplejson.dumps(answers, indent=4))
        # stop

        return self.post_recepcion_materiales_proveedor(answers)

    def post_bitacora_transportista(self, answers):
        """
        Crea el registro en la forma Bitacora de Transportistas
        (form_id FORM_BITACORA_TRANSPORTISTA_ID) con las respuestas ya armadas.

        Args:
            answers (dict): respuestas {field_id: valor} a guardar.

        Returns:
            dict: respuesta de `lkf_api.post_forms_answers`.
        """
        metadata = self.lkf_api.get_metadata(self.FORM_BITACORA_TRANSPORTISTA_ID, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Recibo de Materiales",
                    "action": "Crear Bitacora de Transportista",
                    "from_folio": self.folio,
                    "script": "recibo_de_materiales.py",
                    "module": "stock_ont",
                    "function": "create_record_bitacora_transportista",
                }
            },
            'answers': answers,
        })
        return self.lkf_api.post_forms_answers(metadata)

    def create_record_bitacora_transportista(self):
        """
        Arma las respuestas de la Bitacora de Transportistas a partir de
        `self.data` (almacenes, transportista, inspecciones, evidencias,
        materiales, firma y eventos) y crea el registro en LKF.

        Returns:
            dict: respuesta de `lkf_api.post_forms_answers`.
        """
        delivery_data = self.data.get('delivery', {})
        documents_data = self.data.get('materialDocuments', {})
        evidence_data = self.data.get('sharedEvidence', {})
        materiales_data = self.data.get('items', [])

        info_catalog_almacen_destino, info_catalog_almacen_origen, info_catalog_transportista = \
            self.find_catalogs_bitacora_transportista(delivery_data)
        delivery_date = self.validate_delivery_date(delivery_data)

        answers = {
            self.f['obj_almacen_destino'] : info_catalog_almacen_destino,
            self.f['obj_wh_locations'] : info_catalog_almacen_origen,
            self.f['obj_ubi_transportista'] : info_catalog_transportista,
            self.f_bitacora['fecha_hora_ingreso'] : f"{delivery_date} 00:00:00",
            self.f['field_grp_inspecciones']: self.build_grp_inspecciones(documents_data),
            self.f_bitacora['grupo_fotos_y_documentos']: self.build_grp_evidencias(evidence_data),
            self.f_bitacora['grupo_desglose_empaque']: self.build_grp_materiales(materiales_data),
            self.f_bitacora['firma_conductor']: self.data.get('signature', {}).get('signatureDataUrl', {}),
            self.f['field_grp_bitacora']: self.build_grp_bitacora(self.data.get('events', [])),
        }

        return self.post_bitacora_transportista(answers)

    def recibo_de_materiales(self):
        """
        Se va a ejecutar el proceso de Recibo de Materiales, se creará el registro
        en la forma Bitacora de Transportistas y además se realizará la recepción
        en la forma Recepcion de Materiales de Proveedor
        """
        # resp_bitacora_transportista = {'status_code': 201}
        resp_bitacora_transportista = self.create_record_bitacora_transportista()
        print("+++ +++ +++ resp_bitacora_transportista =",resp_bitacora_transportista)
        if resp_bitacora_transportista.get('status_code') == 201:
            resp_recepcion_materiales = self.create_record_recepcion_materiales_proveedor()
            print("+++ +++ +++ resp_recepcion_materiales =",resp_recepcion_materiales)
            return {
                'bitacora_transportista': resp_bitacora_transportista,
                'recepcion_materiales': resp_recepcion_materiales,
            }
        return {'bitacora_transportista': resp_bitacora_transportista}

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_bitacora_recepcion = stock_obj.recibo_de_materiales()
    stock_obj.HttpResponse({"data": resp_bitacora_recepcion})