# -*- coding: utf-8 -*-
import sys, simplejson, random
from copy import deepcopy
from pytz import timezone, utc

sys.path.append('/srv/scripts/addons/modules/accesos/items/scripts/Accesos')
from accesos_utils import Accesos as AccesosUtils

sys.path.append('/srv/scripts/addons/modules/stock/items/scripts/Stock')
from stock_utils import Stock as StockUtils

from lkf_addons.addons.stock.app import Stock

class Stock(Stock):
    def __init__(self, settings, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.CATALOG_ID_USUARIOS_ALMACEN = 165977
        self.CATALOG_ID_TRANSPORTISTAS = 164728
        self.CATALOG_ID_SKU = 133015
        self.CATALOG_ID_WH_LOCATIONS = 133014
        self.CATALOG_ID_CONTRATISTAS = 59273

        self.FORM_ID_TRANSFERENCIAS = 166688
        self.FORM_BITACORA_TRANSPORTISTA_ID = 165688

        self.FORM_ID_SALIDAS = 179244

        self.f.update({
            # campos para el Almacen Destino
            "obj_almacen_destino": "6a8e2d1ee08b6d156660ea46",
            "field_nombre_usuario_almacen_destino": "6a8e2de6c6ba624ee2e84aaf",
            "field_id_usuario_almacen_destino": "6a8e2de6c6ba624ee2e84ab0",
            "field_location_almacen_destino": "65c12749cfed7d3a0e1a341b",
            "field_wh_name_almacen_destino": "65bdc71b3e183f49761a33b9",

            # Campos para el proveedor
            "obj_wh_locations": "6824e62e7c8af42c04a73d28",
            "field_wh_name_transportista": "6442e4831198daf81456f274",
            "field_location_transportista": "65ac6fbc070b93e656bd7fbe",


            # Campos para catalogo del transportista
            "obj_ubi_transportista": "6a83326c1aad519fd56c1ca8",
            "field_nombre_transportista": "6a83326c1aad519fd56c1ca9",

            # Campos para el catalogo de contratistas
            "obj_catalog_contratistas": "6a87cc65e0e66741ade17c19",
            "field_nombre_contratista": "5f344a0476c82e1bebc991d7",
            "field_correo_contratista": "5f344a0476c82e1bebc991d8",
            "field_razon_social_contratista": "5f344a0476c82e1bebc991db",

            # Desglose onts
            "field_grp_onts": "6a8f11770f16c3ecf722fd8e",
            "field_no_tarima": "6a8f119594112d8156b9c1b5",
            "field_no_series": "6a8f119594112d8156b9c1b6",

            # grupo de inspecciones
            "field_grp_inspecciones": "6a42a7068dcfbf362329a972",
            "field_fotos_carta_porte": "6a4589aea6675c48f34bb270",
            "field_fotos_factura": "6a4589aea6675c48f34bb271",
            "field_fotos_pedimento": "6a4589aea6675c48f34bb272",
            "field_fotos_orden_compra": "6a8f113d9aaa4417a1e93a96",
            "field_fotos_docs_transportista": "6a8f113d9aaa4417a1e93a97",

            # campos del producto
            "obj_products": "6824e631aa87c4ed5f18bcaa",
            "field_sku": "65dec64a3199f9a040829243",
            "field_product_code": "61ef32bcdf0ec2ba73dec33d",
            "field_product_name": "61ef32bcdf0ec2ba73dec33e",
            "field_unidad_medida": "65dec8423199f9a040829246",

            # Bitacora de eventos
            "field_grp_bitacora": "6a90703a358feca0f30c1ccd",
            "field_fecha_evento": "6a90705ccd63fed585bc6eae",
            "field_tipo_evento": "6a9072b4ee23adcc3f752ea9",
            "field_detalle_evento": "6a90705ccd63fed585bc6eaf",

            'capture_num_serie': '66c75e0c0810217b0b5593ca',
            'tipo_material': '66b10b87a1d4483b5369f409',

            ### Campos para la Transferencia de Material ###
            'field_status_transferencia': '6a31921f07fb9cb5840d1f22',
            'field_transfer_date_from': '6a95bcb168696b683d55112f',
            'field_transfer_date_to': '6a95bcb168696b683d551130',

            'field_grp_missing_report': '6a9989a484b5b4e7dbd27359',
            'field_missing_sku': '6a998aaff3ed00ab7c7aa8ff',
            'field_missing_product_code': '6a998aaff3ed00ab7c7aa900',
            'field_missing_product_name': '6a998aaff3ed00ab7c7aa901',
            'field_missing_quantity': '6a998bd60909c0efcfb730a8',
            'field_missing_note': '6a998bd60909c0efcfb730a9',
            'field_missing_evidence': '6a998bd60909c0efcfb730aa',
            'field_missing_at': '6a998bfbcc618b7776d69e62',
            'field_missing_by': '6a998bfbcc618b7776d69e63',

            'field_box_bool_loose_unit': '6a9a1db1ba0b8de77af0d07c',
            'field_box_id': '6a9a196b8f084d0a0bbe1631',
            'field_box_id_pallet': '6a9a196b8f084d0a0bbe1632',
            'field_box_evidence': '6a9a196b8f084d0a0bbe1633',
            'field_box_position': '6a9a196b8f084d0a0bbe1634',
            'field_box_at': '6a9a196b8f084d0a0bbe1635',
            'field_box_by': '6a9a196b8f084d0a0bbe1636',
            'field_box_sku': '6a9d62f7a53f0c0b2896b7d5',

            'field_serie_box_id': '6a9a181cceff751bc0311a34',
            'field_serie_sku': '6a998d651c34062b10a9765c',
            'field_serie_product_id': '6a998d651c34062b10a9765d',
            'field_serie_product_name': '6a998d651c34062b10a9765e',
            'field_serie_num_serie': '6a8f119594112d8156b9c1b6',
            'field_serie_reacondicionado': '6a9a181cceff751bc0311a31',
            'field_serie_source': '6a9a181cceff751bc0311a32',
            'field_serie_corrected_manually': '6a9a181cceff751bc0311a33',
            'field_serie_evidence': '6a9a19a65258221f2528cf9a',
            'field_serie_position': '6a9a1ceea1016d22900f3fe2',

            'field_grp_tarimas': '6a9a1a739c43aaa5c435d7b5',
            'field_pallet_id': '6a9a1a8831743950c27599f9',
            'field_pallet_count': '6aa8c8e2c744764c9641c1aa',
            'field_boxes_by_pallet': '6aa8c8e2c744764c9641c1ab',
            'field_units_by_box': '6aa8c8e2c744764c9641c1ac',
            'field_grp_boxes': '6a9a1862d30d583834257566',
            'field_sku_pallet_association': '6aa8e7a2f2f9a0546363c048',

            'field_grp_stages': '6a9a36d231743950c2759a2a',
            'field_stage_name': '6a9a392dc725d11c0c356f7b',
            'field_stage_at': '6a9a392dc725d11c0c356f7c',
            'field_stage_by': '6a9a392dc725d11c0c356f7d',
            'field_started_at': '6aafdf4162fba9897ccdc20b',
            'field_started_by': '6aafdf4162fba9897ccdc20c',
            'field_stage_canceled_reason': '6a9a392dc725d11c0c356f7e',

            'field_delivery_estimated_date': '6a95c9b2dec9e900acd6facc',
            'field_delivery_signature': '6a95c9b2dec9e900acd6fad0',
            'field_delivery_signature_at': '6a9a2415d30d58383425756b',
            'field_delivery_ev_transport': '6a95c9b2dec9e900acd6facd',
            'field_delivery_ev_plates': '6a95c9b2dec9e900acd6face',
            'field_delivery_ev_material': '6a95c9b2dec9e900acd6facf',

            'field_actual_quantity': '6ab1df9924a4eecce2eea38e',

            # Ajustes de material en la Transferencia
            'adjust_prev_quantity': '6a9893e11186e5b473216a19',
            'adjust_note': '6a98aa9ad277f6e00aa3534c',
            'adjust_at': '6a9893e11186e5b473216a1d',
            'adjust_by': '6a9893e11186e5b473216a1c',
            'adjust_reason': '6a9893e11186e5b473216a1b',

            ### Campos para la Salida de Material ###
            'recipient_type': '6aafd7a1837118d5d01ddf87',
            "dispatch_info_at": "6aaf5d61052e7f8f50f4b8ce",
            "dispatch_info_by": "6aaf5d61052e7f8f50f4b8cf",
            'delivery_type': '6aaf5f89ea22354af8f15b19',
            'captured_via': '6aaf61a923eb005a57bcc4c7',
            'confirmed_by': '6aaf622685349c45b99e71d1',
            'field_delivery_ev_ine': '6aaf62a8166aa311df7c41b2',
            'field_delivery_ev_license': '6aaf62a8166aa311df7c41b3',

            'recipient_stock': '6aafdb657b4e5542019b3f24',
            'recipient_production': '6aafdb657b4e5542019b3f25',

            'field_supervisor_at': '6aafd1d1857ac404277ab85b',
            'field_supervisor_by': '6aafd1d1857ac404277ab85c',
            'field_supervisor_ev_security_seal': '6aafd1d1857ac404277ab85d',
            'field_supervisor_ev_wrap': '6aafd1d1857ac404277ab85e',
            'field_supervisor_ev_printed_voucher': '6aafd1d1857ac404277ab85f',

            'field_final_delivery_at': '6aafd3d33a6b1b1d8d13b0c6',
            'field_final_delivery_by': '6aafd3d33a6b1b1d8d13b0c7',
            'field_final_delivery_signature': '6aafd3d33a6b1b1d8d13b0c5',
            'field_final_delivery_captured_at': '6ab0ada077d43414d84a1682',
            'field_final_delivery_captured_via': '6ab0ada077d43414d84a1683',
        })

        # Esto lo debería jalar de accesos_utils
        self.bitacora_transportista_fields = {
            'estatus': '6a31921f07fb9cb5840d1f22',
            'fecha_hora_ingreso': '6a3bee0a7829a4ca9572d39e',
            'fecha_hora_descarga': '6a3bee0a7829a4ca9572d39f',
            'fecha_hora_terminado': '6a710409eaef5abc8b1a1a69',

            'grupo_fotos_y_documentos': '6a3bee0a7829a4ca9572d3a0',
            'tipo_de_documento': '6a3bee394a7a0748a6fc9a56',
            'documento': '6a3bee394a7a0748a6fc9a57',
            'is_applicable': '6aa880446724ca7cf3c2091e',

            'num_de_pase': '6a31921f07fb9cb5840d1f23',
            'empresa_transportista': '6a31929d0bf8c5fc715d7424',
            'tipo_de_operacion': '6a31929d0bf8c5fc715d7425',
            'procedencia': '6a3193dccf1326ad4b7a9a52',
            'tipo_de_vehiculo': '6a3193dccf1326ad4b7a9a53',
            'placas_de_vehiculo': '6a31921f07fb9cb5840d1f24',
            'placas_de_vehiculo_tarjeta_circulacion': '6a5018081d7498e16bbb4b75',
            'marca_vehiculo': '6a4415c7b7ce8af39efb3aa8',
            'year_vehiculo': '6a4415c7b7ce8af39efb3aa9',
            'color_vehiculo': '6a4415c7b7ce8af39efb3aaa',
            'num_eco_num_rotulo': '6a3193dccf1326ad4b7a9a56',
            'conductor': '6a3193dccf1326ad4b7a9a57',
            'ayudante': '6a42cd6385b4d5aa41c2a922',
            'num_licencia': '6a3193dccf1326ad4b7a9a58',
            'vigencia_licencia': '6a42e2eab55463ad9f31abf3',
            'rfc_conductor': '6a42e5143f8adeaa55ef9a4a',
            'firma_conductor': '6a3193dccf1326ad4b7a9a5b',
            'anden_asignado': '6a31929d0bf8c5fc715d7427',

            'proveedor_cliente': '6a42dfd48e70db919887e4b0',
            'orden_de_compra': '6a42dfd48e70db919887e4b1',

            'grupo_materiales': '6a42c5e02196461994770602',
            'lugar_material': '6a42c7a7a1555d53d6b9194c', # Opciones: vehiculo, remolque, contenedor
            'no_referencia_material': '6a42c7a7a1555d53d6b9194d',
            'producto_material': '6a44091a4e3983d839de22ee',
            'lote_material': '6a4409523a38bb598a0a18a0',
            'cantidad_material': '6a42c7a7a1555d53d6b91950',
            'cantidad_fisica_material': '6a454fb37ddcb3993dd90107',
            'cantidad_buena_material': '6a6ac379fab960f8931dcc77',
            'cantidad_danada_material': '6a6ac35a71f64d908af42f69',
            'cantidad_faltante_material': '6a7a4ee0e6092a8d37f6d448',
            'peso_material': '6a42c7a7a1555d53d6b91951',
            'volumen_material': '6a42c7a7a1555d53d6b91952',

            'grupo_remolques': '6a31959ed11ece87f2b0052d',
            'tipo_remolque': '6a319693884bec802c94fa44',
            'no_referencia_remolque': '6a443aa0f4bede456259a441',
            'num_sello': '6a319693884bec802c94fa45',
            'num_caja_contenedor': '6a319693884bec802c94fa46',
            'placas_de_caja': '6a319693884bec802c94fa47',
            'color_remolque_contenedor': '6a440b059581538d55b3565e',
            'comentarios': '6a319693884bec802c94fa48',

            'grupo_sellos': '6a42c65c03f125df7ad28601',

            'grupo_desglose_empaque': '6a6a4abe639ed7cad54be377',
            'no_referencia_material_desglose': '6a6a4adc169fc82c5fae8668',
            'nivel_desglose': '6a6a4b64c6fd2eaaf5f8c0b6',
            'tipo_unidad_empaque_desglose': '6a6a4b64c6fd2eaaf5f8c0b7',
            'cantidad_desglose': '6a6a4b64c6fd2eaaf5f8c0b8',
            'cantidad_sugerida_desglose': '6a6a4b64c6fd2eaaf5f8c015',
            'cantidad_acumulada_desglose': '6a6a4b64c6fd2eaaf5f8c0b9',
            "cantidad_unidades_sueltas": "6aa8c9398e3c227d7141c1aa",

            'grupo_inspecciones': '6a42a7068dcfbf362329a972',
            'tipo_inspeccion': '6a42c80b03f125df7ad2862b',
            'url_inspeccion': '6a42a71aec3f7153a3d2aea3',
        }
        
        
        self.kwargs['MODULES'] = self.kwargs.get('MODULES',[])
        if self.__class__.__name__ not in kwargs:
            self.kwargs['MODULES'].append(self.__class__.__name__)

        if not hasattr(self, 'accs'):
            # self.load() solo puede importar lkf_addons.addons.accesos.app.Accesos
            # (el core del addon) y no ve el override de accesos_utils.py, que es
            # donde vive BITACORA_TRANSPORTISTAS. Por eso se instancia manual aqui,
            # igual que hacen los reports que ya cruzan de un modulo a otro.
            self.accs = AccesosUtils(self.settings, sys_argv=self.sys_argv, use_api=self.use_api)
            if 'Accesos' not in self.kwargs['MODULES']:
                self.kwargs['MODULES'].append('Accesos')

        if not hasattr(self, 'stk'):
            # Mismo caso: este override tambien se llama "Stock" (choca con el nombre
            # de esta propia clase), asi que se instancia aparte y se guarda en un
            # atributo con otro nombre para no pisar el self.f / metodos de esta clase.
            self.stk = StockUtils(self.settings, sys_argv=self.sys_argv, use_api=self.use_api)

        self.f.update( self.stk.f )

        # El catalogo de Contratistas (CATALOG_ID_CONTRATISTAS) exige la jwt
        # admin, igual que en consultar_material_estimado.py.
        self.config['JWT_ADMIN'] = self.lkf_api.get_jwt(
            api_key='398bd78880b1675a4a8d06d8a89e712ad9b499fb',
            user='adminpclink@operacionpci.com.mx'
        )
        self.settings.config.update(self.config)

    def testing_stock_ont(self):
        print('+++ Importado desde Accesos = ',self.accs.support_guard)
        print('+++ Importado desde Accesos Utils = ',self.accs.BITACORA_TRANSPORTISTAS)
        print('+++ Importado desde Stock Utils = ',self.stk.NUEVA_VARIABLE)
        stop

    def format_fecha_evento(self, val):
        if not val:
            return val
        if not "T" in val:
            return val

        fecha, hora = val.split("T")
        hora = hora.split(".")[0]
        return f"{fecha} {hora}"

    def format_created_at(self, created_at, str_format='%Y-%m-%d %H:%M:%S'):
        """
        Convierte el `created_at` de un registro (datetime en UTC guardado en
        mongo) a string en hora de America/Monterrey.

        Args:
            created_at (datetime | None): `created_at` del registro.

        Returns:
            str | None
        """
        if not created_at:
            return None
        if not created_at.tzinfo:
            created_at = utc.localize(created_at)
        return created_at.astimezone(timezone('America/Monterrey')).strftime(str_format)

    def find_transportista_catalog(self, nombre_transportista):
        field_map = {
            'nombre_transportista': self.f['field_nombre_transportista']
        }
        return self._find_catalog_record(
            self.CATALOG_ID_TRANSPORTISTAS, self.f['field_nombre_transportista'],
            nombre_transportista, field_map
        )

    def create_transportista_catalog(self, nombre_transportista):
        """
        Crea el registro del transportista en CATALOG_ID_TRANSPORTISTAS
        con su nombre.

        Args:
            nombre_transportista (str): nombre del transportista (carrierName).

        Returns:
            dict | None: {field_nombre_transportista: nombre}, o None si no se
            recibio nombre o no se pudo crear el registro.
        """
        if not nombre_transportista:
            return None

        metadata = self.lkf_api.get_catalog_metadata(catalog_id=self.CATALOG_ID_TRANSPORTISTAS)
        metadata['answers'] = {
            self.f['field_nombre_transportista']: nombre_transportista,
        }
        res = self.lkf_api.post_catalog_answers(metadata, jwt_settings_key='APIKEY_JWT_KEY')
        if res.get('status_code') not in (200, 201, 202):
            print(f"ADVERTENCIA: no se pudo crear el transportista '{nombre_transportista}' en catalogo: {res}")
            return None

        print(f"Se creo el transportista '{nombre_transportista}' en el catalogo {self.CATALOG_ID_TRANSPORTISTAS}")
        return {self.f['field_nombre_transportista']: nombre_transportista}

    def find_contratista_catalog(self, nombre_contratista):
        field_map = {
            'nombre_contratista': self.f['field_nombre_contratista'],
            'correo_contratista': self.f['field_correo_contratista'],
            'razon_social_contratista': self.f['field_razon_social_contratista'],
        }
        # print(f"+++ buscando contratista {nombre_contratista} en catalogo {self.CATALOG_ID_CONTRATISTAS}")
        # print(f"+++ usando field {self.f['field_nombre_contratista']}")
        return self._find_catalog_record(
            self.CATALOG_ID_CONTRATISTAS, self.f['field_nombre_contratista'],
            nombre_contratista, field_map, jwt_settings_key='JWT_ADMIN'
        )

    def find_warehouse_location_catalog(self, location):
        """
        Busca en el catalogo de Warehouse Locations
        (self.stk.WH.WAREHOUSE_LOCATION_ID) el registro cuyo campo Location
        coincida con `location`. Compartido por Transferencia y Salida de
        Material (ambas usan almacen de origen).

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

    def find_warehouse_dest_catalog(self, location, find_by_wh=False):
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

        field_filter = self.stk.WH.f['warehouse_location_dest']
        extra_filter = None
        if find_by_wh:
            field_filter = self.stk.WH.f['warehouse_dest']
            extra_filter = {self.stk.WH.f['warehouse_location_dest']: {"$eq": 'Almacen Fibra'}}

        return self._find_catalog_record(
            self.stk.WH.WAREHOUSE_LOCATION_DEST_ID,
            field_filter,
            location,
            field_map,
            rdOnly_fields=False,
            extra_filter=extra_filter
        )

    def _find_catalog_record(self, catalog_id, filter_field, filter_value, field_map, rdOnly_fields=True, field_as_select=[], extra_filter=None, jwt_settings_key='APIKEY_JWT_KEY'):
        """
        Busca el primer registro de un catalogo cuyo campo `filter_field`
        sea igual a `filter_value`, y devuelve solo los campos indicados
        en `field_map`.

        Args:
            catalog_id (int): id del catalogo a consultar (p.ej. self.CATALOG_ID_USUARIOS_ALMACEN).
            filter_field (str): field_id (ObjectId) por el que se filtra.
            filter_value: valor a buscar en `filter_field`.
            field_map (dict): mapeo {nombre_legible: field_id} con los campos
                del registro que se quieren regresar.

        Returns:
            dict | None: {nombre_legible: valor, ...} del primer registro
            encontrado, o None si no hay coincidencias.
        """

        # print(f"===== ===== consultando el catalogo {catalog_id} {filter_field} {filter_value}")

        mango_query = {
            "selector": {
                "answers": {
                    filter_field: {"$eq": filter_value},
                },
            },
            "limit": 1,
            "skip": 0,
        }
        if extra_filter:
            mango_query['selector']['answers'].update(extra_filter)
        
        record = self.lkf_api.search_catalog(catalog_id, mango_query, jwt_settings_key=jwt_settings_key)

        if not record:
            return None

        row = record[0]

        data_catalog_found = {}
        for key, field_id in field_map.items():
            value = self.unlist( row.get(field_id) )

            if not rdOnly_fields:
                data_catalog_found[field_id] = value
            elif field_as_select:
                if (field_id == filter_field) or (field_id in field_as_select):
                    data_catalog_found[field_id] = value
                else:
                    data_catalog_found[field_id] = [value]
            else:
                data_catalog_found[ field_id ] = value if field_id == filter_field else [value]

        return data_catalog_found

    def find_material_catalog_sku(self, sku, field_as_select=[]):
        field_map = {
            'sku': self.f['field_sku'],
            'product_code': self.f['field_product_code'],
            'product_name': self.f['field_product_name'],
            'unidad_medida': self.f['field_unidad_medida'],
        }

        if field_as_select:
            field_map['capturar_serie'] = self.f['capture_num_serie']
            field_map['tipo_material'] = self.f['tipo_material']

        return self._find_catalog_record(
            self.CATALOG_ID_SKU,
            self.f['field_sku'],
            sku,
            field_map,
            field_as_select=field_as_select
        )

    def _get_item_serials(self, item):
        """
        Junta los numeros de serie capturados para un item (cajas escaneadas +
        la unidad suelta, si aplica).

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

    def build_move_group_lines(self, items=None):
        """
        Arma el grupo repetitivo `move_group` de STOCK_ONE_MANY_ONE a partir
        de los items (por defecto self.data['items']). Se genera una linea por
        numero de serie capturado (lot_number); si un item no trae series
        (material a granel), se genera una sola linea agregada con la cantidad
        recibida.

        Returns:
            list[dict]: filas para el campo self.f['move_group'].
        """
        if items is None:
            items = self.data.get('items', [])

        move_group_lines = []
        for item in items:
            sku = item.get('sku')
            info_catalog_sku = self.find_material_catalog_sku(sku, field_as_select=[self.f['product_code']])
            if not info_catalog_sku:
                print(f"ADVERTENCIA: no se encontro el sku '{sku}' en el catalogo")
                continue

            product_info = {
                self.f['product_code']: self.unlist(info_catalog_sku.get(self.f['product_code'])),
                self.f['sku']: self.unlist(info_catalog_sku.get(self.f['field_sku'])),
                self.f['product_name']: [self.unlist(info_catalog_sku.get(self.f['product_name']))]
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
                product_info[self.f['lot_number']] = "LotePCI001"
                move_group_lines.append({
                    self.CATALOG_INVENTORY_OBJ_ID: product_info,
                    self.f['move_group_qty']: item.get('receivedQuantity') or item.get('expectedQuantity', 0),
                })

        return move_group_lines

    def post_stock_one_many_one(self, answers, device_properties):
        """
        Crea el registro de salida de almacen en STOCK_ONE_MANY_ONE
        (forma 'Salida Multiple Productos a una ubicacion').

        Args:
            answers (dict): respuestas {field_id: valor} a guardar.
            device_properties (dict): process/action/script/function del
                proceso que genera el traspaso.

        Returns:
            dict: respuesta de `lkf_api.post_forms_answers`.
        """
        metadata = self.lkf_api.get_metadata(self.STOCK_ONE_MANY_ONE, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "module": "stock_ont",
                    **device_properties,
                }
            },
            'answers': answers,
        })
        metadata['folio'] = f"TRASPASO-{str(int(random.random() * 1000))}"
        return self.lkf_api.post_forms_answers(metadata)

    def get_damage_reports(self, damage_list):
        if not damage_list:
            return {}

        total_quantity, total_notes, total_evidences = 0, [], []
        for damage in damage_list:
            total_quantity += ( damage.get('quantity') or 0 )
            if damage.get('note'):
                total_notes.append(damage['note'])
            if damage.get('evidence'):
                total_evidences.extend(damage.get('evidence', []))

        damage_report = {
            '6a8f105cf579313536b1984d': total_quantity,
            '6a8f10acae2709fa995fc6be': self.list_to_str(total_notes)
        }

        if total_evidences:
            damage_report['6a8f10acae2709fa995fc6bd'] = total_evidences

        # print(simplejson.dumps(damage_report, indent=4))
        # stop

        return damage_report

    def get_adjustment_data(self, adjustment):
        if not adjustment or not isinstance( adjustment, dict ):
            return {}

        return {
            self.f['adjust_prev_quantity']: adjustment.get('previousQuantity'),
            self.f['adjust_note']: adjustment.get('note', ''),
            self.f['adjust_at']: self.format_fecha_evento(adjustment.get('adjustedAt')),
            self.f['adjust_by']: adjustment.get('adjustedBy', ''),
            self.f['adjust_reason']: adjustment.get('reason', '').replace('_', ' ').title(),
        }

    def make_missing_report(self, sku_data, list_missing):
        data_product_missing = {
            self.f['field_missing_sku']: self.unlist( sku_data.get( self.f['field_sku'] ) ),
            self.f['field_missing_product_code']: self.unlist( sku_data.get( self.f['field_product_code'] ) ),
            self.f['field_missing_product_name']: self.unlist( sku_data.get( self.f['field_product_name'] ) ),
        }
        
        missing_products = []
        for missing in list_missing:
            missing_info = deepcopy( data_product_missing )
            missing_info.update({
                self.f['field_missing_quantity']: missing.get('quantity'),
                self.f['field_missing_note']: missing.get('note'),
                self.f['field_missing_evidence']: missing.get('evidence'),
                self.f['field_missing_at']: self.format_fecha_evento( missing.get('createdAt') ),
                self.f['field_missing_by']: missing.get('createdBy'),
            })
            missing_products.append(missing_info)

        return missing_products

    def format_bool_value(self, value):
        if value:
            return 'sí'
        return 'no'

    def make_series_data(self, box_id, sku_data, list_serials):
        if not list_serials:
            return []
        
        data_num_serie = {
            self.f['field_serie_sku']: self.unlist( sku_data.get( self.f['field_sku'] ) ),
            self.f['field_serie_product_id']: self.unlist( sku_data.get( self.f['field_product_code'] ) ),
            self.f['field_serie_product_name']: self.unlist( sku_data.get( self.f['field_product_name'] ) ),
        }

        if box_id:
            data_num_serie[ self.f['field_serie_box_id'] ] = box_id

        group_series = []
        for serial in list_serials:
            serial_info = deepcopy(data_num_serie)
            serial_info.update({
                self.f['field_serie_num_serie']: serial.get('value'),
                self.f['field_serie_reacondicionado']: self.format_bool_value(serial.get('reacondicionado')),
                self.f['field_serie_source']: serial.get('source'),
                self.f['field_serie_corrected_manually']: self.format_bool_value(serial.get('correctedManually')),
                # self.f['field_serie_evidence']: 
                # self.f['field_serie_position']: 
            })
            group_series.append(serial_info)

        return group_series

    def get_materials_scan(self, sku_data, list_boxes):
        if not list_boxes:
            return [], set(), []

        list_fields_box = []
        pallets = set()
        list_series_box = []
        sku_producto = self.unlist( sku_data.get(self.f['field_sku']) ) if sku_data else None
        for box in list_boxes:
            box_id = box.get('id')
            pallet_id = box.get('groupId')

            data_box = {}

            # Si no hay id de caja ni de tarima, se entiende que es una unidad suelta.
            # No trae un id propio, asi que se usa el sku del producto como box_id
            # sintetico: permite ligar sus series (field_serie_box_id) de vuelta a
            # esta caja/producto al consultar, igual que se hace con cajas normales.
            if not box_id and not pallet_id:
                data_box[ self.f['field_box_bool_loose_unit'] ] = 'sí'
                box_id = sku_producto

            data_box[self.f['field_box_id']] = box_id
            data_box[self.f['field_box_id_pallet']] = pallet_id
            # Se guarda el sku del producto en la propia caja (tenga o no series)
            # para poder reagruparla de vuelta a su item al consultar, sin depender
            # de que tenga algun serial asociado.
            data_box[self.f['field_box_sku']] = sku_producto
            data_box[self.f['field_box_evidence']] = box.get('labelPhotos', [])
            data_box[self.f['field_box_position']] = box.get('position')
            data_box[self.f['field_box_at']] = self.format_fecha_evento( box.get('scannedAt') )
            data_box[self.f['field_box_by']] = box.get('scannedBy')
            list_fields_box.append(data_box)
            
            if pallet_id:
                pallets.add(pallet_id)

            series = self.make_series_data( box_id, sku_data, box.get('serials') )
            list_series_box.extend(series)

        return list_fields_box, pallets, list_series_box

    def build_grp_materiales(self, materiales_data, is_transfer=False):
        """
        Arma el desglose de materiales recibidos, resolviendo cada SKU
        contra el catalogo de productos. Si un SKU no se encuentra,
        imprime una advertencia y deja el producto como None.

        Args:
            materiales_data (list[dict]): seccion `items` del payload, cada
            uno con `sku`, `expectedQuantity`, `receivedQuantity` y, si
            `is_transfer`, `distribution` ({'palletGroups': [{'id',
            'palletCount', 'boxesPerPallet', 'unitsPerBox'}], 'looseUnits'}).

        Returns:
            list[dict]: filas para el campo `grupo_desglose_empaque`. Si
            `is_transfer`, regresa ademas (grp_boxes, grp_pallets, grp_series,
            grp_missing), donde grp_pallets es {pallet_id: {palletCount,
            boxesPerPallet, unitsPerBox, sku}}; `sku` es el sku normalizado
            del catalogo del item al que pertenece la tarima, para poder
            reagruparla de vuelta a su item al consultar.
        """
        grp_materiales, grp_missing = [], []
        grp_boxes, grp_pallets, grp_series = [], {}, []
        for data_material in materiales_data:
            info_catalog_sku = self.find_material_catalog_sku( data_material.get('sku') )
            if not info_catalog_sku:
                print(f"ADVERTENCIA: no se encontro el sku '{data_material.get('sku')}' en el catalogo")
            info_material = {
                self.f['obj_products']: info_catalog_sku
            }
            info_material[ self.bitacora_transportista_fields['cantidad_desglose'] ] = data_material.get('expectedQuantity', 0)
            info_material[ self.bitacora_transportista_fields['cantidad_sugerida_desglose'] ] = data_material.get('suggestedQuantity', 0)
            info_material[ self.bitacora_transportista_fields['cantidad_acumulada_desglose'] ] = data_material.get('receivedQuantity', 0)
            info_material.update( self.get_damage_reports( data_material.get('damageReports', []) ) )

            # Datos que aplican para el proceso de Transferencias
            if is_transfer:
                # Se integran los ajustes
                info_material.update( self.get_adjustment_data( data_material.get('adjustment') ) )
                # se obtienen los elementos faltantes en la recepcion
                missing_reports = self.make_missing_report( info_catalog_sku, data_material.get('missingReports', []) )
                grp_missing.extend(missing_reports)
                # se obtienen las Tarimas, Cajas y Núms. de Serie
                boxes, pallets, series = self.get_materials_scan( info_catalog_sku, data_material.get('boxScans') )
                # Puede ser que también haya Unidades sueltan, por tanto hay que integrarlas al grupo de series
                loose_units = data_material.get('looseUnitScan')
                if loose_units:
                    boxes_loose_units, _, loose_units_found = self.get_materials_scan( info_catalog_sku, [loose_units] )
                    series.extend(loose_units_found)
                    boxes.extend(boxes_loose_units)

                # Se acumulan (no se reasignan) para no perder cajas/series de items anteriores
                grp_boxes += boxes
                grp_series += series

                # sku normalizado del catalogo, igual al que se guarda en field_box_sku,
                # para poder reagrupar las tarimas de vuelta a su item al consultar.
                sku_producto = self.unlist( info_catalog_sku.get(self.f['field_sku']) ) if info_catalog_sku else None
                for pallet_id in pallets:
                    grp_pallets.setdefault(pallet_id, {'sku': sku_producto})

                # Distribucion planeada del item: unidades sueltas y tarimas
                # (palletCount/boxesPerPallet/unitsPerBox por cada groupId).
                distribution = data_material.get('distribution') or {}
                info_material[ self.bitacora_transportista_fields['cantidad_unidades_sueltas'] ] = distribution.get('looseUnits', 0)
                for pallet_group in distribution.get('palletGroups', []):
                    pallet_id = pallet_group.get('id')
                    if not pallet_id:
                        continue
                    grp_pallets[pallet_id] = {
                        'palletCount': pallet_group.get('palletCount'),
                        'boxesPerPallet': pallet_group.get('boxesPerPallet'),
                        'unitsPerBox': pallet_group.get('unitsPerBox'),
                        'sku': sku_producto,
                    }

            grp_materiales.append(info_material)

        if is_transfer:
            return grp_materiales, grp_boxes, grp_pallets, grp_series, grp_missing

        return grp_materiales

    def build_grp_tarimas(self, grp_pallets):
        """
        Arma el grupo repetitivo de Tarimas (field_grp_tarimas) a partir de
        los pallets acumulados en build_grp_materiales, con la informacion
        de distribucion (palletCount/boxesPerPallet/unitsPerBox) que venga
        en items[].distribution.palletGroups. Cada tarima guarda ademas el
        sku del item al que pertenece (field_sku_pallet_association), para
        poder reagruparla de vuelta a su item al consultar.

        Args:
            grp_pallets (dict): {pallet_id: {palletCount, boxesPerPallet,
            unitsPerBox, sku}}, tal como lo regresa build_grp_materiales.

        Returns:
            list[dict]: filas para el campo `field_grp_tarimas`.
        """
        return [{
            self.f['field_pallet_id']: pallet_id,
            self.f['field_pallet_count']: info.get('palletCount'),
            self.f['field_boxes_by_pallet']: info.get('boxesPerPallet'),
            self.f['field_units_by_box']: info.get('unitsPerBox'),
            self.f['field_sku_pallet_association']: info.get('sku'),
        } for pallet_id, info in grp_pallets.items()]

    def build_grp_bitacora(self, eventos):
        """
        Arma el grupo repetitivo de eventos de la bitacora (fecha, tipo
        y detalle de cada evento registrado durante el recibo).

        Args:
            eventos (list[dict]): seccion `events` del payload, cada uno
            con `at`, `type` y `detail`.

        Returns:
            list[dict]: filas para el campo `field_grp_bitacora`.
        """
        grp_bitacora = []
        for evento in eventos:
            grp_bitacora.append({
                self.f['field_fecha_evento']: self.format_fecha_evento( evento.get('at') ),
                self.f['field_tipo_evento']: evento.get('type', '').replace('_', ' ').title(),
                self.f['field_detalle_evento']: evento.get('detail')
            })
        return grp_bitacora