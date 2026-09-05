# -*- coding: utf-8 -*-
import sys, simplejson
from copy import deepcopy

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

        self.FORM_ID_TRANSFERENCIAS = 166688

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
            'field_grp_boxes': '6a9a1862d30d583834257566',

            'field_grp_stages': '6a9a36d231743950c2759a2a',
            'field_stage_name': '6a9a392dc725d11c0c356f7b',
            'field_stage_at': '6a9a392dc725d11c0c356f7c',
            'field_stage_by': '6a9a392dc725d11c0c356f7d',
            'field_stage_canceled_reason': '6a9a392dc725d11c0c356f7e',

            # Ajustes de material en la Transferencia
            'adjust_prev_quantity': '6a9893e11186e5b473216a19',
            'adjust_note': '6a98aa9ad277f6e00aa3534c',
            'adjust_at': '6a9893e11186e5b473216a1d',
            'adjust_by': '6a9893e11186e5b473216a1c',
            'adjust_reason': '6a9893e11186e5b473216a1b',
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
            'cantidad_acumulada_desglose': '6a6a4b64c6fd2eaaf5f8c0b9',

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

    def _find_catalog_record(self, catalog_id, filter_field, filter_value, field_map, rdOnly_fields=True, field_as_select=[]):
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
        record = self.lkf_api.search_catalog(catalog_id, mango_query)

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
            return []

        list_fields_box = []
        pallets = set()
        list_series_box = []
        for box in list_boxes:
            box_id = box.get('id')
            pallet_id = box.get('groupId')

            data_box = {}

            # Si no hay id de caja ni de tarima, se entiende que es una unidad suelta
            if not box_id and not pallet_id:
                data_box[ self.f['field_box_bool_loose_unit'] ] = 'sí'
            
            data_box[self.f['field_box_id']] = box_id
            data_box[self.f['field_box_id_pallet']] = pallet_id
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
            uno con `sku`, `expectedQuantity` y `receivedQuantity`.

        Returns:
            list[dict]: filas para el campo `grupo_desglose_empaque`.
        """
        grp_materiales, grp_missing = [], []
        for data_material in materiales_data:
            info_catalog_sku = self.find_material_catalog_sku( data_material.get('sku') )
            if not info_catalog_sku:
                print(f"ADVERTENCIA: no se encontro el sku '{data_material.get('sku')}' en el catalogo")
            info_material = {
                self.f['obj_products']: info_catalog_sku
            }
            info_material[ self.bitacora_transportista_fields['cantidad_desglose'] ] = data_material.get('expectedQuantity', 0)
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
                grp_boxes, grp_pallets, grp_series = self.get_materials_scan( info_catalog_sku, data_material.get('boxScans') )
                # Puede ser que también haya Unidades sueltan, por tanto hay que integrarlas al grupo de series
                loose_units = data_material.get('looseUnitScan')
                if loose_units:
                    boxes_loose_units, _, loose_units_found = self.get_materials_scan( info_catalog_sku, [loose_units] )
                    grp_series.extend(loose_units_found)
                    grp_boxes.extend(boxes_loose_units)

            grp_materiales.append(info_material)
        
        if is_transfer:
            return grp_materiales, grp_boxes, grp_pallets, grp_series

        return grp_materiales

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