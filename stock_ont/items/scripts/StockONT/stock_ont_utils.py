# -*- coding: utf-8 -*-
import sys, simplejson
from lkf_addons.addons.stock.app import Stock

class Stock(Stock):
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.CATALOG_ID_USUARIOS_ALMACEN = 165977
        self.CATALOG_ID_TRANSPORTISTAS = 164728
        self.CATALOG_ID_SKU = 133015
        self.CATALOG_ID_WH_LOCATIONS = 133014

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

    def testing_stock_ont(self):
        print('..... hola mundo .....')