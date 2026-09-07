# -*- coding: utf-8 -*-
from os import access

import pytz
import sys, simplejson, json, pytz, base64, requests

from datetime import datetime, timedelta, date
from math import ceil
from bson import ObjectId
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor, as_completed

from linkaform_api import base, generar_qr
from lkf_addons.addons.accesos.app import Accesos

class Accesos(Accesos):
    print('Entra a accesos_utils')

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.f.update({
            'duracion_rondin':'6639b47565d8e5c06fe97cf3',
            'duracion_traslado_area':'6760a9581e31b10a38a22f1f',
            'fecha_inspeccion_area':'6760a908a43b1b0e41abad6b',
            'fecha_inicio_rondin':'6760a8e68cef14ecd7f8b6fe',
            'status_user':'6639b2744bb44059fc59eb62',
            'nombre_recorrido':'6644fb97e14dcb705407e0ef',
            
            'option_checkin': '663bffc28d00553254f274e0',
            'image_checkin': '6855e761adab5d93274da7d7',
            'comment_checkout': '68798dd1205f333d8f53a1c7',
            'start_shift': '6879828d0234f02649cad390',
            'end_shift': '6879828d0234f02649cad391',
            'foto_end': '6879823d856f580aa0e05a3b',
            
            'dias_libres': '68bb20095035e61c5745de05',
            'nombre_horario': '68b6427cc8f94827ebfed695',
            'hora_entrada': '68b6427cc8f94827ebfed696',
            'hora_salida': '68b6427cc8f94827ebfed697',
            'tolerancia_retardo': '68b6427cc8f94827ebfed698',
            'retardo_maximo': '68b642e2bc17e2713cabe019',
            'grupo_turnos': '68b6427cc8f94827ebfed699',
            'horas_trabajadas': '68d6b0d5f7865907a86c37d7',
            'status_turn': '68d5bbb57691dec5a7640358',
            
            'tipo_guardia': '68acee270f2af5e173b7f92e',
            'nombre_guardia_suplente': '68acb67685a044b5fdd869b2',
            'estatus_guardia': '663bffc28d00553254f274e0',
            'foto_inicio_turno': '6855e761adab5d93274da7d7',
            'foto_cierre_turno': '6879823d856f580aa0e05a3b',
            'fecha_inicio_turno': '6879828d0234f02649cad390',
            'fecha_cierre_turno': '6879828d0234f02649cad391',
            'comentario_inicio_turno': '66a5b9bed0c44910177eb724',
            'comentario_cierre_turno': '68798dd1205f333d8f53a1c7',
            'nombre_horario': '68b6427cc8f94827ebfed695',
            'hora_entrada': '68b6427cc8f94827ebfed696',
            'hora_salida': '68b6427cc8f94827ebfed697',
            'dias_de_la_semana': '68b861ba34290efdd49ab24f',
            'tolerancia_retardo': '68b6427cc8f94827ebfed698',
            'retardo_maximo': '68b642e2bc17e2713cabe019',
            'grupo_ubicaciones_horario': '68b6427cc8f94827ebfed699',
            'dias_libres_empleado': '68bb20095035e61c5745de05',
            'duracion_estimada': '6854459836ea891d9d2be7d9',
            
            'grupo_comentarios_generales': '6927a0cdc03f0f8e5355437a',
            'grupo_comentarios_generales_fecha': '6927a0ea1c378cbd7f60a135',
            'grupo_comentarios_generales_texto': '6927a0ea1c378cbd7f60a136',
            'nombre_suplente': '6927a1176c60848998a157a2',
            'documento_check': '692a1b4e005c84ce5cd5167f',
            'datos_requeridos': '6769756fc728a0b63b8431ea',
            'envio_por': '6810180169eeaca9517baa5b',
            'configuracion_de_accesos': '696e6dda9517e760679e71eb',
            'bitacora_sala': '6998931ce4b114620fd4724d',
            'tipo_de_notificacion': '699dfe3b82be0dbe0319d38c',
            'bitacora_sala': '6998931ce4b114620fd4724d',
            'tipo_de_notificacion': '699dfe3b82be0dbe0319d38c',
            'url_de_etiqueta': '69a88700c02fcbc4bcfe85d2'
        })

        self.CONFIGURACION_RECORRIDOS = self.lkm.catalog_id('configuracion_de_recorridos')
        self.CONFIGURACION_RECORRIDOS_ID = self.CONFIGURACION_RECORRIDOS.get('id')
        self.CONFIGURACION_RECORRIDOS_OBJ_ID = self.CONFIGURACION_RECORRIDOS.get('obj_id')
        self.REGISTRO_ASISTENCIA = self.lkm.form_id('registro_de_asistencia','id')
        self.FORMATO_VACACIONES = self.lkm.form_id('formato_vacaciones_aviso','id')
        self.USUARIOS_FORM = self.lkm.form_id('usuarios', 'id')
        self.ENVIO_DE_NOTIFICACIONES_FORM = self.lkm.form_id('envio_de_notificaciones', 'id')
        self.CONFIGURACION_DE_RECORRIDOS_FORM = self.lkm.form_id('configuracion_de_recorridos','id')
        self.CONF_MODULO_SEGURIDAD = self.lkm.form_id('configuracion_modulo_seguridad','id')
        self.BITACORA_TRANSPORTISTAS = self.lkm.form_id('bitacora_de_transportistas','id')
        # OJO: el slug real registrado en Linkaform es "configuracin..." (sin "ó") —
        # Linkaform le quitó el acento de forma imperfecta al generar el nombre técnico
        # a partir de "Configuración de Flujo de Transportistas". No "corregir" esto sin
        # antes confirmar el item_name real en LKFModules.
        self.CONFIGURACION_FLUJO_TRANSPORTISTAS = self.lkm.form_id('configuracin_de_flujo_de_transportistas','id')

        self.INSPECCION_ENTRADA_CTPAT_TRACTOR = self.lkm.form_id('inspeccion_de_entrada_ctpat_tractor_cabezal','id')
        self.INSPECCION_ENTRADA_CTPAT_REMOLQUE = self.lkm.form_id('inspeccion_de_entrada_ctpat_remolque','id')
        self.INSPECCION_ENTRADA_CTPAT_CONTENEDOR = self.lkm.form_id('inspeccion_de_entrada_ctpat_contenedor','id')
        self.INSPECCION_SELLO = self.lkm.form_id('inspeccion_de_sello','id')

        self.f.update({
            'areas_del_rondin': '66462aa5d4a4af2eea07e0d1',
            'comentario_area_rondin': '66462b9d7124d1540f962088',
            'comentario_check_area': '681144fb0d423e25b42818d4',
            'estatus_del_recorrido': '6639b2744bb44059fc59eb62',
            'fecha_hora_inspeccion_area': '6760a908a43b1b0e41abad6b',
            'fecha_programacion':'6760a8e68cef14ecd7f8b6fe',
            'fecha_hora_fin':'6760a8e68cef14ecd7f8b6ff',
            'foto_evidencia_area': '681144fb0d423e25b42818d2',
            'foto_evidencia_area_rondin': '66462b9d7124d1540f962087',
            'grupo_de_areas_recorrido': '6645052ef8bc829a5ccafaf5',
            'nombre_area':'663e5d44f5b8a7ce8211ed0f',
            'nombre_del_recorrido': '6645050d873fc2d733961eba',
            'nombre_del_recorrido_en_catalog': '6644fb97e14dcb705407e0ef',
            'ubicacion_recorrido': '663e5c57f5b8a7ce8211ed0b',
            'fecha_inicio_rondin': '6818ea068a7f3446f1bae3b3',
            'fecha_fin_rondin': '6760a8e68cef14ecd7f8b6ff',
            'check_status': '681fa6a8d916c74b691e174b',
            'grupo_incidencias_check': '681144fb0d423e25b42818d3',
            'incidente_open': '6811455664dc22ecae83f75b',
            'incidente_area': '663e5d44f5b8a7ce8211ed0f',
            'incidente_location': '663e5c57f5b8a7ce8211ed0b',
            'incidente_evidencia': '681145323d9b5fa2e16e35cd',
            'incidente_documento': '685063ba36910b2da9952697',
            'url_registro_rondin': '6750adb2936622aecd075607',
            'bitacora_rondin_incidencias': '686468a637d014b9e0ab5090',
            'personalizacion_pases': '695d2e1f6be562c3da95c4a7',
            'pases': '695d31b503ccc7766ac28507',
            'grupo_alertas': '695d35b618a37ea04899524f',
            'nombre_alerta': '695d36605f78faab793f497b',
            'accion_alerta': '695d36605f78faab793f497c',
            'llamar_num_alerta': '695d36605f78faab793f497d',
            'email_alerta': '695d36605f78faab793f497e',
            'free_day_start': '55887b7e01a4de2ea71c5ab4',
            'free_day_end': '55887b7e01a4de2ea71c5ab5',
            'free_day_type': '55887b7e01a4de2ea71c5ab2',
            'free_day_autorization': '55887b7e01a4de2ea71c5ab8',
            'grupo_incluir': '69974d3806cc6d6a17f8b1fa',
            'pases_incluir': '69974d55879296015c1cd8d2',
            'prefijo_telefonico':'6a221532db633d0cf4faf12f',
            'grupo_requisitos':"676975321df93a68a609f9ce",
        })
        
        self.checkin_fields.update({
            'nombre_suplente':'6927a1176c60848998a157a2'
        })

        self.pase_entrada_fields.update({
            'grupo_vehiculos':'663e446cadf967542759ebba',
        })

        self.envio_correo_fields.update({
            'phone_to': '699f302213e8f8740c465bfc',
            'tipo_de_notificacion': '699dfe3b82be0dbe0319d38c'
        })
        self.cons_f.update({
            'quien_recibe_otro': '69c47a1ce96590f9dbf494b0',
        })

        self.configuracion_area = {
            'area': '663e5d44f5b8a7ce8211ed0f',
            'create_area': '688a33d9e61fcd2c299ff39e',
            'comentarios': '68504a3fd3ebdc2e9b9869d2',
            'foto_area': '68487646684fe30a8f9f3ef4',
            'nombre_nueva_area': '688a33d9e61fcd2c299ff39f',
            'option': '68487646684fe30a8f9f3ef2',
            'status': '689a46342038ded0e949be07',
            'status_comment': '689a46342038ded0e949be08',
            'qr_area': '68487646684fe30a8f9f3ef3',
            'tag_id': '68487646684fe30a8f9f3ef3',
            'ubicacion': '663e5c57f5b8a7ce8211ed0b',
        }
        
        self.incidence_filter = {
            'reporta_incidencia': "",
            'fecha_hora_incidencia':"",
            'ubicacion_incidencia':"",
            'area_incidencia': "",
            'incidencia':"",
            'comentario_incidencia': "",
            'tipo_dano_incidencia': "",
            'dano_incidencia':"",
            'evidencia_incidencia': [],
            'documento_incidencia':[],
            'prioridad_incidencia':"",
            'notificacion_incidencia':"",
            'datos_deposito_incidencia': [],
            'tags':[],
            'categoria':"",
            'sub_categoria':"",
            'incidente':"",
            'nombre_completo_persona_extraviada':"",
            'edad':"",
            'color_piel':"",
            'color_cabello':"",
            'estatura_aproximada':"",
            'descripcion_fisica_vestimenta':"",
            'nombre_completo_responsable':"",
            'parentesco':"",
            'num_doc_identidad':"",
            'telefono':"",
            'info_coincide_con_videos':"",
            'responsable_que_entrega':"",
            'responsable_que_recibe':"",
            'afectacion_patrimonial_incidencia':[],
            'personas_involucradas_incidencia': [],
            'acciones_tomadas_incidencia':[],
            'seguimientos_incidencia':[],
            'valor_estimado':"",
            'pertenencias_sustraidas':"",
            'placas':"",
            'tipo':"",
            'marca':"",
            'modelo':"",
            'color':"",
        }
        
        self.check_area_filter = {
            "tag_id": "",
            "ubicacion": "",
            "area": "",
            "tipo_de_area": "",
            "foto_del_area": [],
            "evidencia_incidencia": [],
            "documento_incidencia": [],
            "incidencias": [],
            "comentario_check_area": "",
            "status_check_area": "",
        }
        
        self.f.update({
            'bitacora_rondin_url': '690cefdca2dff2f469da17e0',
            'cantidad_areas_inspeccionadas': '68a7b68a22ac030a67b7f8f8',
            'checked_at': '68a7b68a22ac030a67b7f8f8',
            'form_name':'5d810a982628de5556500d55',
            'form_id':'5d810a982628de5556500d56',
        })
        
        self.IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.heic'}

        self.menu_form_fields = {
            "username": "6759e4a7a9a6e13c7b26da33",
            "usuario_id": "638a9a99616398d2e392a9f5",
            "grupo_asignado": "638a9ab3616398d2e392a9fa",
            "grupo_id": "639b65dfaf316bacfc551ba2",
            "elementos": "69efaf4c4a59aa2591074f45",
            "menu": "69efaf883bcb25ed1458465d",
            "seccion": "69efaf883bcb25ed1458465e",
            "elemento": "69efaf883bcb25ed1458465f",
            "key": "69efb57c4a59aa2591074f4e",
            "plataforms": "69f27e8cdf4d7acc80f2e9b0"
        }

        self.menu_catalog_fields = {
            "catalog_menu_key": "69f28216c76fd3bed14949a2",
            "catalog_menu": "69efaf883bcb25ed1458465d",
            "catalog_menu_order": "69f27e8cdf4d7acc80f2e9a8",
            "catalog_menu_icon": "69f27e8cdf4d7acc80f2e9a9",
            "catalog_menu_columns": "69f27e8cdf4d7acc80f2e9aa",
            "catalog_seccion_key": "69f28216c76fd3bed14949a3",
            "catalog_seccion": "69efaf883bcb25ed1458465e",
            "catalog_seccion_order": "69f27e8cdf4d7acc80f2e9ab",
            "catalog_seccion_column": "69f27e8cdf4d7acc80f2e9ac",
            "catalog_seccion_icon": "69f27e8cdf4d7acc80f2e9ad",
            "catalog_seccion_icon_color": "69f27e8cdf4d7acc80f2e9ae",
            "catalog_elemento": "69efaf883bcb25ed1458465f",
            "catalog_key": "69efb57c4a59aa2591074f4e",
            "catalog_type": "69efb3dcfc8545da78179bf9",
            "catalog_item_order": "69efb3dcfc8545da78179bfa",
            "catalog_href_web": "69efb3dcfc8545da78179bf8",
            "catalog_route_mobile": "69f27e8cdf4d7acc80f2e9af",
            "catalog_plataforms": "69f27e8cdf4d7acc80f2e9b0"
        }

        self.pass_fields_transportista = {
            "tipo_de_operacion": "6a1ddb53f5a36ba1c7dd029c",

            "nombre_crea_el_pase": "6a20741046cc9cdddf3b3c07",
            "email_crea_el_pase": "6a20741046cc9cdddf3b3c08",
            "telefono_crea_el_pase": "6a20741046cc9cdddf3b3c09",

            "proveedor": "6a1ddb53f5a36ba1c7dd029d",
            "proveedor_email": "6a207762cd730fb838ce1bb1",
            "proveedor_telefono": "6a207762cd730fb838ce1bb2",
            "empresa_transportista": "6a09fdc32fa9d55259ae9d2b",

            "grupo_documentos_para_ocr": "6a2ae394b8e5ca8fd73705dc",
            "tipo_de_documento": "6a2ae3d8cf0be6f60c19f85d",
            "no_de_documento": "6a2ae3d8cf0be6f60c19f85e",
            "documento_para_ocr": "6a2ae3d8cf0be6f60c19f85f",

            "proveedor_cliente_material": "6a207762cd730fb838ce1bb4",
            "orden_de_compra": "6a1ddb53f5a36ba1c7dd02a0",
            "grupo_materiales": "6a2714954a54077ffa2394e6",
            "contenedor": "6a2714eeca6ac6897ef55d92",
            "sello":      "6a2714eeca6ac6897ef55d93",
            "tipo":       "6a2714eeca6ac6897ef55d94",
            "cantidad":   "6a2714eeca6ac6897ef55d95",
            "peso":       "6a2714eeca6ac6897ef55d96",
            "volumen":    "6a2714eeca6ac6897ef55d97",
            "producto":      "6a3a6c2c9d500676ec5e3fbf",
            "lote":          "6ade55ab470ae4e36395ba2b",
            "no_referencia": "6a5b4fc9651b28e19d6352a2",

            "direccion_de_recoleccion": "6a1ddb53f5a36ba1c7dd02a1",
            "fecha_pase_transportista_desde": "6a1ddcba20dadbb04a29b59f",
            "fecha_pase_transportista_hasta": "6a1f15aec19e655f79987c34",
            "hora_inicial": "6a1f15aec19e655f79987c36",
            "hora_final": "6a1f15aec19e655f79987c37",

            "lugar_de_recoleccion": "6a2079343d463b1222e5d794",
            "direccion_lugar_de_recoleccion": "6a2079343d463b1222e5d795",
            "fecha_de_recoleccion": "6a2079343d463b1222e5d796",
            "hora_inicial_recoleccion": "6a2079343d463b1222e5d797",
            "hora_final_recoleccion": "6a2079343d463b1222e5d798",
            "anden_recoleccion": "6a2079343d463b1222e5d799",
            "responsable": "6a2079343d463b1222e5d79a",
            "responsable_email": "6a2079343d463b1222e5d79b",
            "responsable_telefono": "6a2079343d463b1222e5d79c",
            "metodo_de_embarque": "6a2079343d463b1222e5d79d",
            "incoterm": "6a2079343d463b1222e5d79e",

            "url_del_pase_transportista": "6a20d4a39ebbf58470fe73b5",
            "qr_del_pase_transportista": "6a20a8e138dff4ad8155c325",
            "estado_transportista": "6a20bb99782fe54a2681fc56",
            "token_transportista": "6a20c1811b6edd566116f483",

            "conductor_foto_licencia": "6a2add8342320b4d1b66db84",
            "conductor_nombre": "6a2adc08877c6087f9c2326b",
            "conductor_no_licencia": "6a2adc08877c6087f9c2326c",
            "conductor_lugar_expedicion": "6a2adc08877c6087f9c2326d",
            "conductor_vigencia": "6a2adc08877c6087f9c2326e",
            "ayudante_foto_licencia": "6a2add8342320b4d1b66db85",
            "ayudante_nombre": "6a2adc08877c6087f9c2326f",
            "ayudante_no_licencia": "6a2adc08877c6087f9c23270",
            "ayudante_lugar_expedicion": "6a2adc08877c6087f9c23271",
            "ayudante_vigencia": "6a2adc08877c6087f9c23272",
            "vehiculo_tarjeta_circulacion": "6a2add8342320b4d1b66db86",
            "vehiculo_linea": "6a2add8342320b4d1b66db87",
            "vehiculo_tipo_unidad": "6a2add8342320b4d1b66db88",
            "vehiculo_marca": "6a2add8342320b4d1b66db89",
            "vehiculo_modelo": "6a2add8342320b4d1b66db8a",
            "vehiculo_year": "6a2add8342320b4d1b66db8b",
            "vehiculo_placas": "6a2add8342320b4d1b66db8c",
            "vehiculo_no_economico": "6a2add8342320b4d1b66db8d",
            "vehiculo_niv": "6a2add8342320b4d1b66db8e",
            "vehiculo_color": "6afbbf71031d00fe8bd50a41",
            "conductor_rfc": "6a2c387c7df9203d2f98fcec",
            "foto_contenedores": "6a2b045ed8034654f212c1bc",
            "grupo_contenedores": "6a2add8342320b4d1b66db8f",
            "contenedor_numero": "6a2addcfcee6b93e39ab8a51",
            "contenedor_sello": "6a2addcfcee6b93e39ab8a52",
            "contenedor_tipo": "6a2addcfcee6b93e39ab8a53",
        }

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

        self.conf_flujo_transportistas_fields = {
            'etapas_activas': '6a75056924f23eef843cd01b',
            'configuracion_de_inspecciones': '6a7509cd6e87e5935b853b7b',
            'tipo_de_inspeccion': '6a750a1afd4ed68d7c57c24d',
            'norma': '6a7e2c5c23fb366f1918dea8',
            'subtipo': '6a7e2c5c23fb366f1918dea9',
        }

        self.inspeccion_entrada_tractor_fields = {
            'fotos_y_documentos': '6a5fcf869160bd10e1b0b323',
            'tipo_de_documento': '6a5fe2b0a5af7dac33061ea9',
            'documento': '6a5fe2b0a5af7dac33061eaa',

            'defensa': '20e7950eaac0054dbb8ca133',  # 1. Defensa (Si/No/N.A)
            'defensa_comentarios': '7aa52ec9ded1f199a3bfa307',
            'defensa_evidencia': '529623abe2be9e64816dec78',

            'motor_caja_de_la_bateria_caja_y_filtros_de_aire': '2aa45df8132536520b2a2bdd',  # 2. Motor, caja de la bateria, caja y filtros de aire (Si/No/N.A)
            'motor_caja_de_la_bateria_caja_y_filtros_de_aire_comentarios': '4604526acf0bf06c658add75',
            'motor_caja_de_la_bateria_caja_y_filtros_de_aire_evidencia': '8f12a402e6094434d6028246',

            'llantas_y_rines_tractor_y_remolque': '4b58a0007c1730a1ff9cc56f',  # 3. Llantas y rines (tractor y remolque) (Si/No/N.A)
            'llantas_y_rines_tractor_y_remolque_comentarios': '8e2645d9b0117869c0b93bc1',
            'llantas_y_rines_tractor_y_remolque_evidencia': 'a9be932860ceeb9face9b24d',

            'piso_tractor': 'acba826a28a8d1d48b743b53',  # 4. Piso (tractor) (Si/No/N.A)
            'piso_tractor_comentarios': '5e5cc9112d6c74a8c0d96c6b',
            'piso_tractor_evidencia': '5e0e635e8e5e7788793dc632',

            'tanque_de_combustible': '72e1fe8cf4fad9736fbb141c',  # 5. Tanque de combustible (Si/No/N.A)
            'tanque_de_combustible_comentarios': 'ddd7b180bcb8a98c556c67ef',
            'tanque_de_combustible_evidencia': 'cef55b76f55eed057cf64cad',

            'cabina_dormitorio_puertas_y_compartimientos_de_herramientas_seccion_de_pasajero_y_techo': '83ceff5fda79787b48219268',  # 6. Cabina, dormitorio, puertas y compartimientos de herramientas, seccion de pasajero y techo (Si/No/N.A)
            'cabina_dormitorio_puertas_y_compartimientos_de_herramientas_seccion_de_pasajero_y_techo_comentarios': '700d1c62d264a6c3039f65c1',
            'cabina_dormitorio_puertas_y_compartimientos_de_herramientas_seccion_de_pasajero_y_techo_evidencia': '6cb1dd20ae67dff1e20b08bd',

            'tanque_de_aire': 'ac82529cb6081ee6327ee04f',  # 7. Tanque de aire (Si/No/N.A)
            'tanque_de_aire_comentarios': '9cdc267b92fe4c144de7c370',
            'tanque_de_aire_evidencia': 'e01e5ac0be30514b35bd3d13',

            'ejes_de_transmision': 'bcb4e55eddda4821b9db0304',  # 8. Ejes de transmision (Si/No/N.A)
            'ejes_de_transmision_comentarios': '8e5bc150c3791c9917314b92',
            'ejes_de_transmision_evidencia': '5b72adefa1c7c716e0f24941',

            'quinta_rueda': '3ad0cca2f6449042ad664cfd',  # 9. Quinta rueda (Si/No/N.A)
            'quinta_rueda_comentarios': 'cedf4d6e6f7120c152d9c0fb',
            'quinta_rueda_evidencia': '35ccd51789e6260465d17ea7',

            'chasis': 'd08cc0f655036b4fb2a09056',  # 10. Chasis (Si/No/N.A)
            'chasis_comentarios': 'db0dd2a781343effa2a7153d',
            'chasis_evidencia': 'e957e4cb96e1ef8f999a5938',

            'puertas_externa': '5c100788b4211b8122e4395c',  # 11. Puertas externa (Si/No/N.A)
            'puertas_externa_comentarios': '87fffff1f65ef97ddc4d23bf',
            'puertas_externa_evidencia': '666ce737007a5ccc57c9f369',

            'piso_externo_trailer_contenedor_caja': 'f87fd7be1133ee21cc723f7c',  # 12. Piso externo (trailer, contenedor, caja) (Si/No/N.A)
            'piso_externo_trailer_contenedor_caja_comentarios': 'de6dffa1def019fe589a329a',
            'piso_externo_trailer_contenedor_caja_evidencia': 'e7c54e4187ee035e6bb3be7b',

            'paredes_externa': 'fc63e8996ccf5c91a80c0e2f',  # 13. Paredes externa (Si/No/N.A)
            'paredes_externa_comentarios': '531d51796e724cc7f14cb496',
            'paredes_externa_evidencia': 'b2d3aaf29aa9374130881632',

            'pared_frontal_externa': '731b4abf0672038c57d8d516',  # 14. Pared frontal externa (Si/No/N.A)
            'pared_frontal_externa_comentarios': '1f3c15fb61a4a143f773809d',
            'pared_frontal_externa_evidencia': '56d9b00ce47ae297a64aa90b',

            'techo_externo': '8b18d4aa1d62615cacf2776f',  # 15. Techo externo (Si/No/N.A)
            'techo_externo_comentarios': '85df5aa6a444e9490f14ce86',
            'techo_externo_evidencia': '5b82b568466ceebc18d49dd3',

            'unidad_de_refrigeracion': '8b4e8a6dec2392c9f267e179',  # 16. Unidad de refrigeracion (Si/No/N.A)
            'unidad_de_refrigeracion_comentarios': '747090a5b505163130df82e4',
            'unidad_de_refrigeracion_evidencia': '5544eaaccb74e9d09b7e2f77',

            'escape_mofles': '48de45705387f226f6551c1b',  # 17. Escape / Mofles (Si/No/N.A)
            'escape_mofles_comentarios': '0307abb04ee4f8b3786cca23',
            'escape_mofles_evidencia': '32f0559232cbc31f5cc6a472',
        }

        self.inspeccion_entrada_ctpat_contenedor_fields = {
            'fotos_y_documentos': '6a5fde6455cec5f5e85ea2a0',
            'tipo_de_documento': '6a5fe2b0a5af7dac33061ea9',
            'documento': '6a5fe2b0a5af7dac33061eaa',

            'altura_interior': 'd412fb9f428dfc231c9bc3f0',  # Altura interior (text)
            'ancho_interior': '6477c73222d9b7e8dd1de3b9',  # Ancho interior (text)
            'longitud_interior': 'd7c19cbd2cfe6b19f848d697',  # Longitud interior (text)
            'exterior_parte_inferior_del_contenedor_bastidor_o_chasis': '4a819aa25c6e76080f76317a',  # Exterior / parte inferior del contenedor (bastidor o chasis) (checkbox: Todos/Suciedad/Plagas/Fauna)
            'puertas_interiores_exteriores': 'b4f2b497790d8fa30739ab05',  # Puertas interiores / exteriores (checkbox: Todos/Suciedad/Plagas/Fauna)
            'pared_interior_lado_derecho': 'c334bc2360c643779bdcd495',  # Pared interior lado derecho (checkbox: Todos/Suciedad/Plagas/Fauna)
            'pared_interior_lado_izquierdo': '4c90dcc67f8e9f029878502c',  # Pared interior lado izquierdo (checkbox: Todos/Suciedad/Plagas/Fauna)
            'pared_interior_frontal': '14aea746aadf15c99edb8592',  # Pared interior frontal (checkbox: Todos/Suciedad/Plagas/Fauna)
            'techo_cubierta_superior': 'bc75ab3fdb2258286b0b41c0',  # Techo / cubierta superior (checkbox: Todos/Suciedad/Plagas/Fauna)
            'piso_interior': '371a7d9c3ae8a40a32b3762a',  # Piso (interior) (checkbox: Todos/Suciedad/Plagas/Fauna)
        }

        self.inspeccion_entrada_ctpat_remolque_fields = {
            'fotos_y_documentos': '6a5fde3b04fdbbdbcfdfc2a2',
            'tipo_de_documento': '6a5fe2b0a5af7dac33061ea9',
            'documento': '6a5fe2b0a5af7dac33061eaa',

            'altura_interior': '6703c4acd45242ffb0eb0839',  # Altura interior (text)
            'ancho_interior': '7bfa6fe868c1cbec93a051e5',  # Ancho interior (text)
            'longitud_interior': '2624dc82316e99315084d385',  # Longitud interior (text)

            'tanque_de_aire': 'd1fae4d0b2ec9569fbcf8770',  # 1. Tanque de aire (Si/No)
            'tanque_de_aire_comentarios': 'd2bacb536ead1a15f56bbe6c',
            'tanque_de_aire_evidencia': '28538bb0340a0eccc15e150b',

            'ejes_de_transmision': 'd57c0e9a92f8b3b552f2b66a',  # 2. Ejes de transmision (Si/No)
            'ejes_de_transmision_comentarios': '9f6a0733c5c36bcc4e6051de',
            'ejes_de_transmision_evidencia': '089e40849794b1edbe667291',

            'quinta_rueda': 'aeed49c20dd20d18904ac28f',  # 3. Quinta rueda (Si/No)
            'quinta_rueda_comentarios': '481f00fd61a55c0b9aef99e4',
            'quinta_rueda_evidencia': 'c86cf900756ed0667122d999',

            'chasis': '9a6743b2e92e16e2b727e667',  # 4. Chasis (Si/No)
            'chasis_comentarios': '6aa6dabeb1430c92bf9c36a9',
            'chasis_evidencia': 'c420045f52f188fcbd616165',

            'puertas_externa': 'b0dca85ed86edd92560f634c',  # 5. Puertas externa (Si/No)
            'puertas_externa_comentarios': '3b85b7104be1df0dbe8762e7',
            'puertas_externa_evidencia': '608def717f6c6f14e1f8ab6e',

            'piso_externo_trailer_contenedor_caja': '2cb78278523b502800a47e2e',  # 6. Piso externo (trailer, contenedor, caja) (Si/No)
            'piso_externo_trailer_contenedor_caja_comentarios': '7bc7a9a7a58d45946c2e70a6',
            'piso_externo_trailer_contenedor_caja_evidencia': 'c16b8d4dfc22709c7785cc63',

            'paredes_externa': '198cf876dc13d7bd658a4cbd',  # 7. Paredes externa (Si/No)
            'paredes_externa_comentarios': '8a9af06c2c1045f46dfa44d2',
            'paredes_externa_evidencia': '8af47b03f950e87661b5835b',

            'pared_frontal_externa': '36b4b172e38a3dc1b8b226d1',  # 8. Pared frontal externa (Si/No)
            'pared_frontal_externa_comentarios': 'bb279c901f91c114d1220452',
            'pared_frontal_externa_evidencia': 'ddff798b400d03d48b9ef808',

            'techo_externo': 'bbc21e44dec3040d81e005f2',  # 9. Techo externo (Si/No)
            'techo_externo_comentarios': 'e2e3ae0dbf920b1c44502fbb',
            'techo_externo_evidencia': '59bf2262a664e2b16ba1a299',

            'unidad_de_refrigeracion': 'cbb1c127c08011c3d7d4c344',  # 10. Unidad de refrigeracion (Si/No)
            'unidad_de_refrigeracion_comentarios': '80ad083a0f6319e6fd63d681',
            'unidad_de_refrigeracion_evidencia': 'd0240215edecf39a02c5a891',

            'escape_mofles': '545c0b134ab1d2f11cef90a9',  # 11. Escape / Mofles (Si/No)
            'escape_mofles_comentarios': '736b1fe2e2609d47beef2a03',
            'escape_mofles_evidencia': 'b7618c209a113ef54ec2b58b',
        }

        self.inspeccion_de_sello_fields = {
            'numero_de_sello_fisico': 'ad57d9e43537244dc2f66280',  # Numero de sello fisico (text)
            'numero_de_sello_esperado_revisado': '22e2974e099b937e4c9c7094',  # Numero de sello esperado (revisado) (text)
            'tipo_de_sello_clasificacion_iso_17712': '1e534c51db80d867b1922c86',  # Tipo de sello (clasificacion ISO 17712) (radio: Indicative/Security/High Security)
            'matriz_vttt_marca_cada_accion_verificada': '92ab37dbe06381e6100f88f0',  # Matriz VTTT - Marca cada accion verificada (checkbox: View/Verify/Tug/Twist)
            '1_foto_del_sello': '1defc3e446a9ebd00c649dbc',  # 1. Foto del sello (images)
            '2_sello_colocado_en_las_puertas': '26f5f07d55f304e9015ae64d',  # 2. Sello colocado en las puertas (images)
            '3_puertas_completas_del_remolque': 'be928c48d8a6353077ec5eba',  # 3. Puertas completas del remolque (images)
            '4_placas_o_economico': 'd7479071e6aabdeaa10ce41b',  # 4. Placas o economico (images)
            '5_identificacion_del_operador': '718a0a37c5a6965b2127d2c0',  # 5. Identificacion del operador (images)
            'comentarios': '0e009f7829544463cbf89e1e',  # Comentarios (textarea)
        }


    def flatten_roles(self, roles_raw):
        """
        Se aplana la estructura de roles (viene como [{ROL_CATALOG_OBJ_ID: {rol: 'Gerente'}}, ...])
        a una lista simple de strings (['Gerente', ...]) para el frontend.

        Nota: roles_raw ya pasó por format_cr/_labels, que aplana
        {ROL_CATALOG_OBJ_ID: {rol_field_id: valor}} a {'rol': valor}.
        """
        return [r.get('rol') for r in roles_raw if r.get('rol')]

    def create_pass_transportista(self, data):
        print(simplejson.dumps(data, indent=3))
        f = self.pass_fields_transportista
        metadata = self.lkf_api.get_metadata(form_id=self.PASE_ENTRADA_TRANSPORTISTA)
        metadata.update({
            'id': self.object_id(),
            'properties': {
                'device_properties': {
                    'System': 'Script',
                    'Module': 'Accesos',
                    'Process': 'Pase Transportista',
                    'Action': 'create_pass_transportista',
                    'File': 'modules/accesos/items/scripts/Accesos/accesos_utils.py',
                }
            }
        })
        pass_id = metadata['id']

        crea  = data.get('crea_el_pase', {})
        recibe = data.get('recibe_el_pase', {})
        mat   = data.get('material', {})
        lugar = data.get('lugar_entrega_recepcion', {})

        horario = lugar.get('horario_disponible', '') or ''
        hora_inicio, hora_fin = '', ''
        if '-' in horario:
            partes = horario.split('-')
            hora_inicio = partes[0].strip()
            hora_fin    = partes[1].strip()

        dominio = data.get('dominio', 'http://localhost:3000')
        parent_id = self.user.get('parent_id')
        url_pase_transportista = f"{dominio}/transportistas/preview/transportista/{pass_id}?p_id={parent_id}"
        qr_pase_transportista = self.create_custom_qr(
            url_pase_transportista,
            f"qr_code_pase_transportista_{pass_id}",
            self.PASE_ENTRADA_TRANSPORTISTA,
            f['qr_del_pase_transportista'])

        answers = {
            f"{self.mf['tipo_registro']}": 'entrada',
            f"{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}":{
                f"{self.f['location']}":location,
                f"{self.f['area']}":area
                },
            f"{self.PASE_ENTRADA_OBJ_ID}":pase,
            f"{self.mf['codigo_qr']}": str(access_pass['_id']),
            f"{self.mf['fecha_entrada']}":self.today_str(employee.get('timezone', 'America/Monterrey'), date_format='datetime'),
        }
        vehiculos = data.get('vehiculo',[])
        if vehiculos:
            list_vehiculos = []
            for item in vehiculos:
                if item:
                    tipo = item.get('tipo','')
                    marca = item.get('marca','')
                    modelo = item.get('modelo','')
                    estado = item.get('estado','')
                    placas = item.get('placas','')
                    color = item.get('color','')
                    list_vehiculos.append({
                        self.TIPO_DE_VEHICULO_OBJ_ID:{
                            self.mf['tipo_vehiculo']:tipo,
                            self.mf['marca_vehiculo']:marca,
                            self.mf['modelo_vehiculo']:modelo,
                        },
                        self.ESTADO_OBJ_ID:{
                            self.mf['nombre_estado']:estado,
                        },
                        self.mf['placas_vehiculo']:placas,
                        self.mf['color_vehiculo']:color,
                    })
            answers[self.mf['grupo_vehiculos']] = list_vehiculos  

        equipos = data.get('equipo',[])

        if equipos:
            list_equipos = []
            for item in equipos:
                tipo = item.get('tipo','').lower().replace(' ', '_')
                nombre = item.get('nombre','')
                marca = item.get('marca','')
                modelo = item.get('modelo','')
                color = item.get('color','')
                serie = item.get('serie','')
                list_equipos.append({
                    self.mf['tipo_equipo']:tipo,
                    self.mf['nombre_articulo']:nombre,
                    self.mf['marca_articulo']:marca,
                    self.mf['modelo_articulo']:modelo,
                    self.mf['color_articulo']:color,
                    self.mf['numero_serie']:serie,
                })
            answers[self.mf['grupo_equipos']] = list_equipos

        gafete = data.get('gafete',{})
        if gafete:
            gafete_ans = {}
            gafete_ans[self.GAFETES_CAT_OBJ_ID] = {self.gafetes_fields['gafete_id']:gafete.get('gafete_id')}
            gafete_ans[self.LOCKERS_CAT_OBJ_ID] = {self.mf['locker_id']:gafete.get('locker_id')}
            gafete_ans[self.mf['documento']] = gafete.get('documento_garantia')
            answers.update(gafete_ans)
            self.update_gafet_status(answers)


        comment = data.get('comentario_acceso',[])
        comments_pase = data.get('comentario_pase',[])
        if comment or comments_pase:
            comment_list = []
            for c in comment:
                if c.get('comentario_pase'):
                    comment_list.append(
                        {
                            self.bitacora_fields['comentario']: c.get('comentario_pase'),
                            self.bitacora_fields['tipo_comentario'] :c.get('tipo_de_comentario').lower().replace(' ', '_')
                        }
                    )
            for c in comments_pase:
                if c.get('comentario_pase'):
                    comment_list.append(
                        {
                            self.bitacora_fields['comentario']:c.get('comentario_pase'),
                            self.bitacora_fields['tipo_comentario'] :c.get('tipo_de_comentario').lower().replace(' ', '_')
                        }
                    )
            if comment_list:
                answers.update({self.bitacora_fields['grupo_comentario']:comment_list})

        tipo_pase = data.get('perfil_pase', '')
        visit_list = data.get('visita_a') if tipo_pase != 'Interno' else access_pass.get('answers', {}).get(self.bitacora_fields['visita_a'], [])
        if visit_list:
            visit_list2 = []
            for c in visit_list:
                visit_list2.append({
                    f"{self.bitacora_fields['visita']}": { 
                        self.bitacora_fields['visita_nombre_empleado']: c.get('nombre', c.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID, {}).get(self.mf['nombre_empleado'], '')),
                        self.mf['id_usuario'] : [c.get('user_id')] if c.get('user_id') else c.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID, {}).get(self.mf['id_usuario'], []),
                        self.bitacora_fields['visita_departamento_empleado']: [c.get('departamento')],
                        self.bitacora_fields['puesto_empleado']: [c.get('puesto')],
                        self.mf['email_visita_a']: [c.get('email')] if c.get('email') else c.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID, {}).get(self.mf['email_visita_a'], []),
                   }
                })
            answers.update({self.bitacora_fields['visita_a']:visit_list2})

        #Pregeneracion de PDF de Etiqueta
        qr_code = access_pass.get('_id')
        pdf = self.lkf_api.get_pdf_record(qr_code, name_pdf='Pase de Entrada', send_url=True)
        answers[self.f['url_de_etiqueta']] = pdf.get('json', {}).get('download_url', '')
        
        metadata.update({'answers':answers})
        response_create = self.lkf_api.post_forms_answers(metadata)
        if response_create.get('status_code') in [200, 201, 202]:
            response_create.update({'url_de_etiqueta': pdf.get('json', {}).get('download_url', '')})           
        return response_create

    def access_pass_create_ics(self, access_pass, answers, ics_invitation=False):
        """
        Crea archivo para envio de invitacion a google calenar
        args:
            acces_pass (json): objeto con datos de pase enviados por front
            answers (json): objeto con el pase a crear
        return:
            res (json): reponse, con archivo de ics
        """
        res = {}
        if ics_invitation:
            id_forma = self.PASE_ENTRADA
            id_campo = self.pase_entrada_fields['archivo_invitacion']

            fecha_desde_visita = access_pass.get("fecha_desde_visita")
            descripcion = access_pass.get("descripcion", "")
            ubicacion = self.unlist(access_pass.get("ubicaciones"))
            visita_a = access_pass.get("visita_a")
            tema_cita = access_pass.get("tema_cita", f"Cita en {ubicacion}")
            if "Usuario Actual" in visita_a:
                visita_a = self.employee.get('worker_name')
            creado_por_email = access_pass.get("link", {}).get("creado_por_email")
            nombre = access_pass.get("nombre")
            email = access_pass.get("email")
            #TODO poner mails de la getne vistiada
            #answers...
            attendee_ids = [{"email": email, "nombre": nombre}, {"email": creado_por_email, "nombre": visita_a}]
            address = access_pass.get("address",{})
            geolocation = address.get('geolocation', [])
            if geolocation:
                geolocation = self.unlist(address.get('geolocation', [])).get('search_txt', '')
            else:
                geolocation = ubicacion
            fecha_desde_hasta = access_pass.get("fecha_desde_hasta")
            start_datetime = datetime.strptime(fecha_desde_visita, "%Y-%m-%d %H:%M:%S")
            stop_datetime = start_datetime + timedelta(hours=1)
            
            meeting = [
                {
                    f['tipo_de_documento']:  doc.get('tipo', ''),
                    f['no_de_documento']:    doc.get('no_doc', ''),
                    f['documento_para_ocr']: [{'file_name': doc.get('file_name', ''), 'file_url': doc.get('file_url', '')}] if doc.get('file_url') else [],
                }
            ]

            try:
                respuesta_ics = self.upload_ics(id_forma, id_campo, meetings=meeting)
            except Exception as e:
                print(f"Error al generar o subir el archivo ICS: {e}")
                respuesta_ics = {}
            
            if respuesta_ics:
                res = {
                    self.pase_entrada_fields['archivo_invitacion'] : [
                            {
                                "file_name":respuesta_ics.get('file_name',''),
                                "file_url": respuesta_ics.get('file_url','')
                            }
                        ]}
        
        return res

    def access_pass_google_pass(self, res, access_pass):
        """
        Crea google wallet pass del pase de acceso
        """
        qrcode_to_google_pass = res.get('json', {}).get('id', '')
        link_info=access_pass.get('link', "")
        docs=""
        
        if link_info:
            # for index, d in enumerate(link_info["docs"]): 
            #     if(d == "agregarIdentificacion"):
            #         docs+="iden"
            #     elif(d == "agregarFoto"):
            #         docs+="foto"
            #     if index==0 :
            #         docs+="-"
            # link_pass= f"{link_info['link']}?id={res.get('json')['id']}&user={link_info['creado_por_id']}&docs={docs}"
            id_forma = self.PASE_ENTRADA
            id_campo = self.pase_entrada_fields['archivo_invitacion']

            address = access_pass.get("address")
            tema_cita = access_pass.get("tema_cita")
            descripcion = access_pass.get("descripcion")
            fecha_desde_visita = access_pass.get("fecha_desde_visita")
            fecha_desde_hasta = access_pass.get("fecha_desde_hasta")
            creado_por_email = access_pass.get("link", {}).get("creado_por_email")
            ubicacion = self.unlist(access_pass.get("ubicaciones"))
            nombre = access_pass.get("nombre")
            visita_a = access_pass.get("visita_a")
            email = access_pass.get("email")

            start_datetime = datetime.strptime(fecha_desde_visita, "%Y-%m-%d %H:%M:%S")

            if not fecha_desde_hasta:
                stop_datetime = start_datetime + timedelta(hours=1)
                meeting = [
                    {
                        "id": 1,
                        "start": start_datetime,
                        "stop": stop_datetime,
                        "name": tema_cita,
                        "description": descripcion,
                        "location": ubicacion,
                        "allday": False,
                        "rrule": None,
                        "alarm_ids": [{"interval": "minutes", "duration": 10, "name": "Reminder"}],
                        'organizer_name': visita_a,
                        'organizer_email': creado_por_email,
                        "attendee_ids": [{"email": email, "nombre": nombre}, {"email": creado_por_email, "nombre": visita_a}],
                    }
                ]

                try:
                    respuesta_ics = self.upload_ics(id_forma, id_campo, meetings=meeting)
                except Exception as e:
                    print(f"Error al generar o subir el archivo ICS: {e}")
                    respuesta_ics = {}

                file_name = respuesta_ics.get('file_name', '')
                file_url = respuesta_ics.get('file_url', '')

                access_pass_custom={
                    "link":link_pass,
                    "enviar_correo_pre_registro": access_pass.get("enviar_correo_pre_registro",[]),
                    "archivo_invitacion": [
                        {
                            "file_name": f"{file_name}",
                            "file_url": f"{file_url}"
                        }
                    ]
                }
            else:
                access_pass_custom={
                    "link":link_pass,
                    "enviar_correo_pre_registro": access_pass.get("enviar_correo_pre_registro",[])
                }

            data_to_google_pass = {
                "nombre": access_pass.get("nombre"),
                "visita_a": access_pass.get("visita_a"),
                "ubicacion": access_pass.get("ubicaciones"),
                "address": address.get('address'),
                "empresa": getattr(self, 'company', ""),
                "all_data": access_pass
            }

            google_wallet_pass_url = self.create_class_google_wallet(data=data_to_google_pass, qr_code=qrcode_to_google_pass)
            access_pass_custom.update({
                "google_wallet_pass_url": google_wallet_pass_url,
            })
            
            res = self.update_pass(access_pass=access_pass_custom, folio=res.get("json")["id"])
        return res
    
    def visita_a_set_format(self, employee):
        """
        Crea formato de set para pase de acceso
        args:
            employee (json): objeto de self.get_employee_data
        return:
            res (json) : fromato de vista_a pase de acceso
        """
        res = {}
        nombre_visita_a = employee.get('worker_name')
        phone = self.unlist(employee.get('new_user_phone', employee.get('telefono2', employee.get('telefono1',""))))
        email = self.unlist(employee.get('new_user_email', employee.get('usuario_email', "")))
        user_id_id = self.unlist(employee.get('user_id_id',employee.get('usuario_id',"")))
        username = self.unlist(employee.get('new_user_username',""))
        departamento = self.unlist(employee.get('worker_department',""))
        puesto = self.unlist(employee.get('worker_position',""))
        #Lo seteamo en una lista porque es campo catlog detail
        if nombre_visita_a:
            res = {
                self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID: {
                    self.mf['nombre_empleado'] : nombre_visita_a,
                    self.mf['telefono_visita_a']: [phone, ],
                    self.mf['email_visita_a']: [email, ],
                    self.mf['id_usuario']: [user_id_id, ],
                    self.mf['username']: [username, ],
                    self.mf['departamento_empleado']: [departamento, ],
                    self.mf['puesto_empleado']: [puesto, ],
                }
            }
        return res
    
    def access_pass_set_status(self, answers):
        """
        Evalua criterios del pase y regresa el status del pase
        Proceso
        Activo
        Vencido
        args:
            answers (json): Objeto de answers
        return:
            status (str): String con status
        """
        foto_ok = False
        id_vista = False
        fecha_ok = False
        vista_a_ok = False
        autorizado_ok = False
        status = 'proceso'
        foto  = answers[self.pase_entrada_fields['walkin_fotografia']]
        if isinstance(foto, list) and len(foto) > 0:
            foto = foto[0]

        if isinstance(foto, dict):
            if 'file_url' in foto.keys() and foto['file_url']:
                foto_ok = self.valid_url(foto['file_url'])
        #TODO revisar configuracion
        id_vista = answers.get(self.pase_entrada_fields['walkin_identificacion'], [])
        if isinstance(id_vista, list) and len(id_vista) > 0:
            id_vista = id_vista[0]

        if isinstance(id_vista, dict):
            if 'file_url' in id_vista.keys() and id_vista['file_url']:
                id_vista = self.valid_url(id_vista['file_url'])
        id_vista = True
        today = self.get_today_format()
        try:
            fecha_desde_visita = self.valid_date(answers[self.pase_entrada_fields['fecha_desde_visita']]) 
        except:
            fecha_desde_visita = None
        try:
            fecha_desde_hasta = self.valid_date(answers[self.pase_entrada_fields['fecha_desde_hasta']])
        except:
            fecha_desde_hasta = None
        if fecha_desde_visita and fecha_desde_hasta and fecha_desde_visita >= today and fecha_desde_hasta >= today: 
            fecha_ok = True
        
        grupo_visitados = answers[self.mf['grupo_visitados']]
        for vista in grupo_visitados:
            if isinstance(vista, int):
                vista_a = grupo_visitados[vista]
            else:
                vista_a = vista
            if vista_a.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID,{}).get(self.mf['nombre_empleado']):
                vista_a_ok = True

            if answers.get(self.pase_entrada_fields['catalago_autorizado_por'],{}).get(self.pase_entrada_fields['autorizado_por']):
                autorizado_ok = True

        if foto_ok and id_vista and fecha_ok and vista_a_ok and autorizado_ok:
            status = 'activo'
        elif foto_ok and id_vista and fecha_ok and vista_a_ok and not autorizado_ok:
            status = 'por_autorizar'
        elif not fecha_ok:
            status = 'vencido'
        return status

    def access_pass_vista_a(self, visita_a):
        """
        Crea grupo repetitivo de personas que son vistadas para pase de entrada
        
        args:
            visita_a (list): lista con NOMBRES de empleados a quien se vista
        
        return:
            lista con elementos para visitantes de pase de entrdada
        """
        res = []
        employee = {}
        if isinstance(visita_a, str):
            if visita_a == 'Usuario Actual':
                user_id = self.user['user_id']
                employee = self.Employee.get_employee_data(user_id=self.user['user_id'], get_one=True)
                self.employee = employee
                visita_a = employee.get('worker_name')
            visita_a = [visita_a,]

        if isinstance(visita_a, dict):
            if visita_a == 'Usuario Actual':
                user_id = self.user['user_id']
                employee = self.Employee.get_employee_data(user_id=self.user['user_id'], get_one=True)
                self.employee = employee
                visita_a = {'nombre': employee.get('worker_name')}
            name = visita_a.get('nombre')
            email = visita_a.get('email')
            phone = visita_a.get('telefono')
            visita_set = {}
            if not employee and self.valid_email(email):
                employee = self.Employee.get_employee_data(email=email, get_one=True)
    
            if not employee and name:
                employee = self.Employee.get_employee_data(name=name, get_one=True)
    
            if not employee and phone:
                employee = self.Employee.get_employee_data(phone=phone, get_one=True)

            if employee:
                visita_set = self.visita_a_set_format(employee)
            if visita_set:
                return [visita_set,]
            else:
                return []

        set_autorizado_por = False
        if not visita_a:
            #Si no trae dato utiliza el dato del usuario que esta creando el pase
            visita_a = [self.user.get('email'),]
            set_autorizado_por = True
       
        for visita in visita_a:
            visita_set = {}
            if visita == 'Usuario Actual':
                user_id = self.user['user_id']
                employee = self.Employee.get_employee_data(user_id=self.user['user_id'], get_one=True)
                self.employee = employee
                visita_set.update(self.visita_a_set_format(employee))
                if visita_set:
                    res.append(visita_set)
                continue
            if self.valid_email(visita):
                employee = self.Employee.get_employee_data(email=visita, get_one=True)
                self.employee = employee
                # TODO REVISAR ESTOOOOOO
                if set_autorizado_por:
                    self.autorizado_por = employee.get('worker_name')
            elif "*" in visita:
                visita = visita.replace("*", "")
                employee = {
                    'worker_name': visita,
                    'usuario_email': "",
                    'user_id_id': "",
                    'new_user_username': "",
                    'worker_department': "",
                    'worker_position': "",
                }
            else:
                employee = self.Employee.get_employee_data(name = visita, get_one=True)
                self.employee = employee
            visita_set.update(self.visita_a_set_format(employee))
            if visita_set:
                res.append(visita_set)

        return res

    def catalagos_pase_no_jwt(self, qr_code):
        # se quito porque ya no se edita el pase
        # cat_vehiculos= self.catalogo_vehiculos({})
        # cat_estados= self.catalogo_estados({})
        pass_selected= self.get_pass_custom(qr_code)
        res={"pass_selected":pass_selected}
        return res

    def set_boot_status(self, checkin_type):
        if checkin_type == 'in':
            set_boot_status = 'abierta'
        elif checkin_type == 'out':
            set_boot_status = 'cerrada'
        return set_boot_status

    def is_boot_available(self, location, area):
        self.last_check_in = self.get_last_checkin(location, area)
        last_status = True if self.last_check_in.get('checkin_type') == 'abierta' else False
        return last_status

    def catalogos_pase_area(self, location_name):
        areas, salas = self.get_areas_by_location(location_name, divide_salas=True)
        res = {
            "areas_by_location" : areas,
            "salas_by_location" : salas
        }
        return res

    def get_areas_by_location(self, location_name, divide_salas=False):
        match_query = {
            "deleted_at": {"$exists": False},
            "form_id": self.AREAS_DE_LAS_UBICACIONES,
        }
        if type(location_name) == str:
            match_query[f"answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.f['location']}"] = location_name
        elif type(location_name) == list:
            match_query[f"answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.f['location']}"] = {"$in": location_name}

        query = [
            {"$match": match_query},
            {"$project": {
                "_id": 0,
                "area": f"$answers.{self.Location.f['area']}",
                "tipo_de_area": f"$answers.{self.TIPO_AREA_OBJ_ID}.{self.f['tipo_de_area']}",
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query))
        areas = []
        salas = []
        for item in data:
            if divide_salas:
                if item.get('tipo_de_area') == 'Salas de juntas':
                    salas.append(item.get('area'))
                else:
                    areas.append(item.get('area'))
            else:
                areas.append(item.get('area'))
        if divide_salas:
            return sorted(areas), sorted(salas)
        return sorted(areas)

    def get_employees_data(self, names=None, user_id=None, username=None, email=None,  get_one=False):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.EMPLEADOS,
            }
        if names:
            match_query.update(self._get_match_q(self.f['worker_name'], names))
        if user_id:
            match_query.update(self._get_match_q(f"{self.USUARIOS_OBJ_ID}.{self.employee_fields['user_id_id']}", user_id))
        if username:
            match_query.update(self._get_match_q(self.f['username'], username))
        if email:
            match_query.update(self._get_match_q(self.employee_fields['usuario_email'], email)) 
        query = [
            {'$match': match_query },    
            {'$project': self.project_format(self.employee_fields)},
            {'$sort':{'worker_name':1}},
            ]
        res = self.format_cr_result(self.cr.aggregate(query), get_one=get_one)
        return res

    def get_my_pases(self, tab_status, limit=10, skip=0, search_name=None):
        employee = self.get_employee_data(user_id=self.user.get('user_id'), get_one=True)
        user_data = self.lkf_api.get_user_by_id(self.user.get('user_id'))
        employee['timezone'] = user_data.get('timezone','America/Monterrey')
        fecha_hoy = datetime.now(pytz.timezone(employee.get('timezone'))).replace(microsecond=0).astimezone(pytz.utc).replace(tzinfo=None)
        fecha_hoy_formateada = fecha_hoy.strftime('%Y-%m-%d %H:%M:%S')
        match_query = {
            'form_id':self.PASE_ENTRADA,
            'deleted_at':{'$exists':False},
            f"answers.{self.pase_entrada_fields['visita_a']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_empleado']}": employee.get('worker_name') or '',
        }
        if tab_status == "Favoritos":
            match_query.update({f"answers.{self.pase_entrada_fields['favoritos']}":'si'})
        elif tab_status == "Activos":
            match_query.update({f"answers.{self.pase_entrada_fields['status_pase']}":'activo'})
        elif tab_status == "Vencidos":
            match_query.update({f"answers.{self.pase_entrada_fields['status_pase']}":'vencido'})

        if search_name:
            match_query.update({
                f"$or": [
                    {f"answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['nombre_visita']}": {"$regex": search_name, "$options": "i"}},
                    {f"answers.{self.mf['nombre_pase']}": {"$regex": search_name, "$options": "i"}}
                ]
            })
        # Conteo total de registros
        count_query = [
            {"$match": match_query},
            {"$count": "total"}
        ]
        count_result = self.format_cr(self.cr.aggregate(count_query))
        total_count = count_result[0]['total'] if count_result else 0
        current_page = (skip // limit) + 1
        total_pages = ceil(total_count / limit) if limit else 1

        query = [ 
            {"$match":match_query},
            {'$project':
                {
                    '_id': 1,
                    'folio': "$folio",
                    'favoritos':f"$answers.{self.pase_entrada_fields['favoritos']}",
                    'ubicacion': f"$answers.{self.mf['grupo_ubicaciones_pase']}.{self.UBICACIONES_CAT_OBJ_ID}.{self.f['location']}",
                    # 'ubicacion': f"$answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}",
                    'nombre': {"$ifNull":[
                        f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['nombre_visita']}",
                        f"$answers.{self.mf['nombre_pase']}"]},
                    'estatus': f"$answers.{self.pase_entrada_fields['status_pase']}",
                    'empresa': {"$ifNull":[
                         f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}",
                         f"$answers.{self.mf['empresa_pase']}"]},
                    'email':  {"$ifNull":[
                        f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['email_vista']}",
                        f"$answers.{self.mf['email_pase']}"]},
                    'telefono': {"$ifNull":[
                        f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['telefono']}",
                        f"$answers.{self.mf['telefono_pase']}"]},
                    'fecha_desde_visita': f"$answers.{self.mf['fecha_desde_visita']}",
                    'fecha_desde_hasta':{'$ifNull':[
                        f"$answers.{self.mf['fecha_desde_hasta']}",
                        f"$answers.{self.mf['fecha_desde_visita']}"]
                        },
                    'identificacion': {'$ifNull':[
                        f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['identificacion']}",
                        f"$answers.{self.pase_entrada_fields['walkin_identificacion']}"]},
                    'foto': {'$ifNull':[
                        f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['foto']}",
                        f"$answers.{self.pase_entrada_fields['walkin_fotografia']}"]},
                    'visita_a_nombre':
                        f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_empleado']}",
                    'visita_a_puesto': 
                        f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['puesto_empleado']}",
                    'visita_a_departamento':
                        f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['departamento_empleado']}",
                    'visita_a_user_id':
                        f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['user_id_empleado']}",
                    'visita_a_email':
                        f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.f['email']}",
                    'motivo_visita':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}.{self.mf['motivo']}",
                    'tipo_de_pase':f"$answers.{self.pase_entrada_fields['perfil_pase']}",
                    'tema_cita':f"$answers.{self.pase_entrada_fields['tema_cita']}",
                    'descripcion':f"$answers.{self.pase_entrada_fields['descripcion']}",
                    'tipo_visita': f"$answers.{self.pase_entrada_fields['tipo_visita']}",
                    'limite_de_acceso': f"$answers.{self.mf['config_limitar_acceso']}",
                    'config_dia_de_acceso': f"$answers.{self.mf['config_dia_de_acceso']}",
                    'identificacion': {'$ifNull':[
                        f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['identificacion']}",
                        f"$answers.{self.pase_entrada_fields['walkin_identificacion']}"]},
                    'limitado_a_dias':f"$answers.{self.mf['config_dias_acceso']}",
                    'perfil_pase':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}",
                    'tipo_de_comentario': f"$answers.{self.mf['tipo_de_comentario']}",
                    'tipo_fechas_pase': f"$answers.{self.mf['tipo_visita_pase']}",
                    'enviar_correo_pre_registro': f"$answers.{self.pase_entrada_fields['enviar_correo_pre_registro']}",
                    'enviar_correo': f"$answers.{self.pase_entrada_fields['enviar_correo']}",
                    'grupo_areas_acceso': f"$answers.{self.mf['grupo_areas_acceso']}",
                    'grupo_equipos': f"$answers.{self.mf['grupo_equipos']}",
                    'grupo_vehiculos': f"$answers.{self.mf['grupo_vehiculos']}",
                    'grupo_instrucciones_pase': f"$answers.{self.mf['grupo_instrucciones_pase']}",
                    'comentario_area_pase':f"$answers.{self.mf['grupo_areas_acceso']}.{self.pase_entrada_fields['commentario_area']}",
                    'archivo_invitacion': f"$answers.{self.mf['archivo_invitacion']}",
                    'codigo_qr': f"$answers.{self.mf['codigo_qr']}",
                    'qr_pase': f"$answers.{self.mf['qr_pase']}",
                    'link':f"$answers.{self.pase_entrada_fields['link']}",
                    'perfil_pase': f"$answers.{self.mf['nombre_perfil']}",
                    'status_pase': f"$answers.{self.pase_entrada_fields['status_pase']}",
                    'pdf_to_img': f"$answers.{self.pase_entrada_fields['pdf_to_img']}",
                    'autorizado_por':f"$answers.{self.pase_entrada_fields['autorizado_por']}"
                }
            },
            {'$sort':{'_id':-1}},
        ]
        query.append({'$skip': skip})
        query.append({'$limit': limit})
        records = self.format_cr(self.cr.aggregate(query))
        # print("RECORDS",  simplejson.dumps(records, indent=4))
        for x in records:
            qr_code = x.get('_id')
            total_entradas = self.get_count_ingresos(qr_code)
            if total_entradas:
                x['total_entradas'] = total_entradas.get('total_records')
            else:
                x['total_entradas'] = 0
            visita_a =[]
            v = x.pop('visita_a_nombre') if x.get('visita_a_nombre') else []
            d = x.get('visita_a_departamento',[])
            p = x.get('visita_a_puesto',[])
            e =  x.get('visita_a_user_id',[])
            u =  x.get('visita_a_email',[])

            for idx, nombre in enumerate(v):
                emp = {'nombre':nombre}
                emp['departamento'] = d[idx] if idx < len(d) and d[idx] else [""]
                emp['puesto'] = p[idx] if idx < len(p) and p[idx] else [""]
                emp['user_id'] = e[idx] if idx < len(e) and e[idx] else [""]
                emp['email'] = u[idx] if idx < len(u) and u[idx] else [""]
                visita_a.append(emp)
            if x['tipo_de_pase'] == 'Visita General' or x['tipo_de_pase'] == 'visita general':
                x['visita_a'] = visita_a
                x['favoritos'] = x.get('favoritos', [""]) if x.get('favoritos') else ""
                x['motivo_visita'] = x.get('motivo_visita', [""]) if x.get('motivo_visita') else ""
                x['email'] = x.get('email', [""]) if x.get('email') else ""
                x['empresa'] = x.get('empresa', [""]) if x.get('empresa') else ""
                x['telefono'] = x.get('telefono', [""]) if x.get('telefono') else ""
                # x['pdf'] = self.lkf_api.get_pdf_record(x['_id'], template_id = 447, name_pdf='Pase de Entrada', send_url=True)
            else:
                
                x['visita_a'] = visita_a
                x['favoritos'] = x.get('favoritos') or ""
                x['motivo_visita'] =x.get('motivo_visita') or ""
                x['email']= x.get('email') or ""
                x['empresa']= x.get('empresa') or ""
                x['telefono']= x.get('telefono') or ""
                # x['pdf'] = self.lkf_api.get_pdf_record(x[' # for idx, dic in enumerate(x['grupo_areas_acceso']):
            # x['comentario_area_pase']=x.pop('comentario_area_pase',[])
           

                # for key in list(item.keys()):
                #     if key in id_to_name_mapping:
                #         # Reemplaza el id hexadecimal por su nombre en el diccionario
                #         item[self.pase_entrada_fields['commentario_area']] = item.pop(key)

            for visita in x.get('visita_a', []):
                visita['departamento'] = visita['departamento'][0] if isinstance(visita.get('departamento'), list) and visita.get('departamento') else visita.get('departamento', "")
                visita['puesto'] = visita['puesto'][0] if isinstance(visita.get('puesto'), list) and visita.get('puesto') else visita.get('puesto', "")
                visita['user_id'] = visita['user_id'][0] if isinstance(visita.get('user_id'), list) and visita.get('user_id') else visita.get('user_id', "")
                visita['email'] = visita['email'][0] if isinstance(visita.get('email'), list) and visita.get('email') else visita.get('email', "")

            visitas = x.get('visita_a', [])
            x['status_pase'] = x.get('estatus', "")
            x['autorizado_por'] = x.get('autorizado_por', "")
            x['grupo_areas_acceso'] = self._labels_list(x.pop('grupo_areas_acceso',[]), self.mf)
            x['grupo_instrucciones_pase'] = self._labels_list(x.pop('grupo_instrucciones_pase',[]), self.mf)

            
            x['grupo_vehiculos'] = self.format_vehiculos_simple(x.pop('grupo_vehiculos',[]))
            x['grupo_equipos'] = self.format_equipos_simple(x.pop('grupo_equipos',[]))
            x['comentarios'] = x['grupo_instrucciones_pase']

            comentarios = []
            for item in x.pop('comentarios', []):
                comentario_pase = item.get('comentario_pase', '') 
                tipo_comentario = item.get('tipo_de_comentario', '')
                comentarios.append({
                    'comentario_pase': comentario_pase,
                    'tipo_comentario': tipo_comentario
                })
            x['comentarios'] = comentarios

            x.pop('visita_a_nombre', None)
            x.pop('visita_a_departamento', None)
            x.pop('visita_a_puesto', None)
            x.pop('visita_a_user_id', None)
            x.pop('visita_a_email', None)
        # print("data", simplejson.dumps(records, indent=4))
        return  {
            "records": records,
            "total_records": total_count,
            "total_pages": total_pages,
            "actual_page": current_page,
            "records_on_page": len(records)
        }

    def get_guard_last_checkin(self, user_ids):
        '''
            Se realiza busqued del ulisto registro de checkin de un usuario
        '''
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.CHECKIN_CASETAS,
            }
        unwind_query = {
            f"answers.{self.f['guard_group']}.{self.checkin_fields['checkin_status']}": "entrada"
        }
        if user_ids and type(user_ids) == list:
            if len(user_ids) == 1:
                #hace la busqueda por directa, para optimizar recuros
                user_ids = user_ids[0]
            else:
                #hace busqueda en lista de opciones
                match_query.update({
                    f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.f['user_id_jefes']}":{'$in':user_ids}
                    })
        if user_ids and type(user_ids) == int:
            unwind_query.update({
                f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.f['user_id_jefes']}":user_ids
                })
        if not unwind_query:
            return self.LKFException({"msg":f"Algo salio mal al intentar buscar el checkin del los ids: {user_id}"})
        query = [
            {'$match': match_query },
            {'$unwind': f"$answers.{self.f['guard_group']}"},
            {'$match':unwind_query},
            {'$project': self.project_format(self.checkin_fields)},
            {'$sort':{'created_at':-1}},
            {'$limit':1}
            ]
        return self.format_cr_result(self.cr.aggregate(query), get_one=True)

    def get_booth_config(self, location):
        """
        Se obtiene la configuracion de la ubicacion de la forma Configuracion Modulo Seguridad
        Opciones actuales: impresion_de_pase, auto_acceso
        Args:
            location  (str): Ubicacion de la caseta.
        Returns:
            Lista de configuraciones
        """
        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.CONF_MODULO_SEGURIDAD,
            }},
            {'$sort': {'updated_at': -1}},
            {'$limit': 1},
            {'$project': {
                "answers": 1,
            }},
            {'$unwind': f"$answers.{self.conf_modulo_seguridad['grupo_requisitos']}"},
            {'$match': {
                f"answers.{self.conf_modulo_seguridad['grupo_requisitos']}.{self.UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}": location
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query))
        format_data = []
        if data:
            data = self.unlist(data)
            configuracion_de_accesos = data.get('configuracion_de_accesos', [])
            format_data = list(set(configuracion_de_accesos))
        return format_data

    def get_booth_status(self, booth_area, location):
        last_chekin = self.get_last_checkin(location, booth_area)
        booth_status = {
            "status":'Cerrada',
            "guard_on_dutty":'',
            "user_id":'',
            "stated_at":'',
            "fotografia_inicio_turno":[],
            "fotografia_cierre_turno":[],
            }
        if last_chekin.get('checkin_type') in ['entrada','apertura','disponible', 'abierta']:
            #todo
            #user_id 
            booth_status['status'] = 'Abierta'
            booth_status['guard_on_dutty'] = last_chekin.get('employee') 
            booth_status['stated_at'] = last_chekin.get('boot_checkin_date')
            booth_status['checkin_id'] = last_chekin.get('_id', last_chekin.get('id', ''))
            booth_status['fotografia_inicio_turno'] = last_chekin.get('fotografia_inicio_turno',[]) 
            booth_status['fotografia_cierre_turno'] = last_chekin.get('fotografia_cierre_turno',[]) 
        return booth_status

    def get_attendance_images(self, user_id):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.REGISTRO_ASISTENCIA,
                "created_by_id": user_id,
            }},
            {"$sort": {"created_at": -1}},
            {"$limit": 1},
            {"$project": {
                "_id": 0,
                "start_turn_image": {"$ifNull": [f"$answers.{self.f['image_checkin']}", ""]},
                "end_turn_image": {"$ifNull": [f"$answers.{self.f['foto_cierre_turno']}", ""]},
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query))
        format_data = {}
        if data:
            format_data = self.unlist(data)
        return format_data

    def update_guard_status(self, guard, this_user):
        attendance_images = self.get_attendance_images(this_user.get('user_id', self.unlist(this_user.get('usuario_id', 0000))))
        status_turn = 'Turno Cerrado'
        if this_user.get('status') == 'in':
            status_turn = 'Turno Abierto'

        this_user['start_turn_image'] = attendance_images.get('start_turn_image', [])
        this_user['end_turn_image'] = attendance_images.get('end_turn_image', [])
        this_user['status_turn'] = status_turn
        return this_user

    def get_shift_data(self, booth_location=None, booth_area=None, search_default=True):
        """
        Se obtienen los datos del turno.

        Args:
            booth_location (str, optional): Ubicacion de la caseta. Defaults to None.
            booth_area (str, optional): Area de la caseta. Defaults to None.
            search_default (bool, optional): Buscar caseta por defecto. Defaults to True.

        Returns:
            dict: Datos del turno.
        """
        load_shift_json = {}
        username = self.user.get('username')
        user_id = self.user.get('user_id')
        email = self.user.get('email')

        #! Se obtiene la informacion del usuario, si esta dentro o fuera de turno.
        this_user = self.get_employee_checkin_status_by_id(user_id, booth_location, booth_area)
        if not this_user:
            this_user = self.get_employee_data(user_id=user_id, get_one=True)
            this_user['name'] = this_user.get('worker_name','')
        
        #! Se obtienen los puestos de guardia configurados.
        user_booths = []
        guards_positions = self.config_get_guards_positions()
        if not guards_positions:
            return self.LKFException({'title': 'Advertencia', 'msg': 'No existen puestos de guardias configurados.'})

        check_aux_guard = self.check_in_aux_guard()
        if this_user and this_user.get('status') == 'out':
            #! Si el usuario esta fuera de turno, se verifica si se encuentra como guardia de apoyo para obtener la informacion del usuario.
            for aux_id, aux_data in check_aux_guard.items():
                if aux_id == user_id:
                    this_user = aux_data
                    this_user['status'] = 'in' if aux_data.get('status') == 'in' else 'out'
                    this_user['location'] = aux_data.get('location')
                    this_user['area'] = aux_data.get('area')
                    this_user['checkin_date'] = aux_data.get('checkin_date')
                    this_user['checkout_date'] = aux_data.get('checkout_date')
                    this_user['checkin_position'] = aux_data.get('checkin_position')

        #! Si el usuario esta dentro de turno, se obtienen los guardias de apoyo registrados con el.
        if this_user and this_user.get('status') == 'in':
            location_employees = {self.chife_guard: {}, self.support_guard:[]}
            booth_area = this_user['area']
            booth_location = this_user['location']
            for aux_id, aux_data in check_aux_guard.items():
                if aux_id == user_id:
                    guard = aux_data
                if aux_data.get('status') == 'in' \
                    and aux_data.get('location') == booth_location \
                    and aux_data.get('area') == booth_area \
                    and aux_data.get('user_id') != user_id:
                    location_employees[self.support_guard].append(aux_data)
        else:
            #! Si el usuario esta fuera de turno, se obtienen los guardias disponibles.
            default_booth , user_booths = self.get_user_booth(search_default=False)
            if not booth_location:
                booth_location = default_booth.get('location', '')
                booth_area = default_booth.get('area', '')
            if not default_booth:
                return self.LKFException({'title': 'Advertencia', 'msg': 'No se encontro la caseta por defecto, revisa la configuracion.'})

            location_employees = self.get_booths_guards(booth_location, booth_area, solo_disponibles=True)
            guard = self.get_user_guards(location_employees=location_employees)
            if not guard:
                #! Si el usuario no esta configurado como guardia se agrega su informacion general.
                common_user = {
                    "user_id": self.unlist(this_user.get('usuario_id')),
                    "name": this_user.get('name'),
                    "location": booth_location,
                    "area": booth_area,
                    "config_exception": {
                        "title": "Configuracion",
                        "msg": "El usuario no esta configurado correctamente, faltan configuraciones para Turnos."
                    }
                }
                load_shift_json["guard"] = common_user
                return load_shift_json

        #! Se agregan las fotos de los guardias y se filtran los guardias de apoyo.
        location_employees = self.set_employee_pic(location_employees)
        support_guards = location_employees.get('guardia_de_apoyo', [])
        for idx, guard in enumerate(support_guards):
            if guard.get('user_id') == user_id:
                support_guards.pop(idx)
                break
        location_employees['guardia_de_apoyo'] = support_guards
        
        #! Se obtienen los detalles de la caseta..
        booth_address = self.get_area_address(booth_location, booth_area)
        load_shift_json["location"] = {
            "name":  booth_location,
            "area": booth_area,
            "city": booth_address.get('city'),
            "state": booth_address.get('state'),
            "address": booth_address.get('address'),
        }
        
        #! Se obtienen los detalles del turno.
        load_shift_json["booth_stats"] = self.get_page_stats( booth_area, booth_location, "Turnos")
        load_shift_json["booth_status"] = self.get_booth_status(booth_area, booth_location)
        load_shift_json["support_guards"] = location_employees.get(self.support_guard, "")
        load_shift_json["guard"] = self.update_guard_status(guard, this_user)
        load_shift_json["notes"] = self.get_list_notes(booth_location, booth_area, status='abierto')
        load_shift_json["user_booths"] = user_booths
        load_shift_json["booth_config"] = self.get_booth_config(booth_location)
        # print(simplejson.dumps(load_shift_json, indent=4))
        return load_shift_json

    def get_page_stats(self, booth_area, location, page='', month=None, year=None):
        timezone = pytz.timezone('America/Mexico_City')
        today = datetime.now(timezone).strftime("%Y-%m-%d")        
        res={}

        if page == 'Turnos':
            #Visitas dentro, Gafetes pendientes y Vehiculos estacionados
            query_visitas = [
                {'$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": self.BITACORA_ACCESOS,
                    f"answers.{self.bitacora_fields['status_visita']}": "entrada",
                    f"answers.{self.PASE_ENTRADA_OBJ_ID}.{self.pase_entrada_fields['status_pase']}": {"$in": ["Activo"]},
                    f"answers.{self.bitacora_fields['caseta_entrada']}": booth_area,
                    f"answers.{self.bitacora_fields['ubicacion']}": location,
                    # f"answers.{self.mf['fecha_entrada']}": {"$gte": f"{today} 00:00:00", "$lte": f"{today} 23:59:59"}
                }},
                {'$project': {
                    '_id': 1,
                    'vehiculos': {"$ifNull": [f"$answers.{self.mf['grupo_vehiculos']}", []]},
                    'equipos': {"$ifNull": [f"$answers.{self.mf['grupo_equipos']}", []]},
                    'status_visita': f"$answers.{self.bitacora_fields['status_visita']}",
                    'id_gafete': f"$answers.{self.GAFETES_CAT_OBJ_ID}.{self.gafetes_fields['gafete_id']}",
                    'status_gafete': f"$answers.{self.mf['status_gafete']}"
                }},
                {'$group': {
                    '_id': None,
                    'total_visitas_dentro': {'$sum': 1},
                    'total_equipos_dentro': {
                        '$sum': {
                            '$cond': {
                                'if': {'$eq': ['$status_visita', 'entrada']},
                                'then': {'$size': '$equipos'},
                                'else': 0
                            }
                        }
                    },
                    'total_vehiculos_dentro': {'$sum': {'$size': '$vehiculos'}},
                    'gafetes_info': {
                        '$push': {
                            'id_gafete':'$id_gafete',
                            'status_gafete':'$status_gafete'
                        }
                    }
                }}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_visitas))
            total_vehiculos_dentro = resultado[0]['total_vehiculos_dentro'] if resultado else 0
            total_visitas_dentro = resultado[0]['total_visitas_dentro'] if resultado else 0
            total_equipos_dentro = resultado[0]['total_equipos_dentro'] if resultado else 0
            gafetes_info = resultado[0]['gafetes_info'] if resultado else []
            gafetes_pendientes = sum(1
                for gafete in gafetes_info
                    if gafete.get('id_gafete') and gafete.get('status_gafete', '').lower() != 'entregado'
            )
            
            res['total_vehiculos_dentro'] = total_vehiculos_dentro
            res['in_invitees'] = total_visitas_dentro
            res['total_equipos_dentro'] = total_equipos_dentro
            res['gafetes_pendientes'] = gafetes_pendientes

            #Articulos concesionados
            query_concesionados = [
                {'$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": self.CONCESSIONED_ARTICULOS,
                    f"answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}": location,
                }},
                {'$project': {
                    '_id': 1,
                }},
                {'$group': {
                    '_id': None,
                    'articulos_concesionados': {'$sum': 1}
                }}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_concesionados))
            articulos_concesionados = resultado[0]['articulos_concesionados'] if resultado else 0
            
            res['articulos_concesionados'] = articulos_concesionados

            #Incidentes pendientes
            query_incidentes = [
                {'$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": self.BITACORA_INCIDENCIAS,
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.incidence_fields['area_incidencia']}": booth_area,
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.incidence_fields['ubicacion_incidencia']}": location,
                    f"answers.{self.incidence_fields['estatus']}": 'abierto'
                }},
                {'$project': {
                    '_id': 1,
                }},
                {'$group': {
                    '_id': None,
                    'incidentes_pendientes': {'$sum': 1}
                }}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_incidentes))
            incidentes_pendientes = resultado[0]['incidentes_pendientes'] if resultado else 0
            
            res['incidentes_pendites'] = incidentes_pendientes

            #Fallas pendientes
            query_fallas = [
                {'$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": self.BITACORA_FALLAS,
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.fallas_fields['falla_caseta']}": booth_area,
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.fallas_fields['falla_ubicacion']}": location,
                    f"answers.{self.fallas_fields['falla_estatus']}": 'abierto',
                    # f"answers.{self.incidence_fields['fecha_hora_incidencia']}": {"$gte": today,"$lt": f"{today}T23:59:59"}
                }},
                {'$project': {
                    '_id': 1,
                }},
                {'$group': {
                    '_id': None,
                    'fallas_pendientes': {'$sum': 1}
                }}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_fallas))
            fallas_pendientes = resultado[0]['fallas_pendientes'] if resultado else 0

            res['fallas_pendientes'] = fallas_pendientes

        elif page == 'Accesos' or page == 'Bitacoras':
            #Visitas en el dia, personal dentro, vehiculos dentro, salidas registradas y personas dentro
            match_query_one = {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_ACCESOS,
                f"answers.{self.PASE_ENTRADA_OBJ_ID}.{self.pase_entrada_fields['status_pase']}": {"$in": ["Activo"]},
                f"answers.{self.bitacora_fields['ubicacion']}": location,
            }

            match_query_two = {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_ACCESOS,
                f"answers.{self.PASE_ENTRADA_OBJ_ID}.{self.pase_entrada_fields['status_pase']}": {"$in": ["Activo"]},
                f"answers.{self.bitacora_fields['ubicacion']}": location,
                f"answers.{self.mf['fecha_entrada']}": {"$gte": f"{today} 00:00:00", "$lte": f"{today} 23:59:59"}
            }

            if not booth_area == 'todas' and booth_area:
                match_query_one.update({
                    f"answers.{self.bitacora_fields['caseta_entrada']}": booth_area,
                })
                match_query_two.update({
                    f"answers.{self.bitacora_fields['caseta_entrada']}": booth_area,
                })

            query_visitas = [
                {'$match': match_query_one},
                {'$project': {
                    '_id': 1,
                    'vehiculos': {"$ifNull": [f"$answers.{self.mf['grupo_vehiculos']}", []]},
                    'equipos': {"$ifNull": [f"$answers.{self.mf['grupo_equipos']}", []]},
                    'perfil': f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['nombre_perfil']}",
                    'status_visita': f"$answers.{self.bitacora_fields['status_visita']}",
                    'fecha_salida': f"$answers.{self.mf['fecha_salida']}"
                }},
                {'$group': {
                    '_id': None,
                    'visitas_en_dia': {'$sum': 1},
                    'total_vehiculos_dentro': {
                        '$sum': {
                            '$cond': {
                                'if': {'$eq': ['$status_visita', 'entrada']},
                                'then': {'$size': '$vehiculos'},
                                'else': 0
                            }
                        }
                    },
                    'total_equipos_dentro': {
                        '$sum': {
                            '$cond': {
                                'if': {'$eq': ['$status_visita', 'entrada']},
                                'then': {'$size': '$equipos'},
                                'else': 0
                            }
                        }
                    },
                    'detalle_visitas': {
                        '$push': {
                            'perfil': '$perfil',
                            'status_visita': '$status_visita',
                            'fecha_salida': '$fecha_salida'
                        }
                    }
                }}
            ]

            query_visitas_dia = [
                {'$match': match_query_two},
                {'$project': {
                    '_id': 1,
                    'vehiculos': {"$ifNull": [f"$answers.{self.mf['grupo_vehiculos']}", []]},
                    'equipos': {"$ifNull": [f"$answers.{self.mf['grupo_equipos']}", []]},
                    'perfil': f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['nombre_perfil']}",
                    'status_visita': f"$answers.{self.bitacora_fields['status_visita']}"
                }},
                {'$group': {
                    '_id': None,
                    'visitas_en_dia': {'$sum': 1},
                    'total_vehiculos_dentro': {
                        '$sum': {
                            '$cond': {
                                'if': {'$eq': ['$status_visita', 'entrada']},
                                'then': {'$size': '$vehiculos'},
                                'else': 0
                            }
                        }
                    },
                    'total_equipos_dentro': {
                        '$sum': {
                            '$cond': {
                                'if': {'$eq': ['$status_visita', 'entrada']},
                                'then': {'$size': '$equipos'},
                                'else': 0
                            }
                        }
                    },
                    'detalle_visitas': {
                        '$push': {
                            'perfil': '$perfil',
                            'status_visita': '$status_visita'
                        }
                    }
                }}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_visitas))
            today_salida = f"{today} 00:00:00"
            resultado_dia = self.format_cr(self.cr.aggregate(query_visitas_dia))

            total_vehiculos_dentro = resultado[0]['total_vehiculos_dentro'] if resultado else 0
            total_equipos_dentro = resultado[0]['total_equipos_dentro'] if resultado else 0
            detalle_visitas_todas = resultado[0]['detalle_visitas'] if resultado else []
            visitas_en_dia = resultado_dia[0]['visitas_en_dia'] if resultado_dia else 0

            personal_dentro = 0
            salidas = 0
            personas_dentro = 0

            for visita in detalle_visitas_todas:
                status_visita = visita['status_visita'].lower()

                if status_visita == "entrada":
                    personas_dentro += 1
                    
                if visita.get('fecha_salida') and visita.get('fecha_salida') >= today_salida:
                    salidas += 1

            res['total_vehiculos_dentro'] = total_vehiculos_dentro
            res['total_equipos_dentro'] = total_equipos_dentro
            res['visitas_en_dia'] = visitas_en_dia
            res['personal_dentro'] = personal_dentro
            res['salidas_registradas'] = salidas
            res['personas_dentro'] = personas_dentro

            query_paqueteria = [
                {'$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": self.PAQUETERIA,
                    f"answers.{self.paquetes_fields['estatus_paqueteria']}": "guardado",
                    f"answers.{self.paquetes_fields['fecha_recibido_paqueteria']}": {"$gte": f"{today} 00:00:00", "$lte": f"{today} 23:59:59"}
                }},
                {'$project': {
                    '_id': 1,
                }},
                {'$group': {
                    '_id': None,
                    'paquetes_recibidos': {'$sum': 1},
                }}
            ]

            resultado_paquetes = self.format_cr(self.cr.aggregate(query_paqueteria))
            paquetes_recibidos = resultado_paquetes[0]['paquetes_recibidos'] if resultado_paquetes else 0

            res['paquetes_recibidos'] = paquetes_recibidos

        elif page == 'Incidencias':
            #Incidentes por dia, por semana y por mes
            now = datetime.now(pytz.timezone("America/Mexico_City"))
            today_date = now.date()
            user_data = self.lkf_api.get_user_by_id(self.user.get('user_id'))
            zona = user_data.get('timezone','America/Monterrey')
            dateFromWeek, dateToWeek = self.get_range_dates('this_week', zona)

            match_query_incidentes = {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_INCIDENCIAS,
            }

            if location:
                match_query_incidentes.update({
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.incidence_fields['ubicacion_incidencia']}": location,
                })
            if booth_area:
                match_query_incidentes.update({
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.incidence_fields['area_incidencia']}": booth_area,
                })

            query_incidentes = [
                {'$match': match_query_incidentes},
                {'$addFields': {
                    'fecha_incidencia': {
                        '$dateFromString': {
                            'dateString': f"$answers.{self.incidence_fields['fecha_hora_incidencia']}",
                            'format': "%Y-%m-%d %H:%M:%S"
                        }
                    }
                }},
                {'$facet': {
                    'por_dia': [
                        {'$match': {
                            'fecha_incidencia': {
                                '$gte': datetime.combine(today_date, time.min),
                                '$lte': datetime.combine(today_date, time.max)
                            }
                        }},
                        {'$count': 'incidentes_x_dia'}
                    ],
                    'por_semana': [
                        {'$match': {
                            'fecha_incidencia': {
                                '$gte': dateFromWeek,
                                '$lte': dateToWeek
                            }
                        }},
                        {'$group': {
                            '_id': {
                                'year': {'$isoWeekYear': '$fecha_incidencia'},
                                'week': {'$isoWeek': '$fecha_incidencia'}
                            },
                            'incidentes_x_semana': {'$sum': 1}
                        }}
                    ],
                    'por_mes': [
                        {'$match': {
                            'fecha_incidencia': {
                                '$gte': datetime.combine(today_date.replace(day=1), time.min),
                                '$lte': datetime.combine(today_date, time.max)
                            }
                        }},
                        {'$group': {
                            '_id': {
                                'year': {'$year': '$fecha_incidencia'},
                                'month': {'$month': '$fecha_incidencia'}
                            },
                            'incidentes_x_mes': {'$sum': 1}
                        }}
                    ]
                }}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_incidentes))[0]

            res['incidentes_x_dia'] = resultado['por_dia'][0]['incidentes_x_dia'] if resultado['por_dia'] else 0
            res['incidentes_x_semana'] = resultado['por_semana'][0]['incidentes_x_semana'] if resultado['por_semana'] else 0
            res['incidentes_x_mes'] = resultado['por_mes'][0]['incidentes_x_mes'] if resultado['por_mes'] else 0

            match_query_fallas = {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_FALLAS,
                f"answers.{self.fallas_fields['falla_estatus']}": 'abierto',
            }

            if location:
                match_query_fallas.update({
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.fallas_fields['falla_ubicacion']}": location,
                })
            if booth_area:
                match_query_fallas.update({
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.fallas_fields['falla_caseta']}": booth_area,
                })

            #Fallas pendientes
            query_fallas = [
                {'$match': match_query_fallas},
                {'$project': {
                    '_id': 1,
                }},
                {'$group': {
                    '_id': None,
                    'fallas_pendientes': {'$sum': 1}
                }}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_fallas))
            fallas_pendientes = resultado[0]['fallas_pendientes'] if resultado else 0

            res['fallas_pendientes'] = fallas_pendientes
        elif page == 'Articulos':
            #Articulos concesionados pendientes
            query_concesionados = [
                {'$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": self.CONCESSIONED_ARTICULOS,
                    f"answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}": location,
                    f"answers.{self.cons_f['status_concesion']}": "abierto",
                }},
                {'$project': {
                    '_id': 1,
                }},
                {'$group': {
                    '_id': None,
                    'articulos_concesionados_pendientes': {'$sum': 1}
                }}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_concesionados))
            articulos_concesionados_pendientes = resultado[0]['articulos_concesionados_pendientes'] if resultado else 0
            
            res['articulos_concesionados_pendientes'] = articulos_concesionados_pendientes

            #Articulos perdidos
            query_perdidos = [
                {'$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": self.BITACORA_OBJETOS_PERDIDOS,
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.perdidos_fields['ubicacion_perdido']}": location,
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.perdidos_fields['area_perdido']}": booth_area,
                }},
                {'$project': {
                    '_id': 1,
                    'status_perdido': f"$answers.{self.perdidos_fields['estatus_perdido']}",
                }},
                {'$group': {
                    '_id': None,
                    'perdidos_info': {
                        '$push': {
                            'status_perdido':'$status_perdido'
                        }
                    }
                }}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_perdidos))
            perdidos_info = resultado[0]['perdidos_info'] if resultado else []

            articulos_perdidos = 0
            for perdido in perdidos_info:
                status_perdido = perdido.get('status_perdido', '').lower()
                if status_perdido not in ['entregado', 'donado']:
                    articulos_perdidos += 1

            res['articulos_perdidos'] = articulos_perdidos

            match_query_paqueteria = {
                "deleted_at": {"$exists": False},
                "form_id": self.PAQUETERIA,
                f"answers.{self.paquetes_fields['estatus_paqueteria']}": "guardado",
            }

            if location:
                match_query_paqueteria.update({
                    f"answers.{self.paquetes_fields['ubicacion_paqueteria']}": location,
                })
            if booth_area and not booth_area == "todas" and not booth_area == "":
                match_query_paqueteria.update({
                    f"answers.{self.paquetes_fields['area_paqueteria']}": booth_area,
                })

            query_paqueteria = [
                {'$match': match_query_paqueteria },
                {'$project': {
                    '_id': 1,
                }},
                {'$group': {
                    '_id': None,
                    'paquetes_recibidos': {'$sum': 1},
                }}
            ]

            resultado_paquetes = self.format_cr(self.cr.aggregate(query_paqueteria))
            paquetes_recibidos = resultado_paquetes[0]['paquetes_recibidos'] if resultado_paquetes else 0

            res['paquetes_recibidos'] = paquetes_recibidos

        elif page == 'Notas':
            #Notas
            match_query = {
                "deleted_at": {"$exists": False},
                "form_id": self.ACCESOS_NOTAS,
                f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}": location,
            }

            if booth_area and not booth_area == "todas" and not booth_area == "":
                match_query.update({
                    f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['nombre_area']}": booth_area,
                })
                
            query_notas = [
                {'$match': match_query},
                {'$project': {
                    '_id': 1,
                    'nota_status': f"$answers.{self.notes_fields['note_status']}",
                    'fecha_apertura': f"$answers.{self.notes_fields['note_open_date']}",
                    'fecha_cierre': f"$answers.{self.notes_fields['note_close_date']}"
                }},
            ]

            notas = self.format_cr(self.cr.aggregate(query_notas))
            notas_del_dia = 0
            notas_abiertas = 0
            notas_cerradas = 0

            for nota in notas:
                if(nota.get('nota_status') == 'abierto'):
                    notas_abiertas += 1
                if(nota.get('fecha_apertura') >= f"{today} 00:00:00" and nota.get('fecha_apertura') <= f"{today} 23:59:59"):
                    notas_del_dia += 1
                if(nota.get('fecha_cierre') and nota.get('nota_status') == 'cerrado'):
                   notas_cerradas += 1

            res['notas_abiertas'] = notas_abiertas
            res['notas_del_dia'] = notas_del_dia
            res['notas_cerradas'] = notas_cerradas

        elif page == 'PasesHistorial':
            employee = self.get_employee_data(user_id=self.user.get('user_id'), get_one=True)
            name = employee.get('worker_name')

            query_pases = [
                {"$match": {
                    "deleted_at": {"$exists": False},
                    "form_id": self.PASE_ENTRADA,
                    f"answers.{self.pase_entrada_fields['status_pase']}": {"$in": ["activo", "proceso"]},
                    f"answers.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.mf['nombre_guardia_apoyo']}": name
                }},
                {
                    "$group": {
                        "_id": f"$answers.{self.pase_entrada_fields['status_pase']}",
                        "total": {"$sum": 1}
                    }
                }
            ]
            pases = self.format_cr(self.cr.aggregate(query_pases))
            if pases:
                for item in pases:
                    if item.get('_id') == 'activo':
                        res['pases_activos'] = item.get('total')
                    if item.get('_id') == 'proceso':
                        res['pases_proceso'] = item.get('total')
        elif page == 'Asistencias':
            year_str = str(year).zfill(4)
            month_str = str(month).zfill(2)
            query_asistencias = [
                {'$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": self.REGISTRO_ASISTENCIA,
                    f"answers.{self.f['status_turn']}": {"$exists": True},
                    f"answers.{self.f['fecha_inicio_turno']}": {
                        "$gte": f"{year_str}-{month_str}-01 00:00:00",
                        "$lte": f"{year_str}-{month_str}-31 23:59:59"
                    }
                }},
                {'$project': {
                    '_id': 1,
                    'status_turn': f"$answers.{self.f['status_turn']}",
                }},
                {'$group': {
                    '_id': None,
                    'total_asistencias': {
                        '$sum': {
                            '$cond': {
                                'if': {'$eq': ['$status_turn', 'presente']},
                                'then': 1,
                                'else': 0
                            }
                        }
                    },
                    'total_retardos': {
                        '$sum': {
                            '$cond': {
                                'if': {'$in': ['$status_turn', ['retardo', 'falta_por_retardo']]},
                                'then': 1,
                                'else': 0
                            }
                        }
                    },
                }}
            ]
            data = self.format_cr(self.cr.aggregate(query_asistencias))
            if data:
                data = self.unlist(data)
                res['total_asistencias'] = data.get('total_asistencias', 0)
                res['total_retardos'] = data.get('total_retardos', 0)
        return res

    def check_in_aux_guard(self):
        match_query = {
            "deleted_at": {"$exists": False},
            "form_id": self.CHECKIN_CASETAS,
        }
        query = [
            {'$match': match_query},
            {'$unwind': f"$answers.{self.f['guard_group']}"},
            {'$project': {
                '_id': 1,
                'folio': "$folio",
                'created_at': "$created_at",
                'name': f"$answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.f['worker_name_jefes']}",
                'user_id': {"$first": f"$answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.mf['id_usuario']}"},
                'location': f"$answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['ubicacion']}",
                'area': f"$answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_area']}",
                'checkin_date': f"$answers.{self.f['guard_group']}.{self.f['checkin_date']}",
                'checkout_date': f"$answers.{self.f['guard_group']}.{self.f['checkout_date']}",
                'checkin_status': f"$answers.{self.f['guard_group']}.{self.f['checkin_status']}",
                'checkin_position': f"$answers.{self.f['guard_group']}.{self.f['checkin_position']}",
            }},
            {'$match': {'user_id': {'$ne': None}}},
            {'$sort': {'updated_at': -1}},
            {'$group': {
                '_id': {'user_id': '$user_id'},
                'name': {'$last': '$name'},
                'location': {'$last': '$location'},
                'area': {'$last': '$area'},
                'checkin_date': {'$last': '$checkin_date'},
                'checkout_date': {'$last': '$checkout_date'},
                'checkin_status': {'$last': '$checkin_status'},
                'checkin_position': {'$last': '$checkin_position'},
            }},
            {'$project': {
                '_id': 0,
                'user_id': '$_id.user_id',
                'name': '$name',
                'location': '$location',
                'area': '$area',
                'checkin_date': '$checkin_date',
                'checkout_date': '$checkout_date',
                'checkin_status': {'$cond': [{'$eq': ['$checkin_status', 'entrada']}, 'in', 'out']},
                'checkin_position': '$checkin_position',
            }},
        ]
        data = self.format_cr(self.cr.aggregate(query))
        res = {}
        for rec in data:
            status = 'in' if rec.get('checkin_status') in ['in', 'entrada'] else 'out'
            user_id = rec.get('user_id') or 0
            res[int(user_id)] = {
                'status': status,
                'name': rec.get('name'),
                'user_id': rec.get('user_id'),
                'location': rec.get('location'),
                'area': rec.get('area'),
                'checkin_date': rec.get('checkin_date'),
                'checkout_date': rec.get('checkout_date'),
                'checkin_position': rec.get('checkin_position')
            }
        return res

    def get_employee_checkin_status(self, user_ids, as_shift=False,  **kwargs):
        query = []
        if kwargs.get('user_id'):
            user_id = kwargs['user_id']
        else:
            user_id = self.user.get('user_id')
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.CHECKIN_CASETAS,
            }
        unwind = {'$unwind': f"$answers.{self.f['guard_group']}"}
        query = [{'$match': match_query }, unwind ]

        unwind_query = {f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.mf['id_usuario']}": {"$exists":True}}
        if as_shift:
            match_query.update({'created_by_id':user_id})
            query = [
                {'$match': match_query },
                {'$sort':{'created_at':-1}},
                {'$limit':1},
                unwind
                ]
        else:
            if type(user_ids) == list:
                unwind_query.update({f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.mf['id_usuario']}": {"$in": user_ids}})
            else:
                unwind_query.update({f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.mf['id_usuario']}": user_ids })
        query += [ {'$match': unwind_query }]
        query += [
            {'$addFields': {
                'priority': {
                    '$cond': [{'$eq': [f"$answers.{self.f['guard_group']}.{self.f['checkin_status']}", 'entrada']}, 1, 0]
                }
            }},
            {'$project':
                {'_id': 1,
                    'folio': "$folio",
                    'created_at': "$created_at",
                    'name': f"$answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.f['worker_name_jefes']}",
                    'user_id': {"$first":f"$answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.mf['id_usuario']}"},
                    'location': f"$answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['ubicacion']}",
                    'area': f"$answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_area']}",
                    'checkin_date': f"$answers.{self.f['guard_group']}.{self.f['checkin_date']}",
                    'checkout_date': f"$answers.{self.f['guard_group']}.{self.f['checkout_date']}",
                    'checkin_status': f"$answers.{self.f['guard_group']}.{self.f['checkin_status']}",
                    'checkin_position': f"$answers.{self.f['guard_group']}.{self.f['checkin_position']}",
                    'nombre_suplente': f"$answers.{self.f['guard_group']}.{self.checkin_fields['nombre_suplente']}",
                    'priority': '$priority'
                    }
            },
            {'$sort':{'priority':-1, 'created_at':-1}},
            {'$group':{
                '_id':{
                    'user_id':'$user_id',
                    },
                'name':{'$first':'$name'},
                'location':{'$first':'$location'},
                'area':{'$first':'$area'},
                'checkin_date':{'$first':'$checkin_date'},
                'checkout_date':{'$first':'$checkout_date'},
                'checkin_status':{'$first':'$checkin_status'},
                'checkin_position':{'$first':'$checkin_position'},
                'folio':{'$first':'$folio'},
                'id_register':{'$first':'$_id'},
                'nombre_suplente':{'$first':'$nombre_suplente'}
            }},
            {'$project':{
                '_id':0,
                'user_id':'$_id.user_id',
                'name':'$name',
                'location':'$location',
                'area':'$area',
                'checkin_date':'$checkin_date',
                'checkout_date':'$checkout_date',
                'checkin_status': {'$cond': [ {'$eq':['$checkin_status','entrada']},'in','out']}, 
                'checkin_position':'$checkin_position',
                'folio':'$folio',
                'id_register':'$id_register',
                'nombre_suplente':'$nombre_suplente'
            }}
            ]
        data = self.format_cr(self.cr.aggregate(query))
        res = {}
        for rec in data:
            status = 'in' if rec.get('checkin_status') in ['in','entrada'] else 'out'
            user_id = rec.get('user_id') or 0
            res[int(user_id)] = {
                'status':status, 
                'name': rec.get('name'), 
                'folio': rec.get('folio'),
                '_id': str(rec.get('id_register')),
                'user_id': rec.get('user_id'), 
                'location':rec.get('location'),
                'area':rec.get('area'),
                'checkin_date':rec.get('checkin_date'),
                'checkout_date':rec.get('checkout_date'),
                'checkin_position':rec.get('checkin_position'),
                'nombre_suplente':rec.get('nombre_suplente',"")
                }
        return res

    def check_in_out_employees(self,  checkin_type, check_datetime, checkin={}, employee_list=[], **kwargs):
        checkin_status = 'entrada' if checkin_type == 'in' else 'salida'
        date_id = 'checkin_date' if checkin_type == 'in' else 'checkout_date'
        checkin[self.f['guard_group']] = checkin.get(self.f['guard_group'],[])
        if checkin_type == 'out':
            for guard in checkin[self.f['guard_group']]:
                user_id = int(self.unlist(guard.get(self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID,{})\
                    .get(self.mf['id_usuario'],0)))
                if guard[self.checkin_fields['checkin_status']] != checkin_status:
                    if not employee_list:
                        guard[self.checkin_fields['checkin_status']] = checkin_status
                        guard[self.checkin_fields[date_id]] = check_datetime                    
                    elif user_id in employee_list:
                        guard[self.checkin_fields['checkin_status']] = checkin_status
                        guard[self.checkin_fields[date_id]] = check_datetime
        elif employee_list:
            for idx, guard in enumerate(employee_list):
                empl_cat = {}
                empl_cat[self.f['worker_name_b']] = guard.get('name')
                if isinstance(guard.get('usuario_id'), list):
                    empl_cat[self.mf['id_usuario']] = [(guard.get('usuario_id', [])[0]),]
                else:
                    empl_cat[self.mf['id_usuario']] = [guard.get('user_id'),]
                guard_data = {
                        self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID : empl_cat,
                        self.checkin_fields['checkin_position']:'guardia_de_apoyo',
                        self.checkin_fields['checkin_status']:checkin_status,
                        self.checkin_fields[date_id]:check_datetime,
                        self.checkin_fields['nombre_suplente']:guard.get("nombre_suplente",''),
                       }
                if kwargs.get('employee_type'):
                    guard_data.update({self.checkin_fields['checkin_position']: kwargs['employee_type'] })
                elif idx == 0:
                    guard_data.update({self.checkin_fields['checkin_position']: self.chife_guard})
                else:
                    guard_data.update({self.checkin_fields['checkin_position']: self.support_guard})
                checkin[self.f['guard_group']] += [guard_data,]
        return checkin

    def do_attendance(self, asistencia_answers):
        metadata = self.lkf_api.get_metadata(form_id=self.REGISTRO_ASISTENCIA)
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": 'Accesos',
                    "Process": 'Inicio de turno',
                    "Action": 'asistencia',
                    "File": 'accesos/app.py',
                }
            },
        })
        metadata.update({'answers':asistencia_answers})
        #! Se registra la asistencia.
        response = self.lkf_api.post_forms_answers(metadata)
        if response.get('status_code') in [200, 201, 202]:
            return True
        else:
            return self.LKFException({'title': 'Error en registro de asistencia', 'msg': {'response': response}})

    def do_checkin(self, location, area, employee_list=[], fotografia=[], check_in_manual={}, nombre_suplente="", checkin_id=""):
        """
        Se encarga de hacer el check in de un guardia.

        Args:
            location (str): Ubicacion de la caseta.
            area (str): Area de la caseta.
            employee_list (list, optional): Lista de guardias a checkear. Defaults to [].
            fotografia (list, optional): Lista de fotos para el check in. Defaults to [].
            check_in_manual (dict, optional): Datos del check in manual. Defaults to {}.
            nombre_suplente (str, optional): Nombre del suplente. Defaults to "".
            checkin_id (str, optional): ID del check in. Defaults to "".

        Returns:
            dict: Resultado del check in.
        """
        
        #! Se verifica si la caseta esta abierta.
        is_caseta_open = self.is_boot_available(location, area)
        user_id = self.user.get('user_id')
        user = self.lkf_api.get_user_by_id(user_id)
        user_name = user.get('name', '')
        
        #! Si la caseta esta abierta se actualizan los guardias solamente.
        if is_caseta_open:
            res = self.update_guards_checkin([{'user_id': user_id, 'name': user_name}], checkin_id, location, area, user, nombre_suplente, fotografia)
            format_res = self.unlist(res)
            if format_res.get('status_code') in [200, 201, 202]:
                return format_res
            else:
                self.LKFException({'title': 'Error al hacer check-in', 'msg': format_res.get('json')})

        #! Se hace una lista de los ids de los guardias, el usuario actual y la lista de guardias por parametro.
        user_ids = [user_id] + [x['user_id'] for x in employee_list]
        #! Se obtienen los guardias por ubicacion y area.
        boot_config = self.get_users_by_location_area(
            location_name=location, 
            area_name=area, 
            user_id=user_ids)

        #! Si el guardia no tiene configurada la caseta actual arroja Exception.
        if not boot_config:
            msg = f"El usuario no puede hacer check-in en la caseta: {area} - {location}."
            msg += f"Por favor verifica la configuracion."
            return self.LKFException({'title': 'Advertencia', 'msg': msg})
        else:
            #! Se hace una lista de los ids de los guardias permitidos.
            allowed_users = [x['user_id'] for x in boot_config]
            common_values = list(set(user_ids) & set(allowed_users))
            not_allowed = [user_id for user_id in user_ids if user_id not in common_values]

        #! Si hay algun guardia que no tiene permiso para hacer check-in arroja Exception.
        if not_allowed:
            msg = f"Usuarios con ids {not_allowed}. "
            msg += f"No tienen permitido hacer check-in en esta caseta: {area} - {location}."
            return self.LKFException({'title': 'Advertencia', 'msg': msg})

        #! Si alguno de los guardias ya tiene un check-in abierto arroja Exception.
        validate_status = self.get_employee_checkin_status(user_ids)
        not_allowed = [user_id for user_id, user_data in validate_status.items() if user_data.get('status') == 'in']
        if not_allowed:
            msg = f"El usuario(s) con id(s) {not_allowed}. Se encuentran actualmente registrados en otra caseta."
            msg += f" Es necesario hacer check-out de cualquier caseta antes de querer entrar a una nueva."
            return self.LKFException({'title': 'Advertencia', 'msg': msg})

        #! Se obtiene el empleado actual.
        employee = self.get_employee_data(user_id=user_id, get_one=True)
        if not employee:
            msg = f"No se encontro ningun empleado con id: {user_id}"
            return self.LKFException({'title': 'Advertencia', 'msg': msg})
        user_data = self.lkf_api.get_user_by_id(user_id)
        employee['timezone'] = user_data.get('timezone', 'America/Monterrey')
        employee['name'] = employee['worker_name']
        employee['position'] = self.chife_guard
        employee['nombre_suplente'] = nombre_suplente
        timezone = employee.get('cat_timezone', employee.get('timezone', 'America/Monterrey'))
        data = self.lkf_api.get_metadata(self.CHECKIN_CASETAS)
        now_datetime = self.today_str(timezone, date_format='datetime')

        #! Se obtiene la informacion formateada para hacer el check in.
        checkin = self.checkin_data(employee, location, area, 'in', now_datetime)
        employee_list.insert(0, employee)
        checkin = self.check_in_out_employees('in', now_datetime, checkin=checkin, employee_list=employee_list)
        checkin[self.f['configuracion_de_accesos']] = self.get_booth_config(location)

        #! Se actualiza el check in con la informacion faltante.
        data.update({
                'properties': {
                    "device_properties":{
                        "system": "Modulo Accesos",
                        "process": 'Checkin-Checkout',
                        "action": 'do_checkin',
                        "archive": "accesos_utils.py"
                    }
                },
                'answers': checkin
            })
        if check_in_manual:
            checkin.update({
                self.checkin_fields['checkin_image']: check_in_manual.get('image', []),
                self.checkin_fields['commentario_checkin_caseta']: check_in_manual.get('comment', '')
            })
        if fotografia:
            checkin.update({
                self.checkin_fields['fotografia_inicio_turno']: fotografia
            })

        asistencia_answers = {
            self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID: {
                self.Location.f['location']: location,
                self.Location.f['area']: area
            },
            self.f['tipo_guardia']: 'guardia_regular',
            self.checkin_fields['checkin_type']: 'iniciar_turno',
            self.f['image_checkin']: fotografia
        }

        if nombre_suplente:
            asistencia_answers.update({
                self.f['tipo_guardia']: 'guardia_suplente',
                self.f['nombre_guardia_suplente']: nombre_suplente
            })

        registro_de_asistencia = self.do_attendance(asistencia_answers)
        
        resp_create = self.lkf_api.post_forms_answers(data)
        if resp_create.get('status_code') == 201:
            resp_create['json'].update({'boot_status':{'guard_on_duty':user_data['name']}})
            resp_create.update({'registro_de_asistencia': 'Correcto'})
        return resp_create

    def do_checkout(self, checkin_id=None, location=None, area=None, guards=[], forzar=False, comments=False, fotografia=[], guard_id=None):
        """
        Se encarga de hacer el check out de un empleado.

        Args:
            checkin_id (str): Id del check in.
            location (str): Ubicacion.
            area (str): Area.
            guards (list): Lista de guardias.
            forzar (bool): Forzar el check out.
            comments (bool): Comentarios.
            fotografia (list): Fotografia.

        Returns:
            dict: Response.
        """

        if guard_id:
            user_id = guard_id
        elif guards:
            user_id = guards[0]
        else:
            user_id = self.user.get('user_id')
        
        employee =  self.get_employee_data(user_id=user_id, get_one=True)
        timezone = employee.get('cat_timezone', employee.get('timezone', 'America/Monterrey'))
        now_datetime =self.today_str(timezone, date_format='datetime')
        last_chekin = {}

        if not checkin_id:
            return self.LKFException({"msg":"No encontramos un checking valido del cual podemos hacer checkout...", "title": "Advertencia"})
        
        is_caseta_open = self.is_boot_available(location, area)
        if not is_caseta_open:
            msg = f"No se puede hacer check-out sin antes haber hecho check-in. Caseta: {location} - {area}."
            return self.LKFException({"msg":msg, "title": "Advertencia"})
        
        record = self.get_record_by_id(checkin_id)
        checkin_answers = record['answers']
        folio = record['folio']
        area = checkin_answers.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID,{}).get(self.f['area'])
        location = checkin_answers.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID,{}).get(self.f['location'])
        rec_guards = checkin_answers.get(self.checkin_fields['guard_group'])
        guards_in = sum(
            1
            for guard in rec_guards
            if not guard.get(self.checkin_fields['checkout_date'])
        )
        for guard in rec_guards:
            fecha_cierre_turno = guard.get(self.checkin_fields['checkout_date'])
            guard_id = self.unlist(guard.get(self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID, {}).get(self.mf['id_usuario']))
            actual_guard_id = self.unlist(employee.get('usuario_id'))
            if not fecha_cierre_turno and guards_in > 1 and guard_id == actual_guard_id:
                resp = self.do_checkout_aux_guard(user_id=guard_id, checkin_id=checkin_id, guards=[actual_guard_id], location=location, area=area, fotografia=fotografia)
                return resp

        if not guards:
            checkin_answers[self.checkin_fields['commentario_checkin_caseta']] = \
                checkin_answers.get(self.checkin_fields['commentario_checkin_caseta'],'')
            checkin_answers[self.checkin_fields['checkin_type']] = 'cerrada'
            checkin_answers[self.checkin_fields['boot_checkout_date']] = now_datetime
            checkin_answers[self.checkin_fields['forzar_cierre']] = 'regular'

            if comments:
                checkin_answers[self.checkin_fields['commentario_checkin_caseta']] += comments + ' '
            if forzar:
                checkin_answers[self.checkin_fields['commentario_checkin_caseta']] += f"Cerrado por: {employee.get('worker_name')}"
                checkin_answers[self.checkin_fields['forzar_cierre']] = 'forzar'
        
        data = self.lkf_api.get_metadata(self.CHECKIN_CASETAS)
        checkin_answers = self.check_in_out_employees('out', now_datetime, checkin=checkin_answers, employee_list=guards)
        data['answers'] = checkin_answers

        if fotografia:
            checkin_answers.update({
                self.checkin_fields['fotografia_cierre_turno']: fotografia
            })

        response = self.lkf_api.patch_record( data=data, record_id=checkin_id)
        if response.get('status_code') in [200, 201, 202]:
            print('entra aquiiiiiiii')
            print('employee', employee)
            if employee:
                print('employee', employee)
                print('location', location)
                print('area', area)
                record_id = self.search_guard_asistance(location, area, self.unlist(employee.get('usuario_id')))
                print('record_id', record_id)
                asistencia_answers = {
                    self.f['foto_cierre_turno']: fotografia,
                    self.checkin_fields['checkin_type']: 'cerrar_turno',
                }
                print('asistencia_answers', asistencia_answers)
                res = self.lkf_api.patch_multi_record(answers=asistencia_answers, form_id=self.REGISTRO_ASISTENCIA, record_id=record_id)
                print('res', res)
                if res.get('status_code') in [200, 201, 202]:
                    response.update({'registro_de_asistencia': 'Correcto'})
                else:
                    response.update({'registro_de_asistencia': 'Error'})
        elif response.get('status_code') == 401:
            return self.LKFException({"title": "Advertencia", "msg":"El guardia NO tiene permisos sobre el formulario de cierre de casetas"})
        return response

    def get_cantidades_de_pases(self, x_empresa=False):
        print('entra a get_cantidades_de_pases')
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PASE_ENTRADA,
        }

        proyect_fields = {
            '_id':1,
            'folio': f"$folio",
            'estatus':f"$answers.{self.pase_entrada_fields['status_pase']}",
            'empresa': { "$first" : f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}"},
            'nombre': f"$answers.{self.mf['nombre_pase']}",
            'nombre_perfil': f"$answers.{self.pase_entrada_fields['nombre_perfil']}",
            'fecha_hasta_pase': f"$answers.{self.pase_entrada_fields['fecha_hasta_pase']}",
            'created_at': 1
        }

        match_query.update({f"answers.{self.pase_entrada_fields['status_pase']}":{'$exists': True}})

        post_project_match = {}

        group_by = {
                '_id':{
                    'estatus': '$estatus',
                    },
                'cantidad': {'$sum': 1},
                }
        
        if x_empresa:
            post_project_match = {
                "$and": [
                    {'empresa': {"$ne": None}},
                    {'empresa': {"$ne": ""}}
                ]
            }

            group_by = {
                '_id':{
                    'empresa':'$empresa',
                    'estatus': '$estatus',
                    },
                'cantidad': {'$sum': 1},
            }

        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            {'$match': post_project_match},
            {'$group': group_by}
        ]

        records = self.format_cr(self.cr.aggregate(query))
        print('/////////records', records)
        return  records
    
    def get_cantidades_de_pases_x_persona(self, contratista=None):
        print('entra a get_cantidades_de_pases_x_persona')
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PASE_ENTRADA,
        }

        if contratista:
            match_query.update({f"answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}":contratista})

        proyect_fields = {
            '_id':1,
            'folio': f"$folio",
            'estatus':f"$answers.{self.pase_entrada_fields['status_pase']}",
            'empresa': { "$first" : f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}"},
            'nombre': f"$answers.{self.mf['nombre_pase']}",
            'nombre_perfil': f"$answers.{self.pase_entrada_fields['nombre_perfil']}",
            'fecha_hasta_pase': f"$answers.{self.pase_entrada_fields['fecha_hasta_pase']}",
            'created_at': 1
        }

        match_query.update({f"answers.{self.pase_entrada_fields['status_pase']}":{'$exists': True}})

        group_by = {
                '_id':{
                    'folio':'$folio',
                    'nombre': '$nombre',
                    'empresa': '$empresa',
                    'nombre_perfil': '$nombre_perfil',
                    'fecha_hasta_pase': '$fecha_hasta_pase',
                    }
                }
        

        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            {'$group': group_by}
        ]

        records = self.format_cr(self.cr.aggregate(query))
        print('/////////records', records)
        return  records
    
    def get_catalogo_paquetes(self):
        catalog_id = self.PROVEEDORES_CAT_ID
        form_id= self.PAQUETERIA
        return self.lkf_api.catalog_view(catalog_id, form_id) 

    def create_paquete(self, data_paquete):
        metadata = self.lkf_api.get_metadata(form_id=self.PAQUETERIA)
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": "Accesos",
                    "Process": "Creación de Paquetes",
                    "Action": "nuevo_paquete",
                    "File": "accesos/app.py"
                }
            },
        })
        answers = {}
        for key, value in data_paquete.items():
            if key == 'ubicacion_paqueteria':
                answers[self.UBICACIONES_CAT_OBJ_ID] = { self.mf['ubicacion']: value}
            elif  key == 'area_paqueteria':
                 answers[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID] = { self.mf['nombre_area']: value}
            elif  key == 'guardado_en_paqueteria':
                answers[self.LOCKERS_CAT_OBJ_ID] ={self.mf['locker_id']:value} 
            elif key == 'proveedor':
                answers[self.PROVEEDORES_CAT_OBJ_ID] = {self.paquetes_fields['proveedor']:value}
            elif key == 'quien_recibe_paqueteria':
                answers[self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID] = {self.mf['nombre_empleado']:value}
            else:
                answers.update({f"{self.paquetes_fields[key]}":value})
        metadata.update({'answers':answers})
        res=self.lkf_api.post_forms_answers(metadata)
        return res

    def update_paquete(self, data_paquete, folio):
        #---Define Answers
        answers = {}
        for key, value in data_paquete.items():
            if  key == 'ubicacion_perdido':
                answers[self.cons_f['ubicacion_catalog_concesion']] = { self.mf['ubicacion']: value}
            elif  key == 'area_paqueteria':
                 answers[self.cons_f['area_catalog_concesion']] = { self.mf['nombre_area_salida']: value}
            elif  key == 'guardado_en_paqueteria':
                answers[self.LOCKERS_CAT_OBJ_ID] ={self.mf['locker_id']:value} 
            elif key == 'proveedor':
                answers[self.PROVEEDORES_CAT_OBJ_ID] = {self.paquetes_fields['proveedor']:value}
            elif key == 'quien_recibe_paqueteria':
                answers[self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID] = {self.mf['nombre_empleado']:value}
            else:
                answers.update({f"{self.paquetes_fields[key]}":value})
        if answers or folio:
            return self.lkf_api.patch_multi_record( answers = answers, form_id=self.PAQUETERIA, folios=[folio])
        else:
            self.LKFException('No se mandarón parametros para actualizar')

    def get_list_bitacora(self, location=None, area=None, prioridades=[], dateFrom='', dateTo='', filterDate="", limit=20, offset=0):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.BITACORA_ACCESOS
        }

        if location:
            match_query.update({f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}":location})
        if area:
            match_query.update({f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['nombre_area']}":area})
        if prioridades:
            match_query[f"answers.{self.bitacora_fields['status_visita']}"] = {"$in": prioridades}
  
        zona = self.user.get('timezone','America/Monterrey')
        if filterDate != "range":
            dateFrom, dateTo = self.get_range_dates(filterDate, zona)
            if dateFrom:
                dateFrom = str(dateFrom)
            if dateTo:
                dateTo = str(dateTo)

        if dateFrom and dateTo:
           match_query.update({
                f"answers.{self.mf['fecha_entrada']}": {"$gte": dateFrom, "$lte": dateTo},
            })
        elif dateFrom:
            match_query.update({
                f"answers.{self.mf['fecha_entrada']}": {"$gte": dateFrom}
            })
        elif dateTo:
            match_query.update({
                f"answers.{self.mf['fecha_entrada']}": {"$lte": dateTo}
            })
        
        proyect_fields ={
            '_id': 1,
            'folio': "$folio",
            'created_at': "$created_at",
            'updated_at': "$updated_at",
            'a_quien_visita':f"$answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_empleado']}",
            'documento': f"$answers.{self.mf['documento']}",
            'caseta_entrada':f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['nombre_area']}",
            'codigo_qr':f"$answers.{self.mf['codigo_qr']}",
            'comentarios':f"$answers.{self.bitacora_fields['grupo_comentario']}",
            'fecha_salida':f"$answers.{self.mf['fecha_salida']}",
            'fecha_entrada':f"$answers.{self.mf['fecha_entrada']}",
            'fotografia': f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['foto']}",
            'equipos':f"$answers.{self.mf['grupo_equipos']}",
            'grupo_areas_acceso': f"$answers.{self.mf['grupo_areas_acceso']}",
            'id_gafet': f"$answers.{self.GAFETES_CAT_OBJ_ID}.{self.gafetes_fields['gafete_id']}",
            'id_locker': f"$answers.{self.LOCKERS_CAT_OBJ_ID}.{self.lockers_fields['locker_id']}",
            'identificacion':  f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['identificacion']}",
            'pase_id': {"$toObjectId":f"$answers.{self.mf['codigo_qr']}"},
            'motivo_visita':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}.{self.mf['motivo']}",
            'nombre_area_salida':f"$answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.mf['nombre_area_salida']}",
            'nombre_visitante':f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['nombre_visita']}",
            'contratista':f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['empresa']}",
            'perfil_visita':{'$arrayElemAt': [f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['nombre_perfil']}",0]},
            'status_gafete':f"$answers.{self.mf['status_gafete']}",
            'status_visita':f"$answers.{self.mf['tipo_registro']}",
            'ubicacion':f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}",
            'vehiculos':f"$answers.{self.mf['grupo_vehiculos']}",
            'visita_a': f"$answers.{self.mf['grupo_visitados']}",
            'sala': f"$answers.{self.f['bitacora_sala']}",
            'url_de_etiqueta': f"$answers.{self.f['url_de_etiqueta']}"
        }

        lookup = {
            'from': 'form_answer',
            'localField': 'pase_id',
            'foreignField': '_id',
            "pipeline": [
                {'$match':{
                    "deleted_at":{"$exists":False},
                    "form_id": self.PASE_ENTRADA,
                }},
                {'$project':{
                    "_id": 0, 
                    'motivo_visita':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}.{self.mf['motivo']}",
                    'grupo_areas_acceso': f"$answers.{self.mf['grupo_areas_acceso']}",                    
                }},
            ],
            'as': 'pase',
        }
       
        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            {'$lookup': lookup},
            {'$sort':{'created_at':-1}},
        ]

        count_query = [
            {"$match": match_query},
            {"$count": "total"}
        ]
        
        count_result = self.format_cr(self.cr.aggregate(count_query))
        total_count = count_result[0]['total'] if count_result else 0
        current_page = (offset // limit) + 1 if limit else 1
        total_pages = ceil(total_count / limit) if limit else 1

        query.append({'$skip': offset})
        query.append({'$limit': limit})

        records = self.format_cr(self.cr.aggregate(query))

        for r in records:
            pase = r.pop('pase')
            pase_id = r.pop('pase_id')
            if len(pase) > 0 :
                pase = pase[0]
                r['motivo_visita'] = self.unlist(pase.get('motivo_visita',''))
                r['grupo_areas_acceso'] = self._labels_list(pase.get('grupo_areas_acceso',[]), self.mf)
            r['id_gafet'] = r.get('id_gafet','')
            r['status_visita'] = r.get('status_visita','').title().replace('_', ' ')
            r['contratista'] = self.unlist(r.get('contratista',[]))
            r['status_gafete'] = r.get('status_gafete','').title().replace('_', ' ')
            r['documento'] = r.get('documento','')
            r['grupo_areas_acceso'] = self._labels_list(r.pop('grupo_areas_acceso',[]), self.mf)
            r['comentarios'] = self.format_comentarios(r.get('comentarios',[]))
            r['vehiculos'] = self.format_vehiculos(r.get('vehiculos',[]))
            r['equipos'] = self.format_equipos(r.get('equipos',[]))
            r['visita_a'] = self.format_visita(r.get('visita_a',[]))
            r['pase_id']=str(pase_id)

        return {
            "records": records,
            "total_records": total_count,
            "total_pages": total_pages,
            "actual_page": current_page,
            "records_on_page": len(records)
        }

    def get_pdf_seg(self, qr_code, template_id=None, name_pdf=None):
        return self.lkf_api.get_pdf_record(qr_code, template_id = template_id, name_pdf =name_pdf, send_url=True)

    def get_list_rondines(self, prioridades=[], dateFrom='', dateTo='', filterDate=""):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.BITACORA_RONDINES
        }
        # if location:
        #     match_query.update({f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}":location})
        # if area:
        #     match_query.update({f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['nombre_area']}":area})
        # if prioridades:
        #     match_query[f"answers.{self.bitacora_fields['status_visita']}"] = {"$in": prioridades}
  
        user_data = self.lkf_api.get_user_by_id(self.user.get('user_id'))
        zona = user_data.get('timezone','America/Monterrey')

        if filterDate != "range":
            dateFrom, dateTo = self.get_range_dates(filterDate,zona)

            if dateFrom:
                dateFrom = str(dateFrom)
            if dateTo:
                dateTo = str(dateTo)

        if dateFrom and dateTo:
           match_query.update({
                f"answers.{self.f['fecha_inicio_rondin']}": {"$gte": dateFrom, "$lte": dateTo},
            })
        elif dateFrom:
            match_query.update({
                f"answers.{self.f['fecha_inicio_rondin']}": {"$gte": dateFrom}
            })
        elif dateTo:
            match_query.update({
                f"answers.{self.f['fecha_inicio_rondin']}": {"$lte": dateTo}
            })
        
        proyect_fields ={
            '_id': 1,
            'folio': "$folio",
            'duracion_rondin': f"$answers.{self.f['duracion_rondin']}",
            'duracion_traslado_area':f"$answers.{self.f['duracion_traslado_area']}",
            'fecha_inspeccion_area':f"$answers.{self.f['fecha_inspeccion_area']}",
            'fecha_programacion':f"$answers.{self.f['fecha_programacion']}",
            'fecha_inicio_rondin':f"$answers.{self.f['fecha_inicio_rondin']}",
            'grupo_areas_visitadas':f"$answers.{self.f['grupo_areas_visitadas']}",
            
            # 'areas_del_rondin': '66462aa5d4a4af2eea07e0d1',
            # 'comentario_area_rondin': '66462b9d7124d1540f962088',
            # 'comentario_check_area': '681144fb0d423e25b42818d4',
            # 'estatus_del_recorrido': '6639b2744bb44059fc59eb62',
            # 'fecha_hora_inspeccion_area': '6760a908a43b1b0e41abad6b',
            # 'fecha_programacion':'6760a8e68cef14ecd7f8b6fe',
            # 'foto_evidencia_area': '681144fb0d423e25b42818d2',
            # 'foto_evidencia_area_rondin': '66462b9d7124d1540f962087',
            # 'grupo_de_areas_recorrido': '6645052ef8bc829a5ccafaf5',
            # 'nombre_area':'663e5d44f5b8a7ce8211ed0f',
            # 'nombre_del_recorrido': '6645050d873fc2d733961eba',
            # 'nombre_del_recorrido_en_catalog': '6644fb97e14dcb705407e0ef',
            # 'ubicacion_recorrido': '663e5c57f5b8a7ce8211ed0b',
            # 'fecha_inicio_rondin': '6818ea068a7f3446f1bae3b3',
            # 'fecha_fin_rondin': '6760a8e68cef14ecd7f8b6ff',
            # 'check_status': '681fa6a8d916c74b691e174b',
            # 'grupo_incidencias_check': '681144fb0d423e25b42818d3',
            # 'incidente_open': '6811455664dc22ecae83f75b',
            # 'incidente_comentario': '681145323d9b5fa2e16e35cc',
            # 'incidente_area': '663e5d44f5b8a7ce8211ed0f',
            # 'incidente_location': '663e5c57f5b8a7ce8211ed0b',
            # 'incidente_evidencia': '681145323d9b5fa2e16e35cd',
            # 'incidente_documento': '685063ba36910b2da9952697',
            # 'url_registro_rondin': '6750adb2936622aecd075607',
            # 'bitacora_rondin_incidencias': '686468a637d014b9e0ab5090',
            # 'tipo_de_incidencia': '663973809fa65cafa759eb97'
            }
        # lookup = {
        #  'from': 'form_answer',
        #  'localField': 'pase_id',
        #  'foreignField': '_id',
        #  "pipeline": [
        #         {'$match':{
        #             "deleted_at":{"$exists":False},
        #             "form_id": self.PASE_ENTRADA,
        #             }
        #         },
        #         {'$project':{
        #             "_id":0, 
        #             'motivo_visita':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}.{self.mf['motivo']}",
        #             'grupo_areas_acceso': f"$answers.{self.mf['grupo_areas_acceso']}",                    
        #             }
        #         },
        #         ],
        #  'as': 'pase',
        # }
       
        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            # {'$lookup': lookup},
        ]
        # if not filterDate:
        #     query.append(
        #         {"$limit":1}
        #     )
        if dateFrom:
            query.append(
                {'$sort':{'created_at':-1}},
            )
        else:
            query.append(
                {'$sort':{'created_at':-1}},
            )
           
        records = self.format_cr(self.cr.aggregate(query))
        # print( simplejson.dumps(records, indent=4))
        # for r in records:
        #     pase = r.pop('pase')
        #     r.pop('pase_id')
        #     if len(pase) > 0 :
        #         pase = pase[0]
        #         r['motivo_visita'] = self.unlist(pase.get('motivo_visita',''))
        #         r['grupo_areas_acceso'] = self._labels_list(pase.get('grupo_areas_acceso',[]), self.mf)
        #     r['id_gafet'] = r.get('id_gafet','')
        #     r['status_visita'] = r.get('status_visita','').title().replace('_', ' ')
        #     r['contratista'] = self.unlist(r.get('contratista',[]))
        #     r['status_gafete'] = r.get('status_gafete','').title().replace('_', ' ')
        #     r['documento'] = r.get('documento','')
        #     r['grupo_areas_acceso'] = self._labels_list(r.pop('grupo_areas_acceso',[]), self.mf)
        #     r['comentarios'] = self.format_comentarios(r.get('comentarios',[]))
        #     r['vehiculos'] = self.format_vehiculos(r.get('vehiculos',[]))
        #     r['equipos'] = self.format_equipos(r.get('equipos',[]))
        #     r['visita_a'] = self.format_visita(r.get('visita_a',[]))
        print("rondines", simplejson.dumps( records,indent=4))
        return  records

    def get_rondines_by_status(self, status_list=['programado', 'en_proceso']):
        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                f"answers.{self.f['estatus_del_recorrido']}": {"$in": status_list},
            }},
            {'$project': {
                '_id': 1,
                'timezone': 1,
                'fecha_programacion': f"$answers.{self.f['fecha_programacion']}",
                'rondinero_id': f"$answers.{self.USUARIOS_OBJ_ID}.{self.mf['id_usuario']}",
                'answers': f"$answers"
            }},
        ]

        rondines = self.format_cr(self.cr.aggregate(query))
        return rondines

    def close_rondines(self, list_of_rondines, timezone='America/Mexico_City'):
        #- Expirados son lo que esta en status programados y que tienen mas de 24 de programdos
        # - en progreso son lo que estan con status progreso y tienen mas de 1 hr de su ultimo check.
        answers = {}
        tiz = pytz.timezone(timezone)
        ahora_cierre = datetime.now(tiz)

        rondines_expirados = []
        rondines_en_proceso_vencidos = []

        for rondin in list_of_rondines:
            estatus = rondin.get('estatus_del_recorrido')
            fecha_programacion_str = rondin.get('fecha_programacion')
            user_id = self.unlist(rondin.get('rondinero_id', 0))
            user_data = self.lkf_api.get_user_by_id(user_id)
            user_timezone = user_data.get('timezone', 'America/Mexico_City')
            tz = pytz.timezone(user_timezone)
            ahora = datetime.now(tz)

            if estatus == 'programado' and fecha_programacion_str:
                fecha_programacion = tz.localize(datetime.strptime(fecha_programacion_str, '%Y-%m-%d %H:%M:%S'))
                if ahora > fecha_programacion + timedelta(hours=24):
                    rondines_expirados.append(rondin)
            elif estatus == 'en_proceso':
                areas = rondin.get('areas_del_rondin', [])
                ultima_fecha = None
                for area in areas:
                    fecha_str = area.get('fecha_hora_inspeccion_area', '')
                    if fecha_str:
                        fecha = tz.localize(datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S'))
                        if not ultima_fecha or fecha > ultima_fecha:
                            ultima_fecha = fecha
                if ultima_fecha and ahora > ultima_fecha + timedelta(minutes=15):
                    rondines_en_proceso_vencidos.append(rondin)

        rondines_ids = []
        rondines_expirados = rondines_expirados + rondines_en_proceso_vencidos
        for rondin in rondines_expirados:
            rondines_ids.append(rondin.get('_id'))

        answers[self.f['estatus_del_recorrido']] = 'cerrado'
        answers[self.f['fecha_fin_rondin']] = ahora_cierre.strftime('%Y-%m-%d %H:%M:%S')

        # print(stop)
        if answers:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=rondines_ids)
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                return res
            else: 
                return res

    def extends_date_of_pass(self, qr_code, update_obj):
        if not qr_code:
            return self.LKFException({'title': 'Error', 'msg': 'No se proporciono el QR code'})
        if not update_obj.get('fecha_desde'):
            return self.LKFException({'title': 'Error', 'msg': 'No se proporciono una fecha valida'})
        
        answers = {}
        answers[self.mf['fecha_desde_visita']] = update_obj.get('fecha_desde')
        answers[self.mf['fecha_desde_hasta']] = update_obj.get('fecha_hasta', None)

        if answers:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.PASE_ENTRADA, record_id=[qr_code,])
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                return res
            else:
                return res
        return False

    def assign_rondin(self, record_id, user_to_assign):
        if not record_id:
            return self.LKFException({'title': 'Error', 'msg': 'No se proporciono el record_id'})
        if not user_to_assign.get('user_name'):
            return self.LKFException({'title': 'Error', 'msg': 'No se proporciono el usuario a asignar'})
        
        answers = {}
        answers[self.USUARIOS_OBJ_ID] = {
            self.mf['nombre_usuario']: user_to_assign.get('user_name', ''),
            self.mf['id_usuario']: [user_to_assign.get('user_id')],
            self.mf['email_visita_a']: [user_to_assign.get('user_email')]
        }

        if answers:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=[record_id,])
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                return res
            else:
                return res
        return False

    def LKFResponse(self, msg={}):
        """
        Proporciona un mensaje de respuesta con el formato utilizado en LKF

        Args:
            msg ({
                title: str,
                label: str,
                msg: str,
                icon: str,
                type: str,
                status: int
            }): Un diccionario con la informacion del mensaje

        Returns:
            dict: Un diccionario con la informacion del mensaje
        """
        title_default = "Addons Statement"
        type_default  = "success"
        label_default = "Addons Statement"
        icon_default = "fa-circle-check"
        status_default = 200
        msg_dict = {}

        if not isinstance(msg, dict):
            return 'Error: El mensaje debe ser un diccionario'

        msg_dict['title'] = msg.get('title', title_default)
        msg_dict['label'] = msg.get('label', label_default)
        msg_dict['msg'] = [msg.get('msg', "Something went wrong")]
        msg_dict['icon'] = msg.get('icon', icon_default)
        msg_dict['type'] = msg.get('type', type_default)
        msg_dict["status"] = msg.get('status', status_default)

        return msg_dict

    def get_list_notes(self, location, area, status=None, limit=10, offset=0, dateFrom="", dateTo=""):
        '''
        Función para obtener las notas, puedes pasarle un area, una ubicacion, un estatus, una fecha desde
        y una fecha hasta
        '''
        response = []
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.ACCESOS_NOTAS,
            f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['location']}":location
        }
        if area and not area == 'todas':
            match_query.update({
                f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['area']}":area
            })
        if status != 'dia':
            match_query.update({f"answers.{self.notes_fields['note_status']}":status})
        if dateFrom and dateTo:
            if dateFrom == dateTo:
                if "T" not in dateFrom:
                    dateFrom += " 00:00:00"
                    dateTo += " 23:59:59"
            else:
                if "T" not in dateFrom:
                    dateFrom += " 00:00:00"
                if "T" not in dateTo:
                    dateTo += " 23:59:59"

            match_query.update({
                f"answers.{self.notes_fields['note_open_date']}": {"$gte": dateFrom, "$lte": dateTo}
            })
        elif dateFrom:
            if "T" not in dateFrom:
                dateFrom += " 00:00:00"
            match_query.update({
                f"answers.{self.notes_fields['note_open_date']}": {"$gte": dateFrom}
            })
        elif dateTo:
            if "T" not in dateTo:
                dateTo += " 23:59:59"
            match_query.update({
                f"answers.{self.notes_fields['note_open_date']}": {"$lte": dateTo}
            })
        query = [
            {'$match': match_query },
            {'$project': {
                "folio":"$folio",
                "created_at": 1,
                "created_by_name": f"$created_by_name",
                "created_by_id": f"$created_by_id",
                "created_by_email": f"$created_by_email",
                "note_status": f"$answers.{self.notes_fields['note_status']}",
                "note_open_date": f"$answers.{self.notes_fields['note_open_date']}",
                "note_close_date": f"$answers.{self.notes_fields['note_close_date']}",
                "note_booth": f"$answers.{self.notes_fields['note_catalog_booth']}.{self.notes_fields['note_booth']}",
                "note_guard": f"$answers.{self.notes_fields['note_catalog_guard']}.{self.notes_fields['note_guard']}",
                "note_guard_close": f"$answers.{self.notes_fields['note_catalog_guard_close']}.{self.notes_fields['note_guard_close']}",
                "note": f"$answers.{self.notes_fields['note']}",
                "note_file": f"$answers.{self.notes_fields['note_file']}",
                "note_pic": f"$answers.{self.notes_fields['note_pic']}",
                "note_comments": f"$answers.{self.notes_fields['note_comments_group']}",
            }},
            {'$sort':{'created_at':-1}},
        ]
        
        query.append({'$skip': offset})
        query.append({'$limit': limit})
        
        records = self.format_cr(self.cr.aggregate(query))

        count_query = [
            {'$match': match_query},
            {'$count': 'total'}
        ]

        count_result = self.format_cr(self.cr.aggregate(count_query))
        total_count = count_result[0]['total'] if count_result else 0
        total_pages = ceil(total_count / limit) if limit else 1
        current_page = (offset // limit) + 1 if limit else 1

        notes = {
            'records': records,
            'total_records': total_count,
            'total_pages': total_pages,
            'actual_page': current_page
        }

        return notes
    
    def get_areas_by_locations(self, location_names):
        catalog_id = self.AREAS_DE_LAS_UBICACIONES_CAT_ID
        form_id = self.PASE_ENTRADA
        res_list = []
        response = {}
        
        if not isinstance(location_names, list):
            location_names = [location_names]

        if location_names:
            for l in location_names:
                options = {
                    'startkey': [l],
                    'endkey': [f"{l}\n",{}],
                    'group_level':2
                }
                res = self.catalogo_view(catalog_id, form_id, options)
                if res and isinstance(res, list):
                    res_list.extend(res)

            response.update({
                "areas_by_location": list(set(res_list))
            })

        return response

    def do_access(self, qr_code, location, area, data):
        '''
        Valida pase de entrada y crea registro de entrada al pase
        '''
        access_pass = self.get_detail_access_pass(qr_code)
        if not qr_code and not location and not area:
            return False
        total_entradas = self.get_count_ingresos(qr_code)
        
        diasDisponibles = access_pass.get("limitado_a_dias", [])
        dias_semana = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
        tz = pytz.timezone("America/Mexico_City")
        hoy = datetime.now(tz)
        dia_semana = hoy.weekday()
        nombre_dia = dias_semana[dia_semana]

        if access_pass.get('estatus',"") == 'vencido':
            self.LKFException({'msg':"El pase esta vencido, edita la información o genera uno nuevo.","title":'Revisa la Configuración'})
        elif access_pass.get('estatus', '') == 'proceso':
            self.LKFException({'msg':"El pase no se ha sido completado aun, informa al usuario que debe completarlo primero.","title":'Requisitos faltantes'})

        if diasDisponibles:
            if nombre_dia not in diasDisponibles:
                dias_capitalizados = [dia.capitalize() for dia in diasDisponibles]

                if len(dias_capitalizados) > 1:
                    dias_formateados = ', '.join(dias_capitalizados[:-1]) + ' y ' + dias_capitalizados[-1]
                else:
                    dias_formateados = dias_capitalizados[0]

                self.LKFException({
                        'msg': f"Este pase no te permite ingresar hoy {nombre_dia.capitalize()}. Solo tiene acceso los siguientes dias: {dias_formateados}",
                        "title":'Aviso'
                    })
        
        limite_acceso = access_pass.get('limite_de_acceso')
        if len(total_entradas) > 0 and limite_acceso and int(limite_acceso) > 0:
            if total_entradas['total_records']>= int(limite_acceso) :
                self.LKFException({'msg':"Se ha completado el limite de entradas disponibles para este pase, edita el pase o crea uno nuevo.","title":'Revisa la Configuración'})
        
        timezone = pytz.timezone('America/Mexico_City')
        fecha_actual = datetime.now(timezone).replace(microsecond=0)
        fecha_caducidad = access_pass.get('fecha_de_caducidad')
        fecha_obj_caducidad = datetime.strptime(fecha_caducidad, "%Y-%m-%d %H:%M:%S")
        fecha_caducidad = timezone.localize(fecha_obj_caducidad)

        # Se agrega 1 hora como margen de tolerancia
        fecha_caducidad_con_margen = fecha_caducidad + timedelta(hours=1)

        if fecha_caducidad_con_margen < fecha_actual:
            self.LKFException({'msg':"El pase esta vencido, ya paso su fecha de vigencia.","title":'Advertencia'})
        
        fecha_visita = access_pass.get('fecha_de_expedicion')
        if fecha_visita:
            fecha_obj_visita = datetime.strptime(fecha_visita, "%Y-%m-%d %H:%M:%S")
            fecha_visita_tz = timezone.localize(fecha_obj_visita)
            
            if fecha_actual < fecha_visita_tz - timedelta(minutes=60):
                self.LKFException({'msg': f"Aún no es hora de entrada. Tu acceso comienza a las {fecha_visita}", "title": 'Aviso'})
        
        if location not in access_pass.get("ubicacion",[]):
            msg = f"La ubicación {location}, no se encuentra en el pase. Pase valido para las siguientes ubicaciones: {access_pass.get('ubicacion',[])}."
            self.LKFException({'msg':msg,"title":'Revisa la Configuración'})
        
        if self.validate_access_pass_location(qr_code, location):
            self.LKFException("En usuario ya se encuentra dentro de una ubicacion")
        val_certificados = self.validate_certificados(qr_code, location)

        
        pass_dates = self.validate_pass_dates(access_pass)
        comentario_pase =  data.get('comentario_pase',[])
        if comentario_pase:
            values = {self.pase_entrada_fields['grupo_instrucciones_pase']:{
                -1:{
                self.pase_entrada_fields['comentario_pase']:comentario_pase,
                self.mf['tipo_de_comentario']:'caseta'
                }
            }
            }
            # self.update_pase_entrada(values, record_id=[str(access_pass['_id']),])
        res = self._do_access(access_pass, location, area, data)
        return res

    def get_config_accesos(self):
        response = []
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.CONF_ACCESOS,
            f"answers.{self.EMPLOYEE_OBJ_ID}.{self.employee_fields['user_id_id']}":self.user['user_id'],
        }
        query = [
            {'$match': match_query },
            {'$project': {
                "usuario":f"$answers.{self.conf_accesos_fields['usuario_cat']}",
                "grupos":f"$answers.{self.conf_accesos_fields['grupos']}",
                "menus": f"$answers.{self.conf_accesos_fields['menus']}",
            }},
            {'$limit':1},
            {'$lookup': {
                'from': 'form_answer',
                'pipeline': [
                    {'$match': {
                        'deleted_at': {'$exists': False},
                        'form_id': self.CONF_MODULO_SEGURIDAD,
                    }},
                    {'$project': {
                        "_id": 0,
                        "excluir": f"$answers.{self.f['personalizacion_pases']}",
                        "incluir": f"$answers.{self.f['grupo_incluir']}",
                        "alertas": f"$answers.{self.f['grupo_alertas']}",
                    }}
                ],
                'as': 'personalizaciones'
            }},
            {'$unwind': '$personalizaciones'},
            {'$project': {
                "usuario":1,
                "grupos":1,
                "menus":1,
                "exclude_inputs": "$personalizaciones.excluir",
                "include_inputs": "$personalizaciones.incluir",
                "alertas": "$personalizaciones.alertas",
            }}
        ]
        data = self.format_cr_result(self.cr.aggregate(query),  get_one=True)
        format_data = {}

        if data:
            exclude_inputs = data.get('exclude_inputs', [])
            format_exclude_inputs = self.unlist([i for i in exclude_inputs])

            include_inputs = data.get('include_inputs', [])
            format_include_inputs = self.unlist([i for i in include_inputs])

            alertas = data.get('alertas', [])
            format_alerts = []
            for i in alertas:
                new_item = {}
                new_item[i.get('nombre_alerta')] = {
                    'accion': i.get('accion_alerta', '') if len(i.get('accion_alerta', [])) > 1 else self.unlist(i.get('accion_alerta', [])),
                }
                if 'llamar' in i.get('accion_alerta') or 'sms' in i.get('accion_alerta'):
                    new_item[i.get('nombre_alerta')]['number'] = i.get('llamar_num_alerta', 0000000000)
                if 'email' in i.get('accion_alerta'):
                    new_item[i.get('nombre_alerta')]['email'] = i.get('email_alerta', '')
                format_alerts.append(new_item)

            data.update({
                'exclude_inputs': format_exclude_inputs,
                'include_inputs': format_include_inputs,
                'alertas': format_alerts,
            })

        return data

    def create_access_pass(self, access_pass):
        """
        Crea pase de acceso 

        args:
        location (str): Ubicacion de donde se crea el paso
        access_pass (json): json con datos completos para generar el pase

        return:

        """
        #---Define Metadata
        metadata = self.lkf_api.get_metadata(form_id=self.PASE_ENTRADA)
        self.autorizado_por = ""
        metadata.update({
            "id":self.object_id(),
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": "Accesos",
                    "Process": "Creación de pase",
                    "Action": "create_access_pass",
                    "File": "accesos/app.py"
                }
                for item in mat.get('items', [])
            ],
            self.UBICACIONES_CAT_OBJ_ID: {
                self.mf['ubicacion']:            lugar.get('ubicacion', ''),
                self.f['address_name']:          [lugar.get('direccion', '')],
            },
        })
        answers = {}
        ics_invitation = False

        record_id = metadata['id']

        link_info = access_pass.get('link', "")
        docs=""
        
        if link_info:
            for index, d in enumerate(link_info["docs"]): 
                if(d == "agregarIdentificacion"):
                    docs+="iden"
                elif(d == "agregarFoto"):
                    docs+="foto"
                if index==0 :
                    docs+="-"
            link_pass= f"{link_info['link']}?id={record_id}&user={self.user.get('parent_id')}&docs={docs}"
            answers[self.pase_entrada_fields['link']] = link_pass.replace('web.clave10.com', '3b.clave10.com')
        lkf_qr = generar_qr.LKF_QR(self.settings)
       
        qr_generado = lkf_qr.procesa_qr( 
            record_id, 
            f"qr_{record_id}", 
            self.PASE_ENTRADA, 
            img_field_id=self.pase_entrada_fields['qr_pase'] )

        answers[self.pase_entrada_fields['qr_pase']] = qr_generado
        # 
        #---Define Answers
        perfil_pase = access_pass.get('perfil_pase', 'Visita General')


        user_data = self.lkf_api.get_user_by_id(self.user.get('user_id', self.user.get('id')))
        
        #TODO el timezone debiera de ser de quien crea el registro o de a quien se vista.
        #creo que se debe de poner una opcion advanzada para ajustar el tiemzone
        timezone = user_data.get('timezone','America/Monterrey')
        now_datetime =self.today_str(timezone, date_format='datetime')
        now_datetime_out = self.get_date_str(self.date_operation(now_datetime, '+', 8, 'hours'))

        # Setea personas vistadas
        answers[self.mf['grupo_visitados']] = []

        answers[self.mf['grupo_visitados']] = self.access_pass_vista_a(access_pass.get('visita_a',[]))
        ####

        if(access_pass.get('site', '') == 'accesos'):
            nombre_visita_a = access_pass.get('visita_a')
            # access_pass['ubicaciones'] = [location]

        answers[self.UBICACIONES_CAT_OBJ_ID] = {}

        fecha_limite_seleccionada = access_pass.get('fecha_desde_visita')
        ### Setting defaults
        access_pass['tipo_visita_pase'] = access_pass.get('tipo_visita_pase', 'fecha_fija')

        if not  access_pass.get('fecha_desde_visita') or access_pass['fecha_desde_visita'] == "":
            access_pass['fecha_desde_visita'] =  now_datetime
        
        if access_pass['tipo_visita_pase'] == 'fecha_fija' and fecha_limite_seleccionada:
            access_pass['fecha_desde_visita'] = now_datetime
            limite_fecha = fecha_limite_seleccionada.split(' ')[0]
            access_pass['fecha_desde_hasta'] = f"{limite_fecha} 23:59:59"
        elif not access_pass.get('fecha_desde_hasta') or access_pass['fecha_desde_hasta'] == "":
            ics_invitation = True
            access_pass['fecha_desde_hasta'] = now_datetime_out
        
        if not  access_pass.get('config_limitar_acceso') or access_pass['config_dia_de_acceso'] == "":
            access_pass['config_limitar_acceso'] =  1

        answers[self.pase_entrada_fields['tipo_visita_pase']] = access_pass.get('tipo_visita_pase','fecha_fija')
        answers[self.pase_entrada_fields['fecha_desde_visita']] = access_pass.get('fecha_desde_visita',now_datetime)
        answers[self.pase_entrada_fields['fecha_desde_hasta']] = access_pass.get('fecha_desde_hasta',now_datetime_out)
        answers[self.pase_entrada_fields['config_dia_de_acceso']] = access_pass.get('config_dia_de_acceso',"")
        answers[self.pase_entrada_fields['config_dias_acceso']] = access_pass.get('config_dias_acceso',"")
        answers[self.pase_entrada_fields['status_pase']] = access_pass.get('status_pase',"").lower()
        answers[self.pase_entrada_fields['empresa_pase']] = access_pass.get('empresa',"")
        # answers[self.pase_entrada_fields['ubicacion_cat']] = {self.mf['ubicacion']:access_pass['ubicacion'], self.mf['direccion']:access_pass.get('direccion',"")}
        answers[self.pase_entrada_fields['tema_cita']] = access_pass.get('tema_cita',access_pass.get('motivo',"") ) 
        answers[self.pase_entrada_fields['descripcion']] = access_pass.get('descripcion',"") 
        answers[self.pase_entrada_fields['config_limitar_acceso']] = access_pass.get('config_limitar_acceso',1) 
        answers[self.pase_entrada_fields['tipo_visita']] = 'alta_de_nuevo_visitante'
        answers[self.pase_entrada_fields['walkin_nombre']] = access_pass.get('nombre')
        answers[self.pase_entrada_fields['walkin_email']] = access_pass.get('email', '')
        answers[self.pase_entrada_fields['walkin_empresa']] = access_pass.get('empresa')
        answers[self.pase_entrada_fields['walkin_fotografia']] = access_pass.get('foto')
        answers[self.pase_entrada_fields['walkin_identificacion']] = access_pass.get('identificacion')
        answers[self.pase_entrada_fields['walkin_telefono']] = access_pass.get('telefono', '')
        answers[self.pase_entrada_fields['enviar_correo_pre_registro']] = access_pass.get("enviar_correo_pre_registro",[])
        answers[self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID] = {self.mf['nombre_area_salida']: access_pass.get('sala', '')}

        created_from = access_pass.get('created_from')
        if created_from == 'app':
            created_from = 'pase_de_entrada_app'
        elif created_from == 'web':
            created_from = 'pase_de_entrada_web'
        elif created_from == 'nueva_visita':
            created_from = 'nueva_visita'
        elif created_from == 'auto_registro':
            created_from = 'auto_registro'
        else:
            created_from = 'nueva_visita'

        if created_from:
            answers[self.pase_entrada_fields['creado_desde']] = created_from

        if access_pass.get('ubicaciones'):
            ubicaciones = access_pass.get('ubicaciones',[])
            if isinstance(ubicaciones, str):
                ubicaciones = [ubicaciones, ]
            address_list = self.get_locations_address(list_locations=ubicaciones)
            if ubicaciones:
                ubicaciones_list = []
                for ubi in ubicaciones:
                    ubicaciones_list.append(
                        {
                            self.pase_entrada_fields['ubicacion_cat']: { 
                                self.mf["ubicacion"]: ubi,
                                self.mf["direccion"]: [address_list.get(ubi, {}).get('address', '')],
                                self.f["address_geolocation"]: address_list.get(ubi, {}).get('geolocation', [])
                            }
                        }
                    )
                    if not access_pass.get('address'):
                        access_pass['address'] = address_list.get(ubi, {})
                answers.update({self.pase_entrada_fields['ubicaciones']:ubicaciones_list})
        
        if access_pass.get('comentarios'):
            comm = access_pass.get('comentarios',[])
            if comm:
                comm_list = []
                for c in comm:
                    comm_list.append(
                        {
                            self.pase_entrada_fields['comentario_pase']:c.get('comentario_pase'),
                            self.pase_entrada_fields['tipo_comentario'] :c.get('tipo_comentario').lower()
                        }
                    )
                answers.update({self.pase_entrada_fields['grupo_instrucciones_pase']:comm_list})

        if access_pass.get('todas_las_areas'):
            answers[self.pase_entrada_fields['todas_las_areas']]='sí'
            todas_areas = [] 
            for location in access_pass.get('ubicaciones', []):
                areas = self.get_areas_by_location(location)
                if isinstance(areas, list):
                    for area in areas:
                        todas_areas.append({
                            "nombre_area": area,
                            "commentario_area": "" 
                        })
            print(f"Todas las áreas hasta ahora: {todas_areas}")
            access_pass["areas"] = todas_areas

        if access_pass.get('areas'):
            areas = access_pass.get('areas',[])
            if areas:
                areas_list = []
                for c in areas:
                    # Validar si 'c' es un string (solo nombre) o un dict (objeto completo)
                    area_nombre = c.get('nombre_area') if isinstance(c, dict) else c
                    area_comentario = c.get('commentario_area', '') if isinstance(c, dict) else ""
                    
                    areas_list.append(
                        {
                            self.pase_entrada_fields['commentario_area']: area_comentario,
                            self.pase_entrada_fields['area_catalog_normal'] :{self.mf['nombre_area']: area_nombre}
                        }
                    )
                answers.update({self.pase_entrada_fields['grupo_areas_acceso']:areas_list})

        # Perfil de Pase
        answers[self.CONFIG_PERFILES_OBJ_ID] = {
            self.mf['nombre_perfil'] : perfil_pase
        }

        # lugar_recoleccion — solo tipos 2 y 3
        recoleccion = data.get('lugar_recoleccion', {})
        if recoleccion:
            transporte   = recoleccion.get('transporte', {})
            horario_rec  = recoleccion.get('horario', '') or ''
            hora_ini_rec, hora_fin_rec = '', ''
            if '-' in horario_rec:
                partes       = horario_rec.split('-')
                hora_ini_rec = partes[0].strip()
                hora_fin_rec = partes[1].strip()
            answers.update({
                f['lugar_de_recoleccion']:          recoleccion.get('lugar', ''),
                f['direccion_lugar_de_recoleccion']: recoleccion.get('direccion', ''),
                f['fecha_de_recoleccion']:          recoleccion.get('fecha', ''),
                f['hora_inicial_recoleccion']:      hora_ini_rec + ':00' if hora_ini_rec else '',
                f['hora_final_recoleccion']:        hora_fin_rec + ':00' if hora_fin_rec else '',
                f['anden_recoleccion']:             recoleccion.get('anden', ''),
                f['responsable']:                   transporte.get('responsable', ''),
                f['responsable_email']:             transporte.get('email', ''),
                f['responsable_telefono']:          transporte.get('telefono', ''),
                # f['metodo_de_embarque']:            recoleccion.get('metodo_embarque', '').lower(),
                # f['incoterm']:                      recoleccion.get('incoterm', '').lower(),
            })

        # Crea invitacion de calendario
        if created_from in ('pase_de_entrada_app', 'pase_de_entrada_web') or True:
            #TODO FLUJO DE AUTORIZACION DE PASES
            answers.update(self.access_pass_create_ics(access_pass, answers, ics_invitation))
            answers[self.pase_entrada_fields['catalago_autorizado_por']] = self.autorizar_pase_acceso(answers)


        answers[self.pase_entrada_fields['status_pase']] = self.access_pass_set_status(answers)
        metadata.update({'answers':answers})
        invitados = access_pass.get('invitados', [])
        if len(invitados) > 1:
            responses = self.create_multiple_access_pass(access_pass, invitados, metadata)
            return responses
        
        res = self.lkf_api.post_forms_answers(metadata)
        qr_code = res.get('json', {}).get('id')
        if perfil_pase == 'Interno' and created_from == 'nueva_visita' and qr_code:
            response = self.do_access(qr_code, access_pass.get('location', ''), access_pass.get('area', ''), access_pass)
            res['url_de_etiqueta'] = response.get('url_de_etiqueta', '') if response else ''
        qrcode_to_google_pass = ''
        id_forma = ''
        # if res.get("status_code") ==200 or res.get("status_code")==201:
        #     res = self.access_pass_google_pass(res, access_pass)
        return res
    
    def create_multiple_access_pass(self, access_pass, invitados, metadata):
        responses = []
        for invitado in invitados:
            record_id = self.object_id()
            metadata['id'] = record_id
            answers = metadata.get('answers', {})
            
            link_info = access_pass.get('link', "")
            docs=""
            
            if link_info:
                for index, d in enumerate(link_info["docs"]): 
                    if(d == "agregarIdentificacion"):
                        docs+="iden"
                    elif(d == "agregarFoto"):
                        docs+="foto"
                    if index==0 :
                        docs+="-"
                link_pass= f"{link_info['link']}?id={record_id}&user={self.user.get('parent_id')}&docs={docs}"
                answers[self.pase_entrada_fields['link']] = link_pass.replace('web.clave10.com', '3b.clave10.com')
            
            lkf_qr = generar_qr.LKF_QR(self.settings)
            qr_generado = lkf_qr.procesa_qr( 
                record_id, 
                f"qr_{record_id}", 
                self.PASE_ENTRADA, 
                img_field_id=self.pase_entrada_fields['qr_pase'] )

            answers[self.pase_entrada_fields['qr_pase']] = qr_generado
            answers[self.pase_entrada_fields['walkin_nombre']] = invitado.get('nombre')
            answers[self.pase_entrada_fields['walkin_email']] = invitado.get('email', '')
            metadata.update({'answers':answers})
            res = self.lkf_api.post_forms_answers(metadata)
            responses.append(res)
        return responses

        # Reserva visible en el kanban de bitácora (columna "Programados") desde
        # que se crea el pase — se liga por num_de_pase y se sustituye por el
        # registro real de arribo en create_visit_transportista.
        try:
            bf = self.bitacora_transportista_fields
            fecha_programada = (lugar.get('fecha_pase_transportista_desde') or '').strip()
            if fecha_programada and ' ' not in fecha_programada:
                fecha_programada = f'{fecha_programada} 00:00:00'
            # La bitácora solo acepta "entrega"/"recolección" (binario); el pase
            # maneja 4 valores (entrega_de_materia_prima, recoleccion_de_..., etc.)
            tipo_operacion_pase = data.get('tipo_de_operacion', '') or ''
            tipo_operacion_bitacora = 'recolección' if tipo_operacion_pase.startswith('recoleccion') else 'entrega'
            b_metadata = self.lkf_api.get_metadata(form_id=self.BITACORA_TRANSPORTISTAS)
            b_metadata.update({
                'properties': {
                    'device_properties': {
                        'System': 'Script',
                        'Module': 'Accesos',
                        'Process': 'Pase Transportista',
                        'Action': 'create_pass_transportista',
                        'File': 'modules/accesos/items/scripts/Accesos/accesos_utils.py',
                    }
                },
                'answers': {
                    bf['estatus']:               'programado',
                    bf['fecha_hora_ingreso']:    fecha_programada,
                    bf['num_de_pase']:           pass_id,
                    bf['tipo_de_operacion']:     tipo_operacion_bitacora,
                    bf['empresa_transportista']: data.get('empresa_transportista', ''),
                    bf['proveedor_cliente']:     mat.get('proveedor_cliente', ''),
                    bf['orden_de_compra']:       mat.get('orden_compra', ''),
                    bf['anden_asignado']:        lugar.get('anden', ''),
                },
            })
            res_stub = self.lkf_api.post_forms_answers(b_metadata)
            if res_stub.get('status_code') not in [200, 201, 202]:
                print(f'No se pudo crear el registro programado de bitácora para el pase {pass_id}: {res_stub}')
        except Exception as e:
            print(f'No se pudo crear el registro programado de bitácora para el pase {pass_id}: {e}')

        return res

    def create_visit_transportista(self, data):
        f = self.bitacora_transportista_fields
        print(simplejson.dumps(data, indent=3))
        metadata = self.lkf_api.get_metadata(form_id=self.BITACORA_TRANSPORTISTAS)
        metadata.update({
            'properties': {
                'device_properties': {
                    'System': 'Script',
                    'Module': 'Accesos',
                    'Process': 'Bitácora Transportista',
                    'Action': 'create_visit_transportista',
                    'File': 'modules/accesos/items/scripts/Accesos/accesos_utils.py',
                }
            }
        })

        vehiculo  = data.get('vehiculo', {}) or {}
        conductor = data.get('conductor', {}) or {}
        embarque  = data.get('embarque', {}) or {}
        firma     = conductor.get('firma') or {}

        tz_name = self.user.get('timezone', 'America/Mexico_City')
        tz = pytz.timezone(tz_name)
        fecha_ingreso = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

        answers = {
            f['estatus']:               'arribo',
            f['fecha_hora_ingreso']:    fecha_ingreso,
            f['num_de_pase']:           data.get('num_de_pase', ''),
            f['tipo_de_operacion']:     (data.get('tipo_operacion') or '').lower().replace(' ', '_'),
            f['empresa_transportista']: vehiculo.get('transportista', ''),
            f['procedencia']:           vehiculo.get('procedencia', ''),
            f['tipo_de_vehiculo']:      vehiculo.get('tipo_vehiculo', ''),
            f['placas_de_vehiculo']:                    vehiculo.get('placa', ''),
            f['placas_de_vehiculo_tarjeta_circulacion']: vehiculo.get('placa_tarjeta_circulacion', ''),
            f['num_eco_num_rotulo']:                    vehiculo.get('no_economico', ''),
            f['marca_vehiculo']:        vehiculo.get('marca', ''),
            f['year_vehiculo']:         vehiculo.get('modelo', ''),
            f['color_vehiculo']:        vehiculo.get('color', ''),
            f['conductor']:             conductor.get('nombre', ''),
            f['ayudante']:              conductor.get('acompanante', ''),
            f['num_licencia']:          conductor.get('no_licencia', ''),
            f['vigencia_licencia']:     conductor.get('vigencia_licencia', ''),
            f['rfc_conductor']:         conductor.get('rfc', ''),
            f['firma_conductor']:       firma,
            f['proveedor_cliente']:     embarque.get('proveedor_cliente', ''),
            f['orden_de_compra']:       embarque.get('no_orden_compra', ''),
        }

        # Ubicación + área del turno activo desde donde se registró la visita — se
        # usa después para resolver qué forma de inspección aplica en ese sitio.
        # Campo "Áreas de las Ubicaciones" agregado por Paco a esta forma (mismo
        # catálogo que usan paquetería/incidencias/casetas).
        ubicacion = data.get('ubicacion')
        area = data.get('area')
        if ubicacion or area:
            answers[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID] = {
                self.mf['ubicacion']: ubicacion or '',
                self.mf['nombre_area']: area or '',
            }

        remolques    = data.get('remolques', []) or []
        contenedores = data.get('contenedores', []) or []
        grupo = remolques + contenedores
        if grupo:
            answers[f['grupo_remolques']] = [
                {
                    f['tipo_remolque']:             item.get('tipo', ''),
                    # Los remolques solo traen no_caja; los contenedores traen
                    # además no_contenedor (su propio ID/ISO), que se prefiere
                    # cuando está presente — si no, ambos comparten esta columna.
                    f['num_caja_contenedor']:        item.get('no_contenedor') or item.get('no_caja', ''),
                    f['num_sello']:                  item.get('no_sello', ''),
                    f['placas_de_caja']:             item.get('placas', ''),
                    f['color_remolque_contenedor']:  item.get('color', ''),
                    f['no_referencia_remolque']:     item.get('ref_remolque', ''),
                    f['comentarios']:                item.get('comentarios', ''),
                }
                for item in grupo
            ]

        docs = data.get('documentos_adicionales', []) or []
        if docs:
            answers[f['grupo_fotos_y_documentos']] = [
                {
                    f['tipo_de_documento']: doc.get('tipo', ''),
                    f['documento']:         [{'file_name': doc.get('file_name', ''), 'file_url': doc['file_url']}] if doc.get('file_url') else [],
                }
                for doc in docs
            ]

        materiales = data.get('materiales', []) or []
        if materiales:
            answers[f['grupo_materiales']] = [
                {
                    f['producto_material']:        m.get('producto', ''),
                    f['lote_material']:            m.get('lote', ''),
                    f['cantidad_material']:        m.get('cant_esperada', ''),
                    f['cantidad_fisica_material']: m.get('cant_fisica', ''),
                    f['peso_material']:            m.get('peso', ''),
                    f['volumen_material']:         m.get('volumen', ''),
                    f['no_referencia_material']:   m.get('ref', ''),
                    f['lugar_material']:           'contenedor' if str(m.get('ref', '')).startswith('contenedor') else 'remolque' if str(m.get('ref', '')).startswith('remolque') else 'vehiculo',
                }
                for m in materiales
            ]

        num_de_pase = data.get('num_de_pase')
        programado = None
        if num_de_pase:
            programado = self.cr.find_one({
                'form_id': self.BITACORA_TRANSPORTISTAS,
                'deleted_at': {'$exists': False},
                f'answers.{f["num_de_pase"]}': num_de_pase,
                f'answers.{f["estatus"]}': 'programado',
            })

        if programado:
            # El pase ya reservó su lugar en el kanban (columna "Programados") al
            # crearse — actualizamos ese mismo registro a "arribo" en vez de crear
            # uno duplicado. patch_forms_answers reescribe el documento de answers
            # completo, así que hay que mezclar con lo que ya existía (ej. andén)
            # o se pierde cualquier campo que este payload no vuelva a mandar.
            merged_answers = {**programado.get('answers', {}), **answers}
            metadata.update({'answers': merged_answers, '_id': programado['_id']})
            res = self.net.patch_forms_answers(metadata)
            if res.get('status_code') not in [200, 201, 202]:
                self.LKFException({'title': 'Error al actualizar visita de transportista', 'msg': res})
            res['id'] = str(programado['_id'])
            res['folio'] = programado.get('folio')
            res['created_at'] = self.get_date_str(programado.get('created_at'))
        else:
            metadata.update({'answers': answers})
            res = self.lkf_api.post_forms_answers(metadata)
            if res.get('status_code') not in [200, 201, 202]:
                self.LKFException({'title': 'Error al crear visita de transportista', 'msg': res})

    def get_detail_access_pass(self, qr_code, get_answers=False):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PASE_ENTRADA,
            "_id":ObjectId(qr_code),
        }
        query = [
            {'$match': match_query },
            {'$project': 
                {'_id':1,
                'folio': f"$folio",
                'answers':'$answers',
                'ubicacion': f"$answers.{self.mf['grupo_ubicaciones_pase']}.{self.UBICACIONES_CAT_OBJ_ID}",
                'nombre': {"$ifNull":[
                    f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['nombre_visita']}",
                    f"$answers.{self.mf['nombre_pase']}"]},
                'estatus': f"$answers.{self.pase_entrada_fields['status_pase']}",
                'empresa': {"$ifNull":[
                     f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}",
                     f"$answers.{self.mf['empresa_pase']}"]},
                'email':  {"$ifNull":[
                    f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['email_vista']}",
                    f"$answers.{self.mf['email_pase']}"]},
                'telefono': {"$ifNull":[
                    f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['telefono']}",
                    f"$answers.{self.mf['telefono_pase']}"]},
                'curp': f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['curp']}",
                'fecha_de_expedicion': f"$answers.{self.mf['fecha_desde_visita']}",
                'fecha_de_caducidad':{'$ifNull':[
                    f"$answers.{self.mf['fecha_desde_hasta']}",
                    f"$answers.{self.mf['fecha_desde_visita']}",
                    ]
                    },
                'foto': {'$ifNull':[
                    f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['foto']}",
                    f"$answers.{self.pase_entrada_fields['walkin_fotografia']}"]},
                'limite_de_acceso': f"$answers.{self.mf['config_limitar_acceso']}",
                'config_dia_de_acceso': f"$answers.{self.mf['config_dia_de_acceso']}",
                'identificacion': {'$ifNull':[
                    f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['identificacion']}",
                    f"$answers.{self.pase_entrada_fields['walkin_identificacion']}"]},
                'limitado_a_dias':f"$answers.{self.mf['config_dias_acceso']}",
                'motivo_visita':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}.{self.mf['motivo']}",
                'perfil_pase':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}",
                'tipo_de_pase':f"$answers.{self.pase_entrada_fields['perfil_pase']}",
                'tipo_de_comentario': f"$answers.{self.mf['tipo_de_comentario']}",
                'visita_a_nombre':
                     f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_empleado']}",
                'visita_a_puesto': 
                    f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['puesto_empleado']}",
                'visita_a_departamento':
                    f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['departamento_empleado']}",
                'visita_a_user_id':
                    f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['user_id_empleado']}",
                'visita_a_email':
                    f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['email_visita_a']}",
                'visita_a_telefono':
                    f"$answers.{self.mf['grupo_visitados']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['telefono_visita_a']}",
                'grupo_areas_acceso': f"$answers.{self.mf['grupo_areas_acceso']}",
                # 'grupo_commentario_area': f"$answers.{self.mf['grupo_commentario_area']}",
                'grupo_equipos': f"$answers.{self.mf['grupo_equipos']}",
                'grupo_vehiculos': f"$answers.{self.mf['grupo_vehiculos']}",
                'grupo_instrucciones_pase': f"$answers.{self.mf['grupo_instrucciones_pase']}",
                'comentario': f"$answers.{self.mf['grupo_instrucciones_pase']}",
                'codigo_qr': f"$answers.{self.mf['codigo_qr']}",
                'qr_pase': f"$answers.{self.mf['qr_pase']}",
                'tema_cita': f"$answers.{self.pase_entrada_fields['tema_cita']}",
                'descripcion': f"$answers.{self.pase_entrada_fields['descripcion']}",
                'link': f"$answers.{self.pase_entrada_fields['link']}",
                'google_wallet_pass_url': f"$answers.{self.pase_entrada_fields['google_wallet_pass_url']}",
                'apple_wallet_pass': f"$answers.{self.pase_entrada_fields['apple_wallet_pass']}",
                'pdf_to_img': f"$answers.{self.pase_entrada_fields['pdf_to_img']}",
                'acepto_aviso_privacidad': f"$answers.{self.pase_entrada_fields['acepto_aviso_privacidad']}",
                'acepto_aviso_datos_personales': f"$answers.{self.pase_entrada_fields['acepto_aviso_datos_personales']}",
                'conservar_datos_por': f"$answers.{self.pase_entrada_fields['conservar_datos_por']}",
                'ubicaciones': f"$answers.{self.pase_entrada_fields['ubicaciones']}",
                'sala': f"$answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.mf['nombre_area_salida']}"
                },
            },
            {'$sort':{'created_at':-1}},
        ]
        res = self.cr.aggregate(query)
        x = {}
        for x in res:
            if get_answers:
                x['answers'] = x.get('answers',{})
            visita_a =[]
            x['_id'] = str(x.pop('_id'))
            v = x.pop('visita_a_nombre') if x.get('visita_a_nombre') else []
            d = x.get('visita_a_departamento',[])
            p = x.get('visita_a_puesto',[])
            e =  x.get('visita_a_user_id',[])
            u =  x.get('visita_a_email',[])
            f =  x.get('visita_a_telefono',[])
            x['empresa'] = self.unlist(x.get('empresa',''))
            x['email'] =self.unlist(x.get('email',''))
            x['telefono'] = self.unlist(x.get('telefono',''))
            x['curp'] = self.unlist(x.get('curp',''))
            x['motivo_visita'] = self.unlist(x.get('motivo_visita',''))
            for idx, nombre in enumerate(v):
                emp = {'nombre':nombre}
                if d:
                    emp.update({'departamento':d[idx].pop(0) if d[idx] else ""})
                if p:
                    emp.update({'puesto':p[idx].pop(0) if p[idx] else ""})
                if e:
                    emp.update({'user_id':e[idx].pop(0) if e[idx] else ""})
                if u:
                    emp.update({'email': u[idx].pop(0) if u[idx] else ""})
                if f:
                    emp.update({'telefono': f[idx].pop(0) if f[idx] else ""})
                visita_a.append(emp)
            x['visita_a'] = visita_a
            perfil_pase = x.pop('perfil_pase') if x.get('perfil_pase') else []
            perfil_pase = self._labels(perfil_pase, self.mf)
            if x.get('fecha_de_caducidad') == "":
                x['fecha_de_caducidad'] = x.get('fecha_de_expedicion')
            if perfil_pase:
                x['tipo_de_pase'] = perfil_pase.pop('nombre_perfil')
                empresa = x.get('empresa')
                x['certificaciones'] = self.format_perfil_pase(perfil_pase, x['curp'], empresa)
            x['grupo_areas_acceso'] = self._labels_list(x.pop('grupo_areas_acceso',[]), self.mf)
            x['grupo_instrucciones_pase'] = self._labels_list(x.pop('grupo_instrucciones_pase',[]), self.mf)
            x['grupo_equipos'] = self._labels_list(x.pop('grupo_equipos',[]), self.mf)
            x['grupo_vehiculos'] = self._labels_list(x.pop('grupo_vehiculos',[]), self.mf)
            ubicaciones_full_info = x.get('ubicaciones', [])
            x['ubicacion'] = [x.get(self.UBICACIONES_CAT_OBJ_ID, {}).get(self.Location.f['location']) for x in ubicaciones_full_info]
            ubicaciones = x.get('ubicaciones', [])
            ubicaciones_format = []
            for ubicacion in ubicaciones:
                ubicaciones_format.append(ubicacion.get(self.UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['ubicacion'], ''))
            x['ubicaciones'] = ubicaciones_format
            x['ubicaciones_geolocation'] = {
                x.get(self.UBICACIONES_CAT_OBJ_ID, {}).get(self.Location.f['location']): self.unlist(x.get(self.UBICACIONES_CAT_OBJ_ID, {}).get(self.f['address_geolocation']))
                for x in ubicaciones_full_info
            }
            grupo_visita = x.get('answers', {}).get(self.mf['grupo_visitados'], [])
            if grupo_visita:
                visita_list = []
                for idx, visita in enumerate(grupo_visita):
                    item = {
                        'id': self.unlist(visita.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID, {}).get(self.mf['id_usuario'], '')) or idx,
                        'username': self.unlist(visita.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID, {}).get(self.mf['username'], '')) or '',
                        'name': visita.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID, {}).get(self.mf['nombre_empleado'], '') or '',
                        'telefono': self.unlist(visita.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID, {}).get(self.mf['telefono_visita_a'], '')) or '',
                        'email': self.unlist(visita.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID, {}).get(self.mf['email_visita_a'], '')) or '',
                    }
                    visita_list.append(item)
                x['visita_a_details'] = visita_list
        if not x:
            self.LKFException({'title':'Advertencia', 'msg':'Este pase fue eliminado o no pertenece a esta organizacion.'})
        return x

        return res

    def create_custom_qr(self, url_for_qr, name_qr, form_id, img_field_id):
        lkf_qr = generar_qr.LKF_QR(self.settings)
        qr_generado = lkf_qr.procesa_qr(url_for_qr, name_qr, form_id, img_field_id)
        return qr_generado

    def ocr_acceso_transportista(self, image_source,
                                  extra_instructions: str = None,
                                  model: str = 'google/gemini-2.5-flash-lite') -> dict:
        """
        Analiza uno o varios archivos de un acceso de transportista.
        Acepta mezcla de imágenes y documentos (PDFs, JPGs, PNGs).

        Tipos de archivos soportados:
        - Foto de placas / vehículo
        - Foto del conductor
        - Licencia de conducir
        - Tarjeta de circulación (tractor o remolque)
        - Bill of Lading (BL) / conocimiento de embarque
        - Pedimento de importación temporal
        - Orden de compra / factura / manifiesto de carga
        - Documento de autorización de salida de puerto
        - Foto o documento del contenedor

        Args:
            image_source: URL, ruta local, o lista. Acepta imágenes y PDFs remotos.
            model:        Modelo OpenRouter ('google/gemini-2.5-flash' recomendado para docs).

        Returns:
            dict con status_code, data, msg.
        """
        if not self.ai:
            return {'status_code': 400, 'msg': 'OpenRouter no configurado'}

        system = (
            "You are a certified security supervisor and CTPAT compliance specialist at an industrial facility. "
            "You process transport access events by analyzing any combination of: vehicle photos, license plates, "
            "driver photos, driver licenses, vehicle registration cards (tarjeta de circulación) for both tractors "
            "and trailers, Bills of Lading, temporary import permits (pedimentos), port release documents, "
            "purchase orders, cargo manifests, and container photos. "
            "All inputs refer to ONE transport access event, which may include MULTIPLE remolques and MULTIPLE "
            "contenedores, each with its own tarjeta de circulación or documentation, and each potentially carrying "
            "DIFFERENT cargo. "
            "You ONLY extract information that is clearly visible or printed in the provided files. "
            "You NEVER invent, estimate, or hallucinate data, and you NEVER let data from one vehicle, remolque, "
            "contenedor, or cargo line overwrite or merge with data belonging to a different one. "
            "If a field is not present in any document, return null — never guess. "
            "Always respond with a single valid JSON object and nothing else — "
            "no markdown, no backticks, no explanation, no preamble."
        )

        prompt = (
            "Analyze all provided files (images and/or documents) as a single transport access event. "
            "The files are provided in order: the first is imagen_1, the second is imagen_2, and so on — "
            "this numbering is PER FILE, not per page. A single file can be a multi-page PDF containing "
            "several distinct photos or document pages (e.g. a PDF with 9 different evidence photos, or a "
            "PDF with 3 pages: cover letter, invoice, packing list). When that happens, EVERY page/photo "
            "found inside that one file still gets the SAME `fuente` value (that file's imagen_N) — never "
            "invent a new imagen_N for a page just because it is the file's 2nd, 3rd, etc. internal page. "
            "Use the `pagina` field on each `documentos_detectados` entry to indicate which page/photo "
            "number WITHIN that file it corresponds to (1 for the first page/photo of that file, 2 for the "
            "second, etc.), so a page can still be told apart from others in the same file. "
            "WORKED EXAMPLE: suppose the input list has 2 files — file 1 is a 3-page PDF (a driver photo, a "
            "container photo, and a Carta de Ruta) and file 2 is a 1-page Bill of Lading. The CORRECT output "
            "has THREE `documentos_detectados` entries with `fuente`:\"imagen_1\" (using `pagina` 1, 2, and 3 "
            "respectively, one per internal page, each with its own `tipo`), and ONE entry with "
            "`fuente`:\"imagen_2\", `pagina`:1. It would be WRONG to output fuente values imagen_1, imagen_2, "
            "imagen_3, imagen_4 for that example — there are only 2 actual files, so fuente can never exceed "
            "the number of files provided, no matter how many total pages/photos are found across all of them. "
            "HARD RULE: count the DISTINCT `fuente` values you use across the entire `documentos_detectados` "
            "array — that count must be EXACTLY equal to the number of files given to you, never more. Before "
            "moving on to describe the next page/photo you find, first check whether it belongs to a file whose "
            "imagen_N you already used for a previous entry — if so, reuse that same `fuente` and only increase "
            "`pagina`; only introduce a new, higher `fuente` value once you have moved on to inspecting the next "
            "actual file in the input list. "
            "Files may include vehicle photos, driver photos, driver licenses, vehicle registration cards, "
            "Bills of Lading, pedimentos, port documents, purchase orders, container photos, or a Carta de "
            "Ruta (Dominican customs internal-transit authorization). "
            "Extract every field you can find. If a field is absent from all provided files, use null. "
            "\n\n"
            "IMPORTANT ON CARGO-TO-UNIT LINKING: when a document (Bill of Lading, packing list, manifest) breaks "
            "down cargo per container or trailer — e.g. a container number is followed by its own weight, volume, "
            "package count, and product description — that cargo line belongs EXCLUSIVELY inside that container's "
            "own `materiales` array (nested inside its entry in `contenedores[]`), matched by container number "
            "first, or by the weight/volume/package figures printed right next to that container's row if the "
            "number match is unclear. Do the same for remolques when cargo is described per-trailer. "
            "A cargo line may also carry its OWN purchase-order reference distinct from the shipment-level PO — "
            "capture it in that material's own `no_orden_compra` field when present, without overwriting "
            "`embarque.no_orden_compra`. "
            "If a summary line states a type/quantity that applies to multiple units (e.g. '2 x 40HC CONTAINER' "
            "before individual container rows), apply that type to EACH of those containers' `tipo` field unless a "
            "specific row overrides it — do not leave it null just because it was only stated once at the top. "
            "Only use the top-level `materiales` array as a FALLBACK, for cargo that cannot be attributed to any "
            "specific contenedor or remolque (e.g. loose cargo directly on a rigid vehicle with no container/trailer "
            "breakdown, or a generic document that does not specify per-unit contents). Never duplicate the same "
            "cargo line in both a specific unit's materiales and the top-level materiales. "
            "\n\n"
            "IMPORTANT ON CLOSED-LIST FIELDS (tipo_vehiculo, remolques[].tipo, contenedores[].tipo): these values "
            "feed a form with FIXED dropdown options — there is NO 'otro' catch-all option available downstream. "
            "If the document clearly states a type that matches one of the listed options, use that exact listed "
            "value. If the document states a type that does NOT match any listed option (e.g. 'furgón' when it's "
            "not in the list), do NOT force it into the closest option and do NOT return null — instead return the "
            "type EXACTLY as written/stated in the document, as free text, so a human can review and map it "
            "manually. Only return null if no type information is present at all. "
            "\n\n"
            "IMPORTANT ON VEHICLE ARTICULATION (camion vs. trailer): "
            "A camion (rigid/straight truck) has the cab and cargo box built on ONE single chassis — they cannot "
            "be separated. A trailer (tractocamion articulado) is TWO separable pieces joined by a fifth wheel "
            "(quinta rueda): the tracto (cab + engine, no cargo box of its own) pulling a semirremolque (the box, "
            "which can be unhitched and stands alone on its own landing gear). "
            "DECISION RULE: if the vehicle has a separable remolque (i.e. you will be listing one or more entries "
            "in remolques[]), set vehiculo.tipo_vehiculo to \"trailer\" — do NOT also describe the box type "
            "(caja_seca, plataforma, etc.) at the vehiculo level, that belongs exclusively in remolques[].tipo. "
            "If there is NO separable remolque (rigid single-chassis vehicle), set vehiculo.tipo_vehiculo to "
            "whichever rigid type applies (torton, camion, van, pick_up, pipa, volteo), following the closed-list "
            "rule above. Leave remolques[] empty in that case. "
            "\n\n"
            "IMPORTANT: remolques are trailers/flatbeds pulled by the truck. "
            "contenedores are ISO shipping containers (they have an alphanumeric container number like ECMU7740351, "
            "distinct from any internal box/asset number the facility may also assign). "
            "A remolque may carry a contenedor — if so, list the trailer in remolques and the container in "
            "contenedores. There may be MORE THAN ONE remolque and MORE THAN ONE contenedor in the same event — "
            "keep each one as a separate entry in its array, never merge two different units into one entry. "
            "\n\n"
            "IMPORTANT ON PLATES: a plate value must come from a field EXPLICITLY labeled as a plate (\"PLACAS\", "
            "\"No. de Placas\", a physical plate photo, etc.). Do NOT confuse a plate with a nearby barcode, folio, "
            "or document-verification code — these are different alphanumeric strings that often sit next to a "
            "barcode graphic for document authentication purposes, not the physical plate, even if their format "
            "superficially resembles a plate. When in doubt, prefer the value under an explicit plate label over "
            "any other nearby code. "
            "A single physical vehicle or remolque can have its plate appear in more than one source — a photo of "
            "the plate itself, an incidental mention in another document, AND its own tarjeta de circulación / "
            "pedimento. These are DIFFERENT sources describing the SAME plate, and must be kept in SEPARATE fields, "
            "never overwriting one another: use `placa`/`placas` for what you read from a photo or an incidental/ "
            "general mention, and `placa_tarjeta_circulacion`/`placas_tarjeta_circulacion` EXCLUSIVELY for the plate "
            "printed under an explicit plate label on that specific entity's own registration/import document. "
            "If there are multiple remolques, each with its own tarjeta/pedimento document, match each document to "
            "the correct remolque using its no_caja/unit number/no_economico or contextual order. If you cannot "
            "confidently match a document to a specific remolque, still record its plate value in the most likely "
            "remolque's `placas_tarjeta_circulacion` and note the ambiguity in that remolque's `comentarios` — never "
            "drop the value just because the match is uncertain. "
            "\n\n"
            "IMPORTANT ON SEAL NUMBERS (no_sello_documento / no_sello_fisico): a container or trailer can show MORE "
            "THAN ONE physical seal in photos (e.g. a carrier lock seal, a security tag, AND the official customs/"
            "shipper seal), and their numbers will differ from what is printed in text documents — this is normal "
            "and does not mean any of them is wrong. Do NOT try to decide which one is 'the real seal' yourself: "
            "just report each source into its own field, exactly as it appears there. `no_sello_documento` is "
            "whatever seal number is printed in text on a BL/factura/packing list/carta de ruta/pedimento/"
            "manifiesto for that unit — leave it null if no document prints one. `no_sello_fisico` is whatever seal "
            "number you read directly off a photograph of a physical seal/tag on that unit — leave it null if no "
            "such photo is provided. Fill BOTH independently whenever both kinds of source exist, even if their "
            "values disagree; the decision of which one to trust is made downstream in code, not by you. "
            "\n\n"
            "IMPORTANT ON DRIVER IDENTIFICATION (conductor.nombre / conductor.no_licencia): `no_licencia` must come "
            "EXCLUSIVELY from an official government-issued driving license/permit document. Never use a number "
            "from a company badge, employee ID card, lanyard, or gafete as `no_licencia` — those are internal/"
            "corporate identifiers, not driving licenses; if that is the only ID-like number visible, leave "
            "`no_licencia` null and, if useful, mention the badge number in `observaciones` instead. "
            "`conductor.nombre`, in contrast, is NOT limited to license/permit documents — it can also appear on an "
            "official transit/customs authorization document such as a Carta de Ruta, which typically lists the "
            "assigned driver by name alongside the container/seal/carrier data. These forms are often photographed "
            "at an angle where a column's header label is cropped or unreadable, but the name value itself is still "
            "legible — in that case, use the document's standard layout and the surrounding fields (container "
            "number, seal, compañía transportista, sindicato de camioneros) to infer that a legible person's name "
            "sitting in that position is the driver, and fill `conductor.nombre` accordingly rather than returning "
            "null just because the column label itself was cut off. "
            "\n\n"
            "IMPORTANT ON DATES: interpret dates according to the document's own convention before converting to "
            "YYYY-MM-DD — English-language documents (BL, invoices) typically use MM/DD/YYYY, Mexican documents "
            "(pedimentos, tarjetas, licencias) typically use DD/MM/YYYY. If the convention is genuinely ambiguous "
            "for a given date, keep the original string as printed instead of guessing day vs. month. "
            "If different documents in the same event show dates that are inconsistent with each other in a way "
            "that cannot be explained by normal shipment lead times (e.g. a loading date years apart from the "
            "invoice or BL issue date), do not silently pick one and treat it as resolved — report the value you "
            "found and flag the inconsistency in `observaciones`, and let it lower `confianza` accordingly. "
            "\n\n"
            "Return ONLY a JSON object with this exact structure:\n"
            "{\n"

            # ── VEHÍCULO ──────────────────────────────────────────────────
            '  "vehiculo": {\n'
            '    "transportista": "string — carrier company name (e.g. TRAMO TRANSPORTES MONTERREY SA DE CV), or null",\n'
            '    "procedencia": "string — city or state of origin of the vehicle/shipment if visible on any document, or null",\n'
            '    "tipo_vehiculo": "string — one of: torton, camion, van, pick_up, pipa, volteo, trailer, or null. See DECISION RULE and CLOSED-LIST rule above. Never use caja_seca/caja_refrigerada/plataforma/etc here — those belong to remolques[].tipo.",\n'
            '    "marca": "string — truck/tractor brand (Kenworth, Freightliner, International, Volvo, etc.), or null",\n'
            '    "modelo": "string — truck model year if visible (e.g. 2019), or null",\n'
            '    "color": "string — main cab color. PRIORITY: extract visually from vehicle/plate photos if provided. Fall back to text on registration card only if no vehicle photo is present. Use Spanish color names (Blanco, Negro, Rojo, Azul, Gris, Verde, Amarillo, Naranja, Cafe, Plateado, etc.), or null",\n'
            '    "placa": "string — tractor/cab license plate as read from a vehicle/plate photo or an incidental mention in another document, exactly as printed, or null",\n'
            '    "placa_tarjeta_circulacion": "string — tractor/cab license plate extracted EXCLUSIVELY from an explicit plate label on a tarjeta_circulacion_vehiculo document, exactly as printed, or null",\n'
            '    "no_economico": "string — carrier economic number / rótulo on the vehicle, or null"\n'
            '  },\n'

            # ── CONDUCTOR ─────────────────────────────────────────────────
            '  "conductor": {\n'
            '    "nombre": "string — driver full name from license or permit document, or null",\n'
            '    "no_licencia": "string — driver license number exactly as printed, or null",\n'
            '    "vigencia_licencia": "string — license expiration date in YYYY-MM-DD format, or null",\n'
            '    "rfc": "string — RFC if shown on any document, or null",\n'
            '    "acompanante": "string — co-driver or helper full name if visible on any document, or null"\n'
            '  },\n'

            # ── REMOLQUES ─────────────────────────────────────────────────
            '  "remolques": [\n'
            '    {\n'
            '      "tipo": "string — trailer box type: caja_seca, plataforma, caja_refrigerada, ganadero, basculante, portavehiculos, caravana, or null. This is a CLOSED LIST (no otro option downstream) — see CLOSED-LIST rule above.",\n'
            '      "no_caja": "string — trailer box/unit number (número económico de caja) from registration card or visible on unit, or null",\n'
            '      "no_sello_documento": "string — seal number for this trailer EXACTLY as printed in any text document (BL, factura, packing list, carta de ruta, pedimento, manifiesto), or null if no document states one. See IMPORTANT ON SEAL NUMBERS below — do NOT put a photo-only reading here.",\n'
            '      "no_sello_fisico": "string — seal number read directly off a photograph of the physical seal/tag on this trailer, or null if no such photo is provided or none is legible.",\n'
            '      "placas": "string — trailer license plate as read from a photo or an incidental mention, exactly as printed, or null",\n'
            '      "placas_tarjeta_circulacion": "string — trailer license plate extracted EXCLUSIVELY from an explicit plate label on this trailer\'s own tarjeta/pedimento document, exactly as printed, or null",\n'
            '      "color": "string — trailer color in Spanish (Blanco, Gris, Rojo, etc.), or null",\n'
            '      "comentarios": "string — any relevant note about this trailer (damage, anomaly, ambiguous document match, tipo that did not match the closed list, etc.), or null",\n'
            '      "materiales": [\n'
            '        {\n'
            '          "producto": "string — cargo/product description, or null",\n'
            '          "lote": "string — lot or batch number if stated, or null",\n'
            '          "cant_esperada": "string — expected quantity with unit if stated, or null",\n'
            '          "peso": "string — gross weight with unit, or null",\n'
            '          "volumen": "string — volume with unit if stated, or null",\n'
            '          "no_orden_compra": "string — PO number specific to THIS cargo line, if different from embarque.no_orden_compra, or null"\n'
            '        }\n'
            '      ]\n'
            '    }\n'
            '  ],\n'

            # ── CONTENEDORES ──────────────────────────────────────────────
            '  "contenedores": [\n'
            '    {\n'
            '      "tipo": "string — ISO container type: 20GP, 40GP, 40HC, 20RF, 40RF, 40HR, 20OT, 40OT, 20FR, 40FR, iso_tank, 20VH, open_side, or null. This is a CLOSED LIST (no otro option downstream) — see CLOSED-LIST rule above. Remember to apply a type stated once in a summary line to every matching container (see CARGO-TO-UNIT LINKING above).",\n'
            '      "no_contenedor": "string — official ISO container number exactly as printed (e.g. ECMU7740351, EGHU9785216), or null",\n'
            '      "no_caja": "string — internal facility box/asset number for this container, if separately assigned and distinct from no_contenedor, or null",\n'
            '      "no_sello_documento": "string — seal number for this container EXACTLY as printed in any text document (BL, factura, packing list, carta de ruta, pedimento, manifiesto), or null if no document states one. See IMPORTANT ON SEAL NUMBERS below — do NOT put a photo-only reading here.",\n'
            '      "no_sello_fisico": "string — seal number read directly off a photograph of the physical seal/tag on this container, or null if no such photo is provided or none is legible.",\n'
            '      "placas": "string — chassis plate if visible, or null",\n'
            '      "color": "string — container color in Spanish, or null",\n'
            '      "comentarios": "string — any relevant note about this container (damage, anomaly, tipo that did not match the closed list, etc.), or null",\n'
            '      "materiales": [\n'
            '        {\n'
            '          "producto": "string — cargo/product description for THIS container specifically (e.g. from its own row in the BL), or null",\n'
            '          "lote": "string — lot or batch number if stated, or null",\n'
            '          "cant_esperada": "string — expected quantity with unit as stated for THIS container (e.g. 1820 CAS), or null",\n'
            '          "peso": "string — gross weight with unit for THIS container (e.g. 22944.063 KG), or null",\n'
            '          "volumen": "string — volume with unit for THIS container (e.g. 32.684 M3), or null",\n'
            '          "no_orden_compra": "string — PO number specific to THIS cargo line, if different from embarque.no_orden_compra, or null"\n'
            '        }\n'
            '      ]\n'
            '    }\n'
            '  ],\n'

            # ── MATERIALES SIN ASIGNAR ──────────────────────────────────
            '  "materiales": [\n'
            '    {\n'
            '      "producto": "string — cargo/product description that could NOT be attributed to a specific contenedor or remolque, or null",\n'
            '      "lote": "string — lot or batch number if stated, or null",\n'
            '      "cant_esperada": "string — expected quantity with unit if stated, or null",\n'
            '      "peso": "string — gross weight with unit, or null",\n'
            '      "volumen": "string — volume with unit if stated, or null"\n'
            '    }\n'
            '  ],\n'

            # ── EMBARQUE ──────────────────────────────────────────────────
            '  "embarque": {\n'
            '    "proveedor_cliente": "string — shipper, supplier or consignee company name, or null",\n'
            '    "no_orden_compra": "string — shipment-level purchase order / OC number, or null. If multiple containers each carry their own distinct PO, list those in each material\'s own no_orden_compra instead, and put here only a PO that applies to the whole shipment (or leave null if there is none at that level).",\n'
            '    "no_bl": "string — Bill of Lading number, or null",\n'
            '    "no_pedimento": "string — pedimento or customs document number, or null",\n'
            '    "no_autorizacion_puerto": "string — port release authorization number, or null",\n'
            '    "origen": "string — place/port of loading or origin, or null",\n'
            '    "destino": "string — place/port of discharge or delivery, or null",\n'
            '    "fecha_embarque": "string — on-board or shipment date (YYYY-MM-DD if possible), or null"\n'
            '  },\n'

            # ── METADATA ──────────────────────────────────────────────────
            '  "documentos_detectados": [\n'
            '    {\n'
            '      "fuente": "string — imagen_1 / imagen_2 / imagen_3 ... — the position of the FILE in the input list (never a running page count across files — see numbering rule at the top of this prompt)",\n'
            '      "pagina": "integer — page/photo number WITHIN that file (1 for the first page/photo of that file, 2 for the second, etc.), so distinct pages of the same multi-page file can be told apart even though they share the same fuente",\n'
            '      "tipo": "string — one of: identificacion_chofer, foto_conductor, tarjeta_circulacion_vehiculo, tarjeta_circulacion_remolque, carta_porte, carta_de_ruta, factura_orden_compra, foto_placa_vehiculo, evidencia_carga, conocimiento_embarque_bl. IMPORTANT: identificacion_chofer is an official ID document (INE, passport, license) showing the driver\'s personal data. foto_conductor is a photo of the driver\'s face. tarjeta_circulacion_vehiculo belongs to the tractor/cab; tarjeta_circulacion_remolque belongs to a trailer (this also covers a pedimento de importación temporal de remolques, which functions like a trailer registration document) — never confuse the two, and never confuse either with identificacion_chofer / foto_conductor. carta_de_ruta is a Dominican Ministerio de Hacienda / Dirección General de Aduanas internal-transit authorization (fields typically include propietario, sello control, número de contenedor, chofer, compañía transportista, sindicato de camioneros) — this is DIFFERENT from carta_porte (a waybill/manifest), do not conflate the two. THIS FIELD HAS NO otro CATCH-ALL: this value is shown directly to the end user as a label under the uploaded file in the access form, so it must always be informative. If a file does not clearly match any of the listed types, do NOT return \'otro\' — instead return a short, specific description in Spanish of what the document/photo actually is (e.g. \'foto general del contenedor\', \'manifiesto de carga\', \'foto de sello de seguridad\', \'documento no identificado — ilegible\'), written the way a person reviewing the access would want to see it as a label, so it is never a dead-end value like \'otro\'."\n'
            '    }\n'
            '  ],\n'
            '  "observaciones": "string — CTPAT flags, anomalies, damage, incomplete docs, ambiguous plate/document matches, tipos that did not match a closed list, cargo that could not be attributed to a specific unit, or anything security-relevant, or null",\n'
            '  "confianza": "string — alto: all key documents present and legible, no null in critical fields (vehiculo.placa, conductor.nombre, at least one remolque or contenedor if cargo is present), and no unresolved conflicts | medio: 1-2 documents illegible or secondary fields missing | bajo: key documents missing/illegible, or inconsistencies (e.g. unmatched tarjeta/plate, unlinked cargo, conflicting seal numbers, conflicting dates) across sources"\n'
            "}"
        )

        if extra_instructions:
            prompt += f"\n\nAdditional instructions: {extra_instructions}"

        if isinstance(image_source, str):
            image_source = [image_source]
        elif isinstance(image_source, list):
            image_source = [
                img['file_url'] if isinstance(img, dict) else img
                for img in image_source
            ]

        sources = []
        for src in image_source:
            if isinstance(src, str) and src.lower().endswith('.pdf') and src.startswith('http'):
                r = requests.get(src, timeout=30)
                r.raise_for_status()
                b64 = base64.b64encode(r.content).decode('utf-8')
                sources.append(f'data:application/pdf;base64,{b64}')
            else:
                sources.append(src)

        source_index = {f'imagen_{i+1}': src for i, src in enumerate(image_source)}
        print('>>> ocr_acceso_transportista sources=', [s[:80] for s in sources])

        try:
            raw_text = self.ai.ocr_general(sources, system, prompt, model=model, max_tokens=6000)
        except ValueError as e:
            return {'status_code': 500, 'msg': f'Error al parsear respuesta del modelo: {e}'}
        except RuntimeError as e:
            return {'status_code': 500, 'msg': f'Error al llamar a OpenRouter: {e}'}

        datos = {}
        if raw_text.get('choices'):
            choices = raw_text['choices']
            if isinstance(choices, list) and len(choices) > 0:
                content = choices[0].get('message', {}).get('content')
                if content:
                    datos = content

        print('ocr_acceso_transportista datos=', simplejson.dumps(datos, indent=3))

        datos = self._ocr_normalizar(datos)

        # Enriquecer documentos_detectados con la URL original de cada fuente
        if isinstance(datos, dict) and isinstance(datos.get('documentos_detectados'), list):
            for doc in datos['documentos_detectados']:
                fuente = doc.get('fuente', '')
                if fuente in source_index:
                    doc['url'] = source_index[fuente]

        # Resolver no_sello de forma determinista (código, no el LLM) — el LLM solo
        # reporta lo que ve en cada fuente (no_sello_documento / no_sello_fisico); el
        # sello impreso en documentos de texto (BL/factura/carta de ruta) siempre gana
        # sobre uno leído únicamente de una foto, porque suele repetirse/confirmarse en
        # varios documentos independientes mientras que la foto es una sola lectura.
        if isinstance(datos, dict):
            for unidad in (datos.get('remolques') or []) + (datos.get('contenedores') or []):
                if not isinstance(unidad, dict):
                    continue
                sello_doc = unidad.pop('no_sello_documento', None)
                sello_foto = unidad.pop('no_sello_fisico', None)
                if sello_doc:
                    unidad['no_sello'] = sello_doc
                    if sello_foto and sello_foto != sello_doc:
                        nota = f"Sello fotografiado ({sello_foto}) no coincide con el sello documentado ({sello_doc}); se usó el documentado."
                        unidad['comentarios'] = f"{unidad['comentarios']} {nota}" if unidad.get('comentarios') else nota
                else:
                    unidad['no_sello'] = sello_foto or None

        errores = self._ocr_validar_id(datos)

        # Validación determinista: si el propio modelo reportó una observación o una
        # confianza no-alta, no confiar en que ya resolvió el conflicto en el campo
        # correspondiente (p.ej. no_sello) — forzar revisión humana en vez de aceptarlo.
        if isinstance(datos, dict):
            if datos.get('observaciones'):
                errores.append(f"Observación del modelo: {datos['observaciones']}")
            if datos.get('confianza') and datos['confianza'].lower() != 'alto':
                errores.append(f"Confianza reportada por el modelo: {datos['confianza']}")

        if errores:
            return {
                'status_code': 206,
                'msg': 'Extracción con advertencias',
                'data': datos,
                'warnings': errores,
            }

        return {'status_code': datos.get('status_code', 200), 'msg': 'OK', 'data': datos}
    # PRUEBAS

    def ocr_persona(self, image_source,
                    extra_instructions: str = None,
                    model: str = 'google/gemini-2.5-flash-lite') -> dict:
        """
        Analiza una foto para detectar si hay una persona visible
        y extrae sus características físicas descriptivas.

        Args:
            image_source: URL remota, ruta local, o lista de imágenes.
            model:        Modelo OpenRouter a usar.

        Returns:
            dict con:
                - status_code : 200 OK / 206 advertencias / 400 config / 500 error
                - data        : campos extraídos
                - msg         : mensaje de resultado
        """
        if not self.ai:
            return {'status_code': 400, 'msg': 'OpenRouter no configurado'}

        system = (
            "You are a security system specialist trained to analyze images "
            "and determine whether a person is present, and describe their "
            "visible physical characteristics for identification purposes. "
            "You are objective and descriptive. Never make assumptions about "
            "identity, ethnicity, or personal data beyond what is visually evident. "
            "Always respond with a single valid JSON object and nothing else — "
            "no markdown, no backticks, no explanation, no preamble."
        )

        prompt = (
            "Analyze the provided image and determine if a person is visible. "
            "If a person is present, extract all visible physical characteristics. "
            "If no person is detected, return es_persona: false and all other fields as null. "
            "\n\n"
            "Return ONLY a JSON object with this exact structure:\n"
            "{\n"
            '  "es_persona": true,\n'
            '  "cantidad_personas": "integer — number of people visible in the image",\n'
            '  "rostro_visible": "boolean — true if face is clearly visible",\n'
            '  "genero_aparente": "string — masculino / femenino / no determinado",\n'
            '  "edad_estimada": "string — estimated age range e.g. 20-30",\n'
            '  "complexion": "string — delgado / normal / robusto / corpulento",\n'
            '  "estatura_estimada": "string — bajo / mediano / alto based on context clues",\n'
            '  "color_piel": "string — descriptive skin tone in Spanish",\n'
            '  "color_cabello": "string — hair color in Spanish, or null if not visible",\n'
            '  "tipo_cabello": "string — corto / mediano / largo / calvo, or null",\n'
            '  "color_ojos": "string — eye color if visible, else null",\n'
            '  "rasgos_faciales": "string — notable facial features: beard, glasses, mustache, etc., or null",\n'
            '  "ropa_superior": "string — describe upper garment color and type, or null",\n'
            '  "ropa_inferior": "string — describe lower garment color and type, or null",\n'
            '  "accesorios": "string — hat, backpack, bag, jewelry, or null",\n'
            '  "postura": "string — de pie / sentado / en movimiento / acostado, or null",\n'
            '  "calidad_imagen": "string — buena / regular / mala",\n'
            '  "observaciones": "string — anything unusual, suspicious behavior, or notable context",\n'
            '  "confianza": "string — alto / medio / bajo"\n'
            "}"
        )
        prompt += (
            "\n\nKeep every field extremely concise (1-4 words max per field, "
            "except 'observaciones' which can be a short phrase). "
            "Never omit the closing brace of the JSON object."
        )
        if extra_instructions:
            prompt += f"\n\nAdditional instructions: {extra_instructions}"

        # Sanitizar image_source
        if isinstance(image_source, str):
            image_source = [image_source]
        elif isinstance(image_source, list):
            image_source = [
                img['file_url'] if isinstance(img, dict) else img
                for img in image_source
            ]

        print('>>> ocr_persona image_source=', image_source)

        try:
            raw_text = self.ai.ocr_general(image_source, system, prompt, model=model, max_tokens=1500)
        except ValueError as e:
            return {'status_code': 500, 'msg': f'Error al parsear respuesta del modelo: {e}'}
        except RuntimeError as e:
            return {'status_code': 500, 'msg': f'Error al llamar a OpenRouter: {e}'}

        datos = {}
        if raw_text.get('choices'):
            choices = raw_text['choices']
            if isinstance(choices, list) and len(choices) > 0:
                content = choices[0].get('message', {}).get('content')
                if content:
                    datos = content

        print('ocr_persona datos=', datos)

        datos = self._ocr_normalizar(datos)

        errores = self._ocr_validar_id(datos)
        if errores:
            return {
                'status_code': 206,
                'msg': 'Extracción con advertencias',
                'data': datos,
                'warnings': errores,
            }

        return {'status_code': datos.get('status_code', 200), 'msg': 'OK', 'data': datos}

    def ocr_identificacion(self, image_source: str, form_id: int = None,
                           model: str = 'google/gemini-2.5-flash-lite', 
                           name: str = None, is_employee: bool = False) -> dict:
        """
        Extrae los datos de una identificación (INE, pasaporte, licencia, etc.)
        y opcionalmente crea el registro en LinkaForm.

        Args:
            image_source: URL remota o ruta local de la imagen.
            form_id:      Si se proporciona, crea el registro en ese formulario.
            model:        Modelo OpenRouter a usar (opcional).
            MODEL = "anthropic/claude-haiku-4.5"  # excelente OCR, precio razonable
            MODEL = "google/gemini-2.5-flash"  # un escalón arriba, más caro pero mejor
            name:         Si se indica, valida que la identificación pertenezca a esa persona.
            is_employee:  Si es True, busca a la persona de la identificación en el
                          catálogo de empleados (self.Employee.get_employee_data por
                          nombre) y agrega 'es_empleado' (bool) y 'datos_empleado' a
                          cada identificación extraída.

        Returns:
            dict con:
                - status_code: 200/201/400/500
                - data: campos extraídos por el OCR (incluye 'es_empleado' si is_employee=True)
                - folio: folio del registro creado (si se pasó form_id)
                - msg: mensaje de resultado

        Ejemplo de uso en script:
            response = acceso_obj.ocr_identificacion(
                image_source="https://s3.../ine.jpg",
                form_id=self.EMPLEADOS_FORM,
            )
        """

        if not self.ai:
            return {'status_code': 400, 'msg': 'OpenRouter no configurado'}

        # 1. Extraer datos con el LLM
        try:
            raw_text = self.ai.ocr_id(image_source, model=model, name=name)
        except ValueError as e:
            return {'status_code': 500, 'msg': f'Error OCR: {e}'}
        except Exception as e:
            return {'status_code': 500, 'msg': f'Error inesperado: {e}'}

        # 2. Normalizar — esto es código, no LLM
        datos = {}
        if raw_text.get('choices'):
            if isinstance(raw_text['choices'], list) and len(raw_text['choices']) >0:
                if raw_text['choices'][0].get('message',{}).get('content'):
                    datos = raw_text['choices'][0]['message']['content']

        datos = self._ocr_normalizar(datos)
        # 2.5 Verificar si la persona de la identificación es empleado (opcional)
        if is_employee:
            datos = self._ocr_verificar_empleado(datos)

        # 3. Validar
        errores = self._ocr_validar_id(datos)
        if errores:
            return {
                'status_code': 206,  # partial content — extrajo pero hay campos inválidos
                'msg': 'Extracción con advertencias',
                'data': datos,
                'warnings': errores,
            }
        # 4. Crear registro en LinkaForm si se solicitó
        if form_id:
            try:
                result = self._ocr_crear_registro(datos, form_id)
                return {
                    'status_code': 201,
                    'msg': 'Registro creado exitosamente',
                    'data': datos,
                    'folio': result.get('folio'),
                }
            except Exception as e:
                return {
                    'status_code': 500,
                    'msg': f'OCR OK pero error al crear registro: {e}',
                    'data': datos,
                }

    def send_email_and_sms(self, data):
        tipo_notificacion = data.get('tipo', '')
        response = {}

        if tipo_notificacion == 'email':
            response = self.send_email_notification(data, 'Aviso de Acceso', 'Accesos')
        elif tipo_notificacion == 'sms':
            response = self.send_sms_notification(data, 'Accesos')

        if response.get('status_code') >= 400:
            self.LKFException({'title': 'Error al enviar sms', 'msg': f'Response: {response}'})
        
        return response

    def force_quit_all_persons(self, location: str):
        match = {
            "deleted_at": {"$exists": False},
            "form_id": self.BITACORA_ACCESOS,
            f"answers.{self.mf['tipo_registro']}": "entrada",
        }

        if location:
            match[f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}"] = location

        query = [
            {'$match': match},
            {'$project': {
                '_id': 1,
            }},
        ]
        data = self.format_cr(self.cr.aggregate(query))
        format_data = {"data": data,
            "status_code": 200,
            "json": {   
                "msg": "No hay personas dentro por registrar salida."
            }
        }
        if data:
            record_ids = [record.get('_id') for record in data]
            tz_mexico = pytz.timezone('America/Mexico_City')
            now = datetime.now(tz_mexico)
            fecha_hora_str = now.strftime("%Y-%m-%d %H:%M:%S")
            replace_answers = {
                self.mf['fecha_salida']: fecha_hora_str,
                self.mf['tipo_registro']: 'salida',
            }
            response = self.lkf_api.patch_multi_record(answers=replace_answers, form_id=self.BITACORA_ACCESOS, record_id=record_ids)
            if response.get('status_code') in [200, 201, 202]:
                response['json']['msg'] = f'Salida masiva en {location} ejecutada correctamente.'
                format_data = response
            else:
                print('========== Log:', simplejson.dumps(response, indent=2, default=str))
                self.LKFException({'title': 'Error', 'msg': 'Hubo un error al actualizar los registros.'})
        return format_data

    def get_pass_img(self, qr_code):
        answers = {}
        pdf_to_img = self.update_pass_img(qr_code)
        if pdf_to_img:
            answers.update({self.pase_entrada_fields['pdf_to_img']: pdf_to_img})
            response = self.lkf_api.patch_multi_record( answers = answers, form_id=self.PASE_ENTRADA, record_id=[qr_code])
            if response.get('status_code') in [200, 201, 202]:
                url = self.unlist(pdf_to_img).get('file_url') if len(pdf_to_img) > 0 else ''
                return url
            else:
                print('=============', response)
                self.LKFException({'title': 'Error', 'msg': 'Hubo un error al actualizar los registros.'})
        return False

    def pregenerate_pdf(self):
        qr_code = self.answers.get(self.mf['codigo_qr'])
        if qr_code:
            try:
                self.get_pdf(qr_code, template_id=622)
                return True
            except Exception as e:
                print('========== Log:', simplejson.dumps(e, indent=2, default=str))
                return False
        return False

    def update_pass(self, access_pass,folio=None):
        pass_selected= self.get_detail_access_pass(qr_code=folio, get_answers=True)
        qr_code= folio
        _folio= pass_selected.get("folio")
        answers={}
        for key, value in access_pass.items():
            if not self.pase_entrada_fields.get(key):
                continue
            if key == 'grupo_vehiculos':
                answers[self.mf['grupo_vehiculos']]={}
                for index, item in enumerate(access_pass.get('grupo_vehiculos',[])):
                    tipo = item.get('tipo',item.get('tipo_vehiculo',''))
                    marca = item.get('marca',item.get('marca_vehiculo',''))
                    modelo = item.get('modelo',item.get('modelo_vehiculo',''))
                    estado = item.get('estado',item.get('nombre_estado',''))
                    placas = item.get('placas',item.get('placas_vehiculo',''))
                    color = item.get('color',item.get('color_vehiculo',''))
                    obj={
                        self.TIPO_DE_VEHICULO_OBJ_ID:{
                            self.mf['tipo_vehiculo']:tipo,
                            self.mf['marca_vehiculo']:marca,
                            self.mf['modelo_vehiculo']:modelo,
                        },
                        self.ESTADO_OBJ_ID:{
                            self.mf['nombre_estado']:estado,
                        },
                        self.mf['placas_vehiculo']:placas,
                        self.mf['color_vehiculo']:color,
                    }
                    answers[self.mf['grupo_vehiculos']][(index+1)*-1]=obj
            elif key == 'grupo_equipos':
                answers[self.mf['grupo_equipos']]={}
                for index, item in enumerate(value):
                    nombre = item.get('nombre',item.get('nombre_articulo',''))
                    marca = item.get('marca',item.get('marca_articulo',''))
                    color = item.get('color',item.get('color_articulo',''))
                    tipo = item.get('tipo',item.get('tipo_equipo',''))
                    serie = item.get('serie',item.get('numero_serie',''))
                    modelo = item.get('modelo',item.get('modelo_articulo',''))
                    obj={
                        self.mf['tipo_equipo']:tipo.lower(),
                        self.mf['nombre_articulo']:nombre,
                        self.mf['marca_articulo']:marca,
                        self.mf['numero_serie']:serie,
                        self.mf['color_articulo']:color,
                        self.mf['modelo_articulo']:modelo,
                    }
                    answers[self.mf['grupo_equipos']][(index+1)*-1]=obj
            elif key == 'visita_a':
                for index, item in enumerate(access_pass.get('visita_a',[])):
                    answers[self.mf['grupo_visitados']] = answers.get(self.mf['grupo_visitados'],{})
                    answers[self.mf['grupo_visitados']][(index+1)*-1] =self.catalog_visita_a_pases(item)
            elif key == 'status_pase':
                answers.update({f"{self.pase_entrada_fields[key]}":value.lower()})
            elif key == 'archivo_invitacion':
                answers.update({f"{self.pase_entrada_fields[key]}": value})
            elif key == "google_wallet_pass_url":
                answers.update({f"{self.pase_entrada_fields[key]}": value})
            elif key == "apple_wallet_pass":
                answers.update({f"{self.pase_entrada_fields[key]}": value})
            elif key == "pdf_to_img":
                answers.update({f"{self.pase_entrada_fields[key]}": value})
            elif key == 'favoritos':
                answers.update({f"{self.pase_entrada_fields[key]}": [value]})  
            elif key == 'conservar_datos_por':
                answers.update({f"{self.pase_entrada_fields[key]}": value.replace(" ", "_")})      
            else:
                if value:
                    answers.update({f"{self.pase_entrada_fields[key]}":value})

        employee = getattr(self,'employee',self.get_employee_data(email=self.user.get('email'), get_one=True))
        if answers:
            new_answers = deepcopy(pass_selected['answers'])
            new_answers.update(answers)
            status = self.access_pass_set_status(new_answers)
            answers[self.pase_entrada_fields['status_pase']] = status
            res= self.lkf_api.patch_multi_record( answers = answers, form_id=self.PASE_ENTRADA, record_id=[qr_code])
            if res.get('status_code') == 201 or res.get('status_code') == 202 and folio:
                pdf = getattr(self, 'pdf', self.lkf_api.get_pdf_record(qr_code, name_pdf='Pase de Entrada', send_url=True))
                res['json'].update({'qr_pase':pass_selected.get("qr_pase")})
                res['json'].update({'telefono':pass_selected.get("telefono")})
                res['json'].update({'enviar_a':pass_selected.get("nombre")})
                #TODO pregutnar a Paco porque aqui usa el nombre del empeado con el user.get('email')
                #en vez de la persona seleccionada como vista....
                res['json'].update({'enviar_de':employee.get('worker_name')})
                res['json'].update({'enviar_de_correo':employee.get('email')})
                res['json'].update({'ubicacion':pass_selected.get('ubicacion')})
                res['json'].update({'fecha_desde':pass_selected.get('fecha_de_expedicion')})
                res['json'].update({'fecha_hasta':pass_selected.get('fecha_de_caducidad')})
                res['json'].update({'asunto':pass_selected.get('tema_cita')})
                res['json'].update({'descripcion':pass_selected.get('descripcion')})
                res['json'].update({'pdf': pdf})
                return res
            else: 
                return res
        else:
            self.LKFException('No se mandarón parametros para actualizar')
