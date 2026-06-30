# -*- coding: utf-8 -*-
import pytz,threading, random, time, unicodedata, tempfile, os
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
            'tipo_de_notificacion': '699dfe3b82be0dbe0319d38c',
            'tipo_rondin': '69b9b98d2a02f4a0dd35f5c1'
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

            'grupo_fotos_y_documentos': '6a3bee0a7829a4ca9572d3a0',
            'tipo_de_documento': '6a3bee394a7a0748a6fc9a56',
            'documento': '6a3bee394a7a0748a6fc9a57',

            'num_de_pase': '6a31921f07fb9cb5840d1f23',
            'empresa_transportista': '6a31929d0bf8c5fc715d7424',
            'tipo_de_operacion': '6a31929d0bf8c5fc715d7425',
            'procedencia': '6a3193dccf1326ad4b7a9a52',
            'tipo_de_vehiculo': '6a3193dccf1326ad4b7a9a53',
            'placas_de_vehiculo': '6a31921f07fb9cb5840d1f24',
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
            'sello_material': '6a42c7a7a1555d53d6b9194e',
            'tipo_material': '6a42c7a7a1555d53d6b9194f',
            'cantidad_material': '6a42c7a7a1555d53d6b91950',
            'peso_material': '6a42c7a7a1555d53d6b91951',
            'volumen_material': '6a42c7a7a1555d53d6b91952',

            'grupo_remolques': '6a31959ed11ece87f2b0052d',
            'tipo_remolque': '6a319693884bec802c94fa44',
            'num_sello': '6a319693884bec802c94fa45',
            'num_caja_contenedor': '6a319693884bec802c94fa46',
            'placas_de_caja': '6a319693884bec802c94fa47',
            'comentarios': '6a319693884bec802c94fa48',

            'grupo_sellos': '6a42c65c03f125df7ad28601',

            'grupo_inspecciones': '6a42a7068dcfbf362329a972',
            'tipo_inspeccion': '6a42c80b03f125df7ad2862b',
            'url_inspeccion': '6a42a71aec3f7153a3d2aea3',
        }

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
            self.pase_entrada_fields['creado_desde']: data.get('creado_desde', 'pase_de_entrada_web'),
            f['tipo_de_operacion']:              data.get('tipo_de_operacion', ''),
            f['nombre_crea_el_pase']:            crea.get('nombre', ''),
            f['email_crea_el_pase']:             crea.get('email', ''),
            f['telefono_crea_el_pase']:          crea.get('telefono', ''),
            f['proveedor']:                      recibe.get('nombre', ''),
            f['proveedor_email']:                recibe.get('email', ''),
            f['proveedor_telefono']:             recibe.get('telefono', ''),
            f['proveedor_cliente_material']:     mat.get('proveedor_cliente', ''),
            f['orden_de_compra']:                mat.get('orden_compra', ''),
            f['grupo_documentos_para_ocr']:       [
                {
                    f['tipo_de_documento']:  doc.get('tipo', ''),
                    f['no_de_documento']:    doc.get('no_doc', ''),
                    f['documento_para_ocr']: [{'file_name': doc.get('file_name', ''), 'file_url': doc.get('file_url', '')}] if doc.get('file_url') else [],
                }
                for doc in mat.get('documentos', [])
            ],
            f['grupo_materiales']:               [
                {
                    f['tipo']:       item.get('tipo', ''),
                    f['cantidad']:   item.get('cantidad', ''),
                    f['volumen']:    item.get('volumen', ''),
                    f['peso']:       item.get('peso', ''),
                    f['sello']:      item.get('sello', ''),
                    f['contenedor']: item.get('contenedor', ''),
                }
                for item in mat.get('items', [])
            ],
            self.UBICACIONES_CAT_OBJ_ID: {
                self.mf['ubicacion']:            lugar.get('ubicacion', ''),
                self.f['address_name']:          [lugar.get('direccion', '')],
            },
            self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID: {
                self.mf['nombre_area_salida']:   lugar.get('area', '')
            },
            f['fecha_pase_transportista_desde']: lugar.get('fecha_pase_transportista_desde', ''),
            f['fecha_pase_transportista_hasta']: lugar.get('fecha_pase_transportista_hasta', ''),
            f['hora_inicial']:                   hora_inicio + ':00' if hora_inicio else '',
            f['hora_final']:                     hora_fin    + ':00' if hora_fin    else '',
            self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                self.mf['nombre_area']: lugar.get('anden', ''),
            },
            f['url_del_pase_transportista']: url_pase_transportista,
            f['qr_del_pase_transportista']: qr_pase_transportista,
            f['estado_transportista']: "pendiente"
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

        metadata.update({'answers': answers})
        print(simplejson.dumps(answers, indent=3))
        res = self.lkf_api.post_forms_answers(metadata)
        if res.get('status_code') not in [200, 201, 202]:
            self.LKFException({'title': 'Error al crear pase transportista', 'msg': res})
        res['qr_pase_transportista'] = qr_pase_transportista
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
            f['tipo_de_operacion']:     data.get('tipo_operacion', '').lower().replace(' ', '_'),
            f['empresa_transportista']: vehiculo.get('transportista', ''),
            f['procedencia']:           vehiculo.get('procedencia', ''),
            f['tipo_de_vehiculo']:      vehiculo.get('tipo_vehiculo', ''),
            f['placas_de_vehiculo']:    vehiculo.get('placa', ''),
            f['num_eco_num_rotulo']:    vehiculo.get('no_economico', ''),
            f['conductor']:             conductor.get('nombre', ''),
            f['ayudante']:              conductor.get('acompanante', ''),
            f['num_licencia']:          conductor.get('no_licencia', ''),
            f['vigencia_licencia']:     conductor.get('vigencia_licencia', ''),
            f['rfc_conductor']:         conductor.get('rfc', ''),
            f['firma_conductor']:       firma,
            f['proveedor_cliente']:     embarque.get('proveedor_cliente', ''),
            f['orden_de_compra']:       embarque.get('no_orden_compra', ''),
        }

        remolques = data.get('remolques', []) or []
        if remolques:
            answers[f['grupo_remolques']] = [
                {
                    f['tipo_remolque']:       r.get('tipo_remolque', ''),
                    f['num_sello']:           r.get('no_sello', ''),
                    f['num_caja_contenedor']: r.get('no_caja', ''),
                    f['placas_de_caja']:      r.get('placas_caja', ''),
                    f['comentarios']:         r.get('comentarios', ''),
                }
                for r in remolques
            ]

        docs = data.get('documentos_adicionales', []) or []
        if docs:
            answers[f['grupo_fotos_y_documentos']] = [
                {
                    f['tipo_de_documento']: doc.get('tipo', '').upper().replace('_', ' '),
                    f['documento']:         [{'file_name': doc.get('file_name', ''), 'file_url': doc['file_url']}] if doc.get('file_url') else [],
                }
                for doc in docs
            ]

        materiales = data.get('materiales', []) or []
        if materiales:
            answers[f['grupo_materiales']] = [
                {
                    f['tipo_material']:     m.get('tipo', ''),
                    f['cantidad_material']: m.get('cantidad', ''),
                    f['peso_material']:     m.get('peso', ''),
                    f['volumen_material']:  m.get('volumen', ''),
                    f['sello_material']:    m.get('sello', ''),
                }
                for m in materiales
            ]

        metadata.update({'answers': answers})
        res = self.lkf_api.post_forms_answers(metadata)
        if res.get('status_code') not in [200, 201, 202]:
            self.LKFException({'title': 'Error al crear visita de transportista', 'msg': res})
        return res

    def create_custom_qr(self, url_for_qr, name_qr, form_id, img_field_id):
        lkf_qr = generar_qr.LKF_QR(self.settings)
        qr_generado = lkf_qr.procesa_qr(url_for_qr, name_qr, form_id, img_field_id)
        return qr_generado

    def ocr_acceso_transportista(self, image_source,
                                  extra_instructions: str = None,
                                  model: str = 'google/gemini-2.5-flash') -> dict:
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
            "driver photos, driver licenses, vehicle registration cards (tarjeta de circulación), "
            "Bills of Lading, temporary import permits (pedimentos), port release documents, "
            "purchase orders, cargo manifests, and container photos. "
            "All inputs refer to ONE transport access event. "
            "You ONLY extract information that is clearly visible or printed in the provided files. "
            "You NEVER invent, estimate, or hallucinate data. "
            "If a field is not present in any document, return null — never guess. "
            "Always respond with a single valid JSON object and nothing else — "
            "no markdown, no backticks, no explanation, no preamble."
        )

        prompt = (
            "Analyze all provided files (images and/or documents) as a single transport access event. "
            "The files are provided in order: the first is imagen_1, the second is imagen_2, and so on. "
            "Files may include vehicle photos, driver photos, driver licenses, vehicle registration cards, "
            "Bills of Lading, pedimentos, port documents, purchase orders, or container photos. "
            "Extract every field you can find. If a field is absent from all provided files, use null. "
            "\n\n"
            "Return ONLY a JSON object with this exact structure:\n"
            "{\n"

            # ── VEHÍCULO ──────────────────────────────────────────────────
            '  "vehiculo": {\n'
            '    "transportista": "string — carrier company name (e.g. TRAMO TRANSPORTES MONTERREY SA DE CV), or null",\n'
            '    "tipo_vehiculo": "string — one of: torton, trailer, caja seca, caja refrigerada, plataforma, volteo, van, pick up, camión, pipa, contenedor, or null",\n'
            '    "marca": "string — truck/tractor brand (Kenworth, Freightliner, International, Volvo, etc.), or null",\n'
            '    "modelo": "string — truck model or year if visible, or null",\n'
            '    "color": "string — main cab color. PRIORITY: extract visually from vehicle/plate photos if provided. Fall back to text on registration card only if no vehicle photo is present. Use Spanish color names (Blanco, Negro, Rojo, Azul, Gris, Verde, Amarillo, Naranja, Cafe, Plateado, etc.), or null",\n'
            '    "placa": "string — tractor/cab license plate exactly as printed, or null",\n'
            '    "estado_placa": "string — Mexican state or country of the tractor plate, or null",\n'
            '    "no_economico": "string — carrier economic number / rótulo on the vehicle, or null",\n'
            '    "no_serie": "string — chassis/VIN if visible on registration card or document, or null"\n'
            '  },\n'

            # ── CONDUCTOR ─────────────────────────────────────────────────
            '  "conductor": {\n'
            '    "nombre": "string — driver full name from license or permit document, or null",\n'
            '    "no_licencia": "string — driver license number exactly as printed, or null",\n'
            '    "vigencia_licencia": "string — license expiration date (YYYY-MM-DD if possible), or null",\n'
            '    "rfc": "string — RFC if shown on any document, or null"\n'
            '  },\n'

            # ── REMOLQUES ─────────────────────────────────────────────────
            '  "remolques": [\n'
            '    {\n'
            '      "tipo_remolque": "string — caja seca, caja refrigerada, plataforma, contenedor, tanque, etc., or null",\n'
            '      "marca": "string — trailer brand (Wabash, Hyundai, etc.) from registration card, or null",\n'
            '      "modelo": "string — trailer model/year from registration card, or null",\n'
            '      "placa_caja": "string — trailer license plate exactly as printed, or null",\n'
            '      "no_economico": "string — trailer economic number / RTI number, or null",\n'
            '      "no_serie": "string — trailer serial number from registration card, or null"\n'
            '    }\n'
            '  ],\n'

            # ── CARGA / MATERIALES ────────────────────────────────────────
            '  "carga": [\n'
            '    {\n'
            '      "no_contenedor": "string — container number (e.g. ECMU7740351), or null",\n'
            '      "no_sello": "string — seal number, or null",\n'
            '      "tipo_contenedor": "string — 40HC, 20GP, 40GP, tank, etc., or null",\n'
            '      "descripcion": "string — cargo description (e.g. 1305 CAJAS CON CERVEZAS), or null",\n'
            '      "cantidad": "string — quantity and unit if stated, or null",\n'
            '      "peso_bruto": "string — gross weight with unit (e.g. 19,603.50 KGS), or null",\n'
            '      "volumen": "string — volume with unit if stated, or null"\n'
            '    }\n'
            '  ],\n'

            # ── EMBARQUE / DOCUMENTOS ─────────────────────────────────────
            '  "embarque": {\n'
            '    "no_bl": "string — Bill of Lading number, or null",\n'
            '    "no_orden_compra": "string — purchase order / OC number, or null",\n'
            '    "proveedor": "string — shipper/supplier company name, or null",\n'
            '    "cliente": "string — consignee/buyer company name, or null",\n'
            '    "origen": "string — place/port of loading or origin, or null",\n'
            '    "destino": "string — place/port of discharge or delivery, or null",\n'
            '    "naviera": "string — shipping line name (CMA CGM, MSC, etc.), or null",\n'
            '    "vessel": "string — vessel/ship name, or null",\n'
            '    "fecha_embarque": "string — on-board or shipment date, or null",\n'
            '    "no_pedimento": "string — pedimento or customs document number, or null",\n'
            '    "no_autorizacion_puerto": "string — port release authorization number, or null"\n'
            '  },\n'

            # ── METADATA ──────────────────────────────────────────────────
            '  "documentos_detectados": [\n'
            '    {\n'
            '      "fuente": "string — imagen_1 / imagen_2 / imagen_3 ... (position of the file in the input list)",\n'
            '      "tipo": "string — document type: bill_of_lading, tarjeta_circulacion_tractor, tarjeta_circulacion_remolque, licencia_conducir, pedimento, autorizacion_puerto, orden_compra, foto_vehiculo, foto_placa, foto_conductor, foto_contenedor, manifiesto_carga, otro"\n'
            '    }\n'
            '  ],\n'
            '  "observaciones": "string — CTPAT flags, anomalies, damage, incomplete docs, or anything security-relevant, or null",\n'
            '  "confianza": "string — alto / medio / bajo — overall confidence based on document/image quality"\n'
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

        raw_text = self.ai.ocr_general(sources, system, prompt, model=model, max_tokens=2000)

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

        errores = self._ocr_validar_id(datos)
        if errores:
            return {
                'status_code': 206,
                'msg': 'Extracción con advertencias',
                'data': datos,
                'warnings': errores,
            }

        return {'status_code': datos.get('status_code', 200), 'msg': 'OK', 'data': datos}

    # PRUEBAS
