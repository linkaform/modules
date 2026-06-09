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
            'tolerancia_de_entrada':"6a22155492b193f057990682",
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
            "grupo_nombre": "638a9ab3616398d2e392a9fa",
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

            "documentos_para_ocr": "6a207762cd730fb838ce1bb3",
            "proveedor_cliente_material": "6a207762cd730fb838ce1bb4",
            "material": "6a1ddb53f5a36ba1c7dd029e",
            "cantidad": "6a1ddb53f5a36ba1c7dd029f",
            "orden_de_compra": "6a1ddb53f5a36ba1c7dd02a0",

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
            "token_transportista": "6a20c1811b6edd566116f483"
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

        tipo_de_operacion = data.get('tipo_de_operacion', '')
        tipos_de_operacion_abreviaturas = {
            "entrega_de_materia_prima": "EDMP",
            "recoleccion_de_materia_prima": "RDMP",
            "entrega_de_producto_terminado": "EDPT",
            "recoleccion_de_producto_terminado": "RDPT"
        }

        dominio = data.get('dominio', 'http://localhost:3000')
        abreviatura_url = tipos_de_operacion_abreviaturas[tipo_de_operacion]
        url_pase_transportista = f"{dominio}/transportistas/preview/{abreviatura_url}/{pass_id}"
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
            f['material']:                       mat.get('material', ''),
            f['cantidad']:                       mat.get('cantidad', ''),
            f['orden_de_compra']:                mat.get('orden_compra', ''),
            f['documentos_para_ocr']:            mat.get('documentos', []),
            self.UBICACIONES_CAT_OBJ_ID: {
                self.mf['ubicacion']:            lugar.get('ubicacion', ''),
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
                f['metodo_de_embarque']:            recoleccion.get('metodo_embarque', ''),
                f['incoterm']:                      recoleccion.get('incoterm', ''),
            })

        metadata.update({'answers': answers})
        print(simplejson.dumps(answers, indent=3))
        res = self.lkf_api.post_forms_answers(metadata)
        if res.get('status_code') not in [200, 201, 202]:
            self.LKFException({'title': 'Error al crear pase transportista', 'msg': res})
        res['qr_pase_transportista'] = qr_pase_transportista
        return res

    def create_custom_qr(self, url_for_qr, name_qr, form_id, img_field_id):
        lkf_qr = generar_qr.LKF_QR(self.settings)
        qr_generado = lkf_qr.procesa_qr(url_for_qr, name_qr, form_id, img_field_id)
        return qr_generado

    # PRUEBAS
