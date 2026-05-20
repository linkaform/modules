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
            'pases_incluir': '69974d55879296015c1cd8d2'
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

        self.MENUS_CATALOG = self.lkm.catalog_id('elementos_menu')
        self.MENUS_CATALOG_ID = self.MENUS_CATALOG.get('id')
        self.MENUS_CATALOG_OBJ_ID = self.MENUS_CATALOG.get('obj_id')
        self.MENUS_FORM = self.lkm.form_id('configuracion_menus','id')

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

    #! PERSONALIZACION - SE AGREGAN 2 HORAS DE TOLERANCIA PARA EL INGRESO
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
            
            if fecha_actual < fecha_visita_tz - timedelta(hours=2):
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
        res = self._do_access(access_pass, location, area, data)
        return res