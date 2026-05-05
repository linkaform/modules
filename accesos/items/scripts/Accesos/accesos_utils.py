# -*- coding: utf-8 -*-
import pytz,threading, random, time
import sys, simplejson, json, pytz, base64, requests

from datetime import datetime, timedelta, date
from math import ceil
from bson import ObjectId
#borrar cuando nos llevemos update_pass
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor, as_completed


from linkaform_api import base, generar_qr
from lkf_addons.addons.accesos.app import Accesos


class Accesos(Accesos):
    print('Entra a acceos utils')

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

        #BORRAR
        self.CONFIGURACION_RECORRIDOS = self.lkm.catalog_id('configuracion_de_recorridos')
        self.CONFIGURACION_RECORRIDOS_ID = self.CONFIGURACION_RECORRIDOS.get('id')
        self.CONFIGURACION_RECORRIDOS_OBJ_ID = self.CONFIGURACION_RECORRIDOS.get('obj_id')
        self.REGISTRO_ASISTENCIA = self.lkm.form_id('registro_de_asistencia','id')
        self.FORMATO_VACACIONES = self.lkm.form_id('formato_vacaciones_aviso','id')
        self.USUARIOS_FORM = self.lkm.form_id('usuarios', 'id')
        self.ENVIO_DE_NOTIFICACIONES_FORM = self.lkm.form_id('envio_de_notificaciones', 'id')

        # self.bitacora_fields.update({
        #     "catalogo_pase_entrada": "66a83ad652d2643c97489d31",
        #     "gafete_catalog": "66a83ace56d1e741159ce114"
        # })

        # self.cons_f.update({
        #     "catalogo_ubicacion_concesion": "66a83a74de752e12018fbc3c",
        # })

        self.CONFIGURACION_DE_RECORRIDOS_FORM = self.lkm.form_id('configuracion_de_recorridos','id')

        self.f.update({
            'areas_del_rondin': '66462aa5d4a4af2eea07e0d1',
            'comentario_area_rondin': '66462b9d7124d1540f962088',
            'comentario_check_area': '681144fb0d423e25b42818d4',
            'estatus_del_recorrido': '6639b2744bb44059fc59eb62',
            'fecha_hora_inspeccion_area': '6760a908a43b1b0e41abad6b',
            'fecha_programacion':'6760a8e68cef14ecd7f8b6fe',
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

        
        centralized_fields = {
            'categoria':'686807d46e41614d708f6fc9',
            'icono_categoria':'686807d46e41614d708f6fca',
            'sub_categoria': '686807a7ee7705c5c8eb181a',
            'icono_sub_categoria': '686807a7ee7705c5c8eb181b',
            'incidencia': '663973809fa65cafa759eb97',
            'area': '663e5d44f5b8a7ce8211ed0f',
            'ubicacion': '663e5c57f5b8a7ce8211ed0b',
            'address': '663a7e0fe48382c5b1230902',
            'address_complement': '663a7f79e48382c5b123090a',
            'address_email': '663a7ee1e48382c5b1230907',
            'tipo_de_area': '663e5e68f5b8a7ce8211ed18',
            'num_de_habitacion': '680977786d022b9bff6e3645',
            'piso': '680977786d022b9bff6e3646',
            'address_name': '663a7e0fe48382c5b1230901',
            'address_phone': '663a7ee1e48382c5b1230906',
            'address_geolocation': '663e5c8cf5b8a7ce8211ed0c',
            'area_city': '6654187fc85ce22aaf8bb070',
            'area_status': '663e5e4bf5b8a7ce8211ed15',
            'area_state': '663e5e4bf5b8a7ce8211ed14',
            'area_qr_code': '663e5e4bf5b8a7ce8211ed13',
            'area_tag_id': '6762f7b0922cc2a2f57d4044',
            'area_foto': '6763096aa99cee046ba766ad',
            'ubicacion_nombre_comercial': '667468e3e577b8b98c852aaa',
            'address_type': '663a7f67e48382c5b1230908',
            'ubicacion_country': '663a7ca6e48382c5b12308fa',
            'nombre_del_recorrido': '6645050d873fc2d733961eba',
            'usuario_id': '638a9a99616398d2e392a9f5',
            'usuario_nombre_completo': '638a9a7767c332f5d459fc81',
            'usuario_username': '6759e4a7a9a6e13c7b26da33',
            'usuario_email': '638a9a7767c332f5d459fc82',
            'usuario_phone': '67be0c43a31e5161c47f2bba',
            'empleado_nombre_completo': '62c5ff407febce07043024dd',
            'empleado_departamento': '663bc4ed8a6b120eab4d7f1e',
            'empleado_puesto': '663bc4c79b8046ce89e97cf4',
            'empleado_rfc': '663bcbe2274189281359eb71',
            'empleado_estatus_disponibilidad': '663bcbe2274189281359eb78',
            'tipo_de_equipo': '6639a9d9d38959539f59eb9f',
            'falla_concepto': '66397bae9e8b08289a59ec86',
            'falla_subconcepto': '679124a8483c5220455bcb99',
            'empleado_apoyo_nombre_completo': '663bd36eb19b7fb7d9e97ccb',
            'empleado_apoyo_id': '663bd466b19b7fb7d9e97cdc',
            'empleado_apoyo_estatus_disponibilidad': '663bd5c0b6e749213859eb72',
            'area_salida': '663fb45992f2c5afcfe97ca8',
            'visita_autorizada_nombre_completo': '5ea0693a0c12d5a8e43d37df',
            'visita_autorizada_curp': '5ea0897550b8dfe1f4d83a9f',
            'visita_autorizada_email': '5ea069562f8250acf7d83aca',
            'visita_autorizada_telefono': '663ec042713049de31e97c93',
            'visita_autorizada_foto': '5ea35de83ab7dad56c66e045',
            'visita_autorizada_identificacion': '65ce34985fa9df3dbf9dd2d0',
            'visita_autorizada_razon_social': '65fc814fb170488cf4d44c51',
            'visita_autorizada_email_contratista': '65fc814fb170488cf4d44c53',
            'visita_autorizada_estatus': '5ea1bd280ae8bad095055e61',
            'estados_state': '663a7dd6e48382c5b12308ff',
            'estados_state_code': '663a7dd6e48382c5b1230900',
            'proveedores_nombre_comercial': '667468e3e577b8b98c852aaa',
            'proveedores_razon_social': '6687f2f37b2c023e187d6252',
            'proveedores_rfc': '667468e3e577b8b98c852aab',
            'proveedores_email_contacto': '66bfd647cd15883ed163e9b5',
            'proveedores_telefono': '66bfd666cd15883ed163e9b6',
            'proveedores_web': '66bfd66ecd15883ed163e9b7',
            'lockers_id': '66480101786e8cdb66e70124',
            'lockers_tipo': '66ccfec6acaa16b31e5593a3',
            'lockers_estatus': '663961d5390b9ec511e97ca5',
            'objetos_categoria': '66ce23efc5c4d148311adf86',
            'objetos_descripcion': '66ce23efc5c4d148311adf87',
            'objetos_nombre': '66ce2441d63bb7a3871adeaf',
        }
        # self.f.update(centralized_fields)
        # forced_values = set(centralized_fields.values())
        # for k, v in list(self.f.items()):
        #     if v in forced_values and k not in centralized_fields:
        #         del self.f[k]
        # self.f.update(centralized_fields)
        
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
                    "id": 1,
                    "start": start_datetime,
                    "stop": stop_datetime,
                    "name": tema_cita,
                    "description": descripcion,
                    "location": geolocation,
                    "allday": False,
                    "rrule": None,
                    "alarm_ids": [{"interval": "minutes", "duration": 10, "name": "Reminder"}],
                    'organizer_name': visita_a,
                    'organizer_email': creado_por_email,
                    "attendee_ids": attendee_ids,
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
        res = {self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID: {
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
            else:
                employee = self.Employee.get_employee_data(name = visita, get_one=True)
                self.employee = employee
            visita_set.update(self.visita_a_set_format(employee))
            if visita_set and self.employee:
                res.append(visita_set)
            else: 
                visita_set = {self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID: {
                self.mf['nombre_empleado'] : visita}}
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

    def get_area_images(self, areas, location=None):
        if not location:
            location = self.answers.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.Location.f['location'], '')
        format_areas = []
        for area in areas:
            if isinstance(area, dict):
                area = area.get(self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.Location.f['area'], '')
            if area:
                format_areas.append(area)
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.AREAS_DE_LAS_UBICACIONES,
                f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": location,
                f"answers.{self.Location.f['area']}": {"$in": format_areas}
            }},
            {"$project": {
                "_id": 0,
                "tag_id": f"$answers.{self.f['area_tag_id']}",
                "ubicacion": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}",
                "area": f"$answers.{self.Location.f['area']}",
                "tipo_de_area": f"$answers.{self.Location.TIPO_AREA_OBJ_ID}.{self.f['tipo_de_area']}",
                "foto_del_area": f"$answers.{self.f['area_foto']}",
            }}
        ]
        res = self.cr.aggregate(query)
        format_res = list(res)
        return format_res

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
        print('guards_positions',guards_positions)
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
            elif key == 'quien_recibe_otro':
                answers[self.cons_f['quien_recibe_otro']] = value
            else:
                answers.update({f"{self.paquetes_fields[key]}":value})
        metadata.update({'answers':answers})
        res=self.lkf_api.post_forms_answers(metadata)
        return res

    def get_paquetes(self, location= "", area="", status="", dateFrom="", dateTo="", filterDate=""):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PAQUETERIA,
        }
        if location:
             match_query[f"answers.{self.paquetes_fields['ubicacion_paqueteria']}"] = location
        if area:
             match_query[f"answers.{self.paquetes_fields['area_paqueteria']}"] = area
        if status:
             match_query[f"answers.{self.paquetes_fields['estatus_paqueteria']}"] = status

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
                f"answers.{self.paquetes_fields['fecha_recibido_paqueteria']}": {"$gte": dateFrom, "$lte": dateTo},
            })
        elif dateFrom:
            match_query.update({
                f"answers.{self.paquetes_fields['fecha_recibido_paqueteria']}": {"$gte": dateFrom}
            })
        elif dateTo:
           match_query.update({
                f"answers.{self.paquetes_fields['fecha_recibido_paqueteria']}": {"$lte": dateTo}
            })
        print("HOLAA")
        query = [
            {'$match': match_query },
            {'$project': {
                "folio":"$folio",
                "_id":"$_id",
                'created_at':'$created_at',
                'ubicacion_paqueteria':f"$answers.{self.paquetes_fields['ubicacion_paqueteria']}",
                'area_paqueteria': f"$answers.{self.paquetes_fields['area_paqueteria']}",
                'fotografia_paqueteria':f"$answers.{self.paquetes_fields['fotografia_paqueteria']}",
                'descripcion_paqueteria':f"$answers.{self.paquetes_fields['descripcion_paqueteria']}",
                'quien_recibe_paqueteria':f"$answers.{self.paquetes_fields['quien_recibe_cat']}.{self.paquetes_fields['quien_recibe_paqueteria']}",
                'guardado_en_paqueteria': f"$answers.{self.paquetes_fields['guardado_en_paqueteria']}",
                'fecha_recibido_paqueteria': f"$answers.{self.paquetes_fields['fecha_recibido_paqueteria']}",
                'fecha_entregado_paqueteria': f"$answers.{self.paquetes_fields['fecha_entregado_paqueteria']}",
                'estatus_paqueteria': f"$answers.{self.paquetes_fields['estatus_paqueteria']}",
                'entregado_a_paqueteria': f"$answers.{self.paquetes_fields['entregado_a_paqueteria']}",
                'proveedor': f"$answers.{self.paquetes_fields['proveedor_cat']}.{self.paquetes_fields['proveedor']}",
                'quien_recibe_otro': f"$answers.{self.cons_f['quien_recibe_otro']}",
            }},
            {'$sort':{'created_at':-1}},
        ]
        if not filterDate:
            query.append(
                {"$limit":25}
            )
        pr= self.format_cr_result(self.cr.aggregate(query))
        for x in pr:
            status = x.get('estatus_paqueteria', [])
            x['estatus_paqueteria'] = status.pop() if status else ""
        return pr
   
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
            elif key == 'quien_recibe_otro':
                answers[self.cons_f['quien_recibe_otro']] = value
            else:
                answers.update({f"{self.paquetes_fields[key]}":value})
        if answers or folio:
            return self.lkf_api.patch_multi_record( answers = answers, form_id=self.PAQUETERIA, folios=[folio])
        else:
            self.LKFException('No se mandarón parametros para actualizar')

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
            'areas_del_rondin':f"$answers.{self.f['areas_del_rondin']}",
            
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
        id_vista  = answers[self.pase_entrada_fields['walkin_identificacion']]
        if isinstance(id_vista, list) and len(id_vista) > 0:
            id_vista = id_vista[0]

        if isinstance(id_vista, dict):
            if 'file_url' in id_vista.keys() and id_vista['file_url']:
                id_vista = self.valid_url(id_vista['file_url'])
        id_vista = True
        today = self.get_today_format()
        if isinstance(today, datetime):
            today = today.strftime('%Y-%m-%d')
        elif today and isinstance(today, str) and len(today) > 10:
            today = today[:10]

        try:
            val_visita = answers[self.pase_entrada_fields['fecha_desde_visita']]
            if isinstance(val_visita, datetime):
                fecha_desde_visita = val_visita.strftime('%Y-%m-%d')
            else:
                fecha_desde_visita = self.valid_date(val_visita)
                if fecha_desde_visita:
                    if isinstance(fecha_desde_visita, datetime):
                        fecha_desde_visita = fecha_desde_visita.strftime('%Y-%m-%d')
                    elif isinstance(fecha_desde_visita, str) and len(fecha_desde_visita) > 10:
                        fecha_desde_visita = fecha_desde_visita[:10]
        except Exception as e:
            print(f"DEBUG DESDE ERROR: {e}")
            fecha_desde_visita = None

        try:
            val_hasta = answers[self.pase_entrada_fields['fecha_desde_hasta']]
            if isinstance(val_hasta, datetime):
                fecha_desde_hasta = val_hasta.strftime('%Y-%m-%d')
            else:
                fecha_desde_hasta = self.valid_date(val_hasta)
                if fecha_desde_hasta:
                    if isinstance(fecha_desde_hasta, datetime):
                        fecha_desde_hasta = fecha_desde_hasta.strftime('%Y-%m-%d')
                    elif isinstance(fecha_desde_hasta, str) and len(fecha_desde_hasta) > 10:
                        fecha_desde_hasta = fecha_desde_hasta[:10]
        except Exception as e:
            print(f"DEBUG HASTA ERROR: {e}")
            fecha_desde_hasta = None
            
        if fecha_desde_hasta and today <= fecha_desde_hasta: 
            fecha_ok = True
        else:
            print(f"DEBUG FECHA_OK FALSE: today={today}, desde={fecha_desde_visita}, hasta={fecha_desde_hasta}")
        
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

    def autorizar_pase_acceso(self, answers):
        autorizado_por = {}
        #TODO FLUJO DE AUTORIZACION
        if not self.use_api or True:
            first_name = self.user.get('first_name')
            if not first_name:
                first_name = self.settings.config['USER']['name']
            autorizado_por = {self.pase_entrada_fields['autorizado_por']:first_name}
        return autorizado_por 

    def update_full_pass(self, access_pass,folio=None, qr_code=None, location=None):
        answers = {}
        perfil_pase = access_pass.get('perfil_pase', 'Visita General')
        user_data = self.lkf_api.get_user_by_id(self.user.get('user_id'))
        this_user = self.get_employee_data(user_id=self.user.get('user_id'), get_one=True)
        this_user_name = this_user.get('worker_name', '')
        timezone = user_data.get('timezone','America/Monterrey')
        now_datetime =self.today_str(timezone, date_format='datetime')
        answers[self.mf['grupo_visitados']] = []
        employee = self.get_employee_data(email=self.user.get('email'), get_one=True)
        nombre_visita_a = employee.get('worker_name')

        # answers[self.UBICACIONES_CAT_OBJ_ID] = {}
        # answers[self.UBICACIONES_CAT_OBJ_ID][self.f['location']] = location
        answers[self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID] = {}
        answers[self.CONFIG_PERFILES_OBJ_ID] = {}
        answers[self.VISITA_AUTORIZADA_CAT_OBJ_ID] = {}
        # answers[self.pase_entrada_fields['qr_pase']] = []
        for key, value in access_pass.items():
            if key == 'grupo_vehiculos':
                vehiculos = access_pass.get('grupo_vehiculos',[])
                if vehiculos:
                    list_vehiculos = []
                    for item in vehiculos:
                        tipo = item.get('tipo_vehiculo', item.get('tipo', ''))
                        marca = item.get('marca_vehiculo', item.get('marca', ''))
                        modelo = item.get('modelo_vehiculo', item.get('modelo', ''))
                        estado = item.get('state', item.get('estado', ''))
                        placas = item.get('placas_vehiculo', item.get('placas', ''))
                        color = item.get('color_vehiculo', item.get('color', ''))
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
            elif key == 'grupo_equipos':
                equipos = access_pass.get('grupo_equipos',[])
                if equipos:
                    list_equipos = []
                    for item in equipos:
                        tipo = item.get('tipo_equipo', item.get('tipo', '')).lower().replace(' ', '_')
                        nombre = item.get('nombre_articulo', item.get('nombre', ''))
                        marca = item.get('marca_articulo', item.get('marca', ''))
                        modelo = item.get('modelo_articulo', item.get('modelo', ''))
                        color = item.get('color_articulo', item.get('color', ''))
                        serie = item.get('numero_serie', item.get('serie', ''))
                        list_equipos.append({
                            self.mf['tipo_equipo']:tipo,
                            self.mf['nombre_articulo']:nombre,
                            self.mf['marca_articulo']:marca,
                            self.mf['modelo_articulo']:modelo,
                            self.mf['color_articulo']:color,
                            self.mf['numero_serie']:serie,
                        })
                    answers[self.mf['grupo_equipos']] = list_equipos
            elif key == 'grupo_instrucciones_pase':
                acciones = access_pass.get('grupo_instrucciones_pase',[])
                if acciones:
                    acciones_list = []
                    for c in acciones:
                        acciones_list.append(
                            {
                                self.pase_entrada_fields['tipo_comentario']:c.get('tipo_comentario'),
                                self.pase_entrada_fields['comentario_pase'] :c.get('comentario_pase')
                            }
                        )
                    answers.update({self.pase_entrada_fields['grupo_instrucciones_pase']:acciones_list})
            elif key == 'grupo_areas_acceso':
                acciones = access_pass.get('grupo_areas_acceso',[])
                if acciones:
                    acciones_list = []
                    for c in acciones:
                        acciones_list.append(
                            {
                                self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID : {
                                    self.pase_entrada_fields['nombre_area']:c.get('nombre_area')
                                } ,
                                self.pase_entrada_fields['commentario_area'] :c.get('commentario_area')
                            }
                        )
                    answers.update({self.pase_entrada_fields['grupo_areas_acceso']:acciones_list})
            elif key == 'autorizado_por':
                answers[self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID] = {
                    self.mf['nombre_guardia_apoyo'] : this_user_name,
                }
            elif key == 'link':
                link_info=access_pass.get('link', '')
                if link_info:
                    docs=""
                    for index, d in enumerate(link_info["docs"]): 
                        if(d == "agregarIdentificacion"):
                            docs+="iden"
                        elif(d == "agregarFoto"):
                            docs+="foto"
                        if index==0 :
                            docs+="-"
                    link_pass= f"{link_info['link']}?id={link_info['qr_code']}&user={link_info['creado_por_id']}&docs={docs}"

                answers.update({f"{self.pase_entrada_fields[key]}":link_pass}) 
            elif key == 'ubicacion':
                # answers[self.pase_entrada_fields['ubicacion_cat']] = {self.mf['ubicacion']:access_pass['ubicacion']}
                ubicaciones = access_pass.get('ubicacion',[])
                if ubicaciones:
                    ubicaciones_list = []
                    for ubi in ubicaciones:
                        ubicaciones_list.append(
                            {
                                self.pase_entrada_fields['ubicacion_cat']:{ self.mf["ubicacion"] : ubi}
                            }
                        )
                    answers.update({self.pase_entrada_fields['ubicaciones']:ubicaciones_list})
            elif key == 'created_from':     
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

            elif key == 'visita_a': 
                answers[self.mf['grupo_visitados']] = self.access_pass_vista_a(access_pass.get('visita_a',[]))
    
            elif key == 'perfil_pase':
                # Perfil de Pase
                answers[self.CONFIG_PERFILES_OBJ_ID] = {}
                answers[self.CONFIG_PERFILES_OBJ_ID] = {
                    self.mf['nombre_perfil'] : perfil_pase,
                }
                options = {
                      "group_level": 2,
                      "startkey": [perfil_pase],
                      "endkey": [f"{perfil_pase}\n",{}],
                    }
                cat_perfil = self.catalogo_view(self.CONFIG_PERFILES_ID, self.PASE_ENTRADA, options)
                if len(cat_perfil) > 0:
                    cat_perfil[0][self.mf['motivo']]= [cat_perfil[0].get(self.mf['motivo'])]
                    cat_perfil = cat_perfil[0]
                answers[self.CONFIG_PERFILES_OBJ_ID].update(cat_perfil)
                if answers[self.CONFIG_PERFILES_OBJ_ID].get(self.mf['nombre_permiso']) and \
                   type(answers[self.CONFIG_PERFILES_OBJ_ID][self.mf['nombre_permiso']]) == str:
                    answers[self.CONFIG_PERFILES_OBJ_ID][self.mf['nombre_permiso']] = [answers[self.CONFIG_PERFILES_OBJ_ID][self.mf['nombre_permiso']],]
            elif key == 'archivo_invitacion':
                # id_forma = 121736
                id_forma = self.PASE_ENTRADA
                # id_campo = '673773741b2adb2d05d99d63'
                id_campo = self.pase_entrada_fields['archivo_invitacion']
                tema_cita = access_pass.get("tema_cita")
                descripcion = access_pass.get("descripcion")
                fecha_desde_visita = access_pass.get("fecha_desde_visita")
                fecha_desde_hasta = access_pass.get("fecha_desde_hasta")
                creado_por_email = access_pass.get("link", {}).get("creado_por_email")
                ubicacion = access_pass.get("ubicacion",'')
                nombre = access_pass.get("nombre_pase",'')
                visita_a = access_pass.get("visita_a",'')
                email = access_pass.get("email_pase",'')

                start_datetime = datetime.strptime(fecha_desde_visita, "%Y-%m-%d %H:%M:%S")

                if not fecha_desde_hasta:
                    stop_datetime = start_datetime + timedelta(hours=1)
                else:
                    stop_datetime = datetime.strptime(fecha_desde_hasta, "%Y-%m-%d %H:%M:%S")

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
                respuesta_ics = self.upload_ics(id_forma, id_campo, meetings=meeting)
                file_name = respuesta_ics.get('file_name', '')
                file_url = respuesta_ics.get('file_url', '')

                archivo_invitacion= [
                    {
                        "file_name": f"{file_name}",
                        "file_url": f"{file_url}"
                    }
                ]
                answers.update({f"{self.pase_entrada_fields[key]}": archivo_invitacion})
            else:
                answers.update({f"{self.pase_entrada_fields[key]}":value})

        if answers or folio:
            metadata = self.lkf_api.get_metadata(form_id=self.PASE_ENTRADA)
            metadata.update(self.get_record_by_folio(folio, self.PASE_ENTRADA, select_columns={'_id':1}, limit=1))

            metadata.update({
                    'properties': {
                        "device_properties":{
                            "system": "Addons",
                            "process":"Actualizacion de Pase de Entrada", 
                            "accion":'update_full_pass', 
                            "folio": folio, 
                            "archive": "pase_acceso.py"
                        }
                    },
                    'answers': answers,
                    '_id': qr_code
                })
            res= self.net.patch_forms_answers(metadata)
            return res
        else:
            self.LKFException('No se mandarón parametros para actualizar')
     
    def catalogo_tipo_concesion(self,location="", tipo=""):
        catalog_id = self.ACTIVOS_FIJOS_CAT_ID
        form_id= self.CONCESSIONED_ARTICULOS
        options={}
        response=[]
        if location and not tipo:
            response= self.catalogo_view(catalog_id, form_id)
        else:

            if location and tipo:
                options = {
                    "group_level": 2,
                    "startkey": [tipo],
                    "endkey": [f"{tipo}\n"]
                }
                res= self.catalogo_view(catalog_id, form_id, options)
                format_data = []
                if res:
                    # Se obtienen datos extras de los articulos
                    # Nombre, imagen y costo.
                    format_data = self.get_more_info_conscessioned_articles(res)
                    response=format_data

            elif tipo and not location:
                self.LKFException('Location es requerido')
        print(response)
        return response

    def assets_access_pass(self, location):
        """
        Regresa diccionario con las areas, personas que puede visitar en esa ubicacion y los perfiles
            
        args:
            location (str|list): Nombre de la ubicacion

        returns:
            {
            Areas:[ lista de areas ],
            Vistia_a:[ lista de personas ]
            Perfiles:[ lista de prefiles ]
            }
        """
        ### Areas
        try:
            areas = self.get_areas_by_location(location)
        except:
            areas = []
        ### Aquien Visita
        try:
            visita_a =  self.Employee.get_users_by_location_area(location_name=location)
            visita_a = [x['name'] for x in visita_a if x.get('name')]
        except:
            visita_a = []
        ### Perfiles de accesos
        # try:
        #     perfiles = self.get_pefiles_walkin(location)
        # except:
        #     perfiles = []
        try:
            config_modulo = self.get_config_modulo_seguridad(location)
            requerimientos = config_modulo.get('requerimientos',[])
            envios = config_modulo.get('envios',[])
            perfiles = config_modulo.get('tipos',[])
        except:
            Perfiles = []
            envios = []
            requerimientos = []
        res = {
            'Areas': areas,
            'Visita_a': visita_a,
            'Perfiles': perfiles,
            'requerimientos': requerimientos,
            'envios':envios,
            'Perfiles':perfiles

        }
        return res

    def update_guards_checkin(self, data_guard, record_id, location, area, user_data={}, nombre_suplente="", foto_checkin=[]):
        response = []
        timezone = user_data.get('timezone', 'America/Monterrey')
        now_datetime =self.today_str(timezone, date_format='datetime')
        checkin = self.check_in_out_employees(
            'in',
            now_datetime,
            checkin={},
            employee_list=data_guard,
            **{'employee_type': self.support_guard}
        )
        
        for idx, employee in enumerate(checkin.get(self.mf['guard_group'],[])):
            user_id = employee[self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID].get(self.mf['id_usuario'])
            
            validate_status = self.get_employee_checkin_status(user_id)
            not_allowed = [user_id for user_id, user_data in validate_status.items() if user_data.get('status') == 'in']
            if not_allowed:
                msg = f"El usuario con id {not_allowed}. Se encuentra actualmente registrado en una caseta."
                msg += f"Es necesario primero cerrar turno de cualquier caseta antes de querer entrar a una nueva."
                return self.LKFException({'msg': msg, "title": 'Advertencia'})

            answers = {}
            answers[self.mf['guard_group']] = {'-1': employee}

            asistencia_answers = {
                self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID: {
                    self.Location.f['location']: location,
                    self.Location.f['area']: area
                },
                self.f['tipo_guardia']: 'guardia_regular',
                self.checkin_fields['checkin_type']: 'iniciar_turno',
                self.f['image_checkin']: foto_checkin
            }

            if nombre_suplente:
                asistencia_answers.update({
                    self.f['tipo_guardia']: 'guardia_suplente',
                    self.f['nombre_guardia_suplente']: nombre_suplente
                })

            registro_de_asistencia = self.do_attendance(asistencia_answers)

            data = self.lkf_api.patch_multi_record( answers = answers, form_id=self.CHECKIN_CASETAS, record_id=[record_id])
            data.update({'registro_de_asistencia': 'Correcto'})
            response.append(data)
        return response

    def do_checkout_aux_guard(self, user_id=None, checkin_id=None, location=None, area=None, guards=[], forzar=False, comments=False, fotografia=[]):
        """
        Realiza el checkout de los guardias auxiliares especificados en guards.
        """
        employee = self.get_employee_data(user_id=user_id, get_one=True)
        timezone = employee.get('cat_timezone', employee.get('timezone', 'America/Monterrey'))
        now_datetime = self.today_str(timezone, date_format='datetime')
        last_chekin = {}

        # Solo buscamos el último checkin de los guards especificados
        if not checkin_id and guards:
            last_chekin = self.get_guard_last_checkin(guards)
            checkin_id = last_chekin.get('_id')

        if not checkin_id:
            self.LKFException({
                "msg": "No encontramos un checking valido del cual podemos hacer checkout...", 
                "title": "Una Disculpa!!!"
            })

        record = self.get_record_by_id(checkin_id)
        checkin_answers = record['answers']
        folio = record['folio']

        # Realiza el checkout solo de los guards especificados
        data = self.lkf_api.get_metadata(self.CHECKIN_CASETAS)
        checkin_answers = self.check_in_out_employees('out', now_datetime, checkin=checkin_answers, employee_list=guards)
        data['answers'] = checkin_answers
        response = self.lkf_api.patch_record(data=data, record_id=checkin_id)
        if response.get('status_code') in [200, 201, 202]:
            print('entra aquiiiiiiii')
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

    def get_user_guards(self, location_employees=[]):
        location_guards = []
        for clave in ["guardia_de_apoyo", "guardia_lider"]:
            if location_employees.get(clave):
                for usuario in location_employees[clave]:
                    if usuario.get("user_id") == self.user.get('user_id'):
                        location_guards = location_employees[clave]
                
        location_employees = location_guards

        for employee in location_employees:
            if employee.get('user_id',0) == self.user.get('user_id'):
                    return employee
        return None

    def get_employee_checkin_status_by_id(self, user_id, location, area):
        """
        Obtiene el estado de checkin de un empleado
        Args:
            user_id (int): ID del usuario

        Returns:
            dict: Estado de checkin del usuario
        """

        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.CHECKIN_CASETAS,
        }

        query = [
            {'$match': match_query},
            {'$unwind': f"$answers.{self.f['guard_group']}"},
            {'$match': {
                f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.mf['id_usuario']}": {"$exists":True},
                f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.mf['id_usuario']}": {"$in": [user_id]},
            }},
            {'$addFields': {
                'priority': {
                    '$cond': [{'$eq': [f"$answers.{self.f['guard_group']}.{self.f['checkin_status']}", 'entrada']}, 1, 0]
                }
            }},
            {'$sort': {'priority': -1, 'created_at': -1}},
            {'$limit': 1},
            {'$project': {
                '_id': 1,
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
            }},
            {'$group':{
                '_id': {
                    'user_id':'$user_id',
                },
                'name': {'$last':'$name'},
                'location': {'$last':'$location'},
                'area': {'$last':'$area'},
                'checkin_date': {'$last':'$checkin_date'},
                'checkout_date': {'$last':'$checkout_date'},
                'checkin_status': {'$last':'$checkin_status'},
                'checkin_position': {'$last':'$checkin_position'},
                'folio': {'$last':'$folio'},
                'id_register': {'$last':'$_id'},
                'nombre_suplente': {'$last':'$nombre_suplente'}
            }},
            {'$project':{
                '_id': 0,
                'user_id': '$_id.user_id',
                'name': '$name',
                'location': '$location',
                'area': '$area',
                'checkin_date': '$checkin_date',
                'checkout_date': '$checkout_date',
                'checkin_status': {'$cond': [ {'$eq':['$checkin_status','entrada']},'in','out']}, 
                'checkin_position': '$checkin_position',
                'folio': '$folio',
                'id_register': '$id_register',
                'nombre_suplente': '$nombre_suplente'
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query))
        format_data = {}
        if data:
            record = self.unlist(data)
            status = 'in' if record.get('checkin_status') in ['in', 'entrada'] else 'out'
            format_data = {
                'status':status, 
                'name': record.get('name'), 
                'folio': record.get('folio'),
                '_id': str(record.get('id_register')),
                'user_id': record.get('user_id'), 
                'location':record.get('location'),
                'area':record.get('area'),
                'checkin_date':record.get('checkin_date'),
                'checkout_date':record.get('checkout_date'),
                'checkin_position':record.get('checkin_position'),
                'nombre_suplente':record.get('nombre_suplente',"")
            }
        return format_data

    def get_more_info_conscessioned_articles(self, articles=[]):
        """
        Obtiene informacion adicional de los articulos de concesion
        Args:
            articles (list): Lista de articulos
        Returns:
            list: Lista de articulos con informacion adicional
        """
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.ACTIVOS_FIJOS,
                f"answers.{self.cons_f['_nombre_equipo']}": {"$in": articles}
            }},
            {"$project": {
                "_id": 0,
                "article_name": f"$answers.{self.cons_f['_nombre_equipo']}",
                "article_image": f"$answers.{self.cons_f['_imagen_equipo_concesion']}",
                "article_cost": f"$answers.{self.cons_f['_costo_equipo_concesion']}"
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query))
        return data

    def get_anfitrion_data(self, anfitrion_id):
        query = [
            {"$match": {
                "form_id": self.USUARIOS_FORM,
                "deleted_at": {"$exists": False},
                f"answers.{self.mf['id_usuario']}": anfitrion_id
            }},
            {"$sort": {"created_at": -1}},
            {"$limit": 1},
            {"$project": {
                "_id": 0,
                "answers": 1
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query))
        format_data = []
        if data:
            format_data = self.unlist(data)
        return format_data

    def search_access_pass(self, qr_code=None, location=None):
        """
        Busca pases de acceso
        Si se entega el puro qr_code, se entrega la info de QR code
        Si se entrega el qr_code con location y area, te valida si el qr es valido para dicha area
        Si NO entregas el qr_code, te regresa todos los qr de dicha area y ubicacion
        Si no entregas nada, te regrea un warning...
        """
        last_move = {}
        if self.validate_value_id(qr_code):
            last_moves = self.get_list_last_user_move(qr_code, limit=10)
            if len(last_moves) > 0:
                last_move = last_moves[0]
            # else:
            #     self.LKFException({"msg":"No se econtro ninguan entrada con pase "+ qr_code})
            # print('last_moves=',simplejson.dumps(last_moves, indent=3))
            #last_move = self.get_last_user_move(qr_code, location)
            gafete_info = {}
            access_pass = self.get_detail_access_pass(qr_code)
            if not last_move or last_move.get('status_visita') == 'salida':
                tipo_movimiento = 'Entrada'
                access_pass['grupo_vehiculos'] = self.format_vehiculos_simple(access_pass.get('grupo_vehiculos',[]))
                access_pass['grupo_equipos'] = self.format_equipos_simple(access_pass.get('grupo_equipos',[]))
                print("entrada",access_pass['grupo_vehiculos'])
            else:
                gafete_info['gafete_id'] = last_move.get('gafete_id')
                gafete_info['locker_id'] = last_move.get('locker_id')
                access_pass['grupo_vehiculos'] = self.format_vehiculos_simple(last_move.get('vehiculos',[]))
                access_pass['grupo_equipos'] = self.format_equipos_simple(last_move.get('equipos',[]))
                tipo_movimiento = 'Salida'
                print("salida", access_pass['grupo_vehiculos'],access_pass['grupo_equipos'])
                print("last_move", simplejson.dumps(last_move, indent=4))
            #---Last Access
            access_pass['ultimo_acceso'] = last_moves
            access_pass['tipo_movimiento'] = tipo_movimiento
            access_pass['gafete_id'] = gafete_info.get('gafete_id')
            access_pass['locker_id'] = gafete_info.get("locker_id")
            access_pass['status_pase']= self.unlist(access_pass.get('estatus',"")).title() or "" 
            access_pass['limitado_a_dias']= access_pass.get('limitado_a_dias','')
            access_pass['limitado_a_acceso']= access_pass.get('limite_de_acceso','')
            access_pass['config_dia_de_acceso']=access_pass.get('config_dia_de_acceso',"").replace("_", " ")
            total_entradas = self.get_count_ingresos(qr_code)
            access_pass['total_entradas'] = total_entradas.get('total_records') if total_entradas else "0"
            access_pass['anfitrions_data'] = access_pass.get('visita_a_details', [])
            if access_pass.get('grupo_areas_acceso'):
                for area in access_pass['grupo_areas_acceso']:
                    area['status'] = self.get_area_status(access_pass['ubicacion'], area['nombre_area'])
            return access_pass
        else:
            return self.LKFException({"status_code":400, "msg":'El parametro para QR, no es valido'})

    def get_pass_custom(self,qr_code):
        pass_selected= self.get_detail_access_pass(qr_code=qr_code)
        answers={}
        for key, value in pass_selected.items():
            if key == 'nombre' or \
               key == 'email' or \
               key == 'telefono' or \
               key == 'visita_a' or \
               key == 'ubicacion' or \
               key == 'fecha_de_expedicion' or \
               key == 'fecha_de_caducidad' or \
               key == "qr_pase" or \
               key == "pdf_to_img" or \
               key == "_id" or \
               key == "estatus" or \
               key == "foto" or \
               key == "identificacion" or \
               key == "grupo_equipos" or \
               key == "grupo_vehiculos" or \
               key == "google_wallet_pass_url" or \
               key == "limite_de_acceso" or \
               key == "empresa" or \
               key == "ubicaciones_geolocation" or \
               key == "google_wallet_pass_url":
                answers[key] = value
        answers['folio']= pass_selected.get("folio")
        return answers

    def get_pdf(self, qr_code, template_id=None, name_pdf=None):
        return self.lkf_api.get_pdf_record(qr_code, template_id = template_id, name_pdf =name_pdf, send_url=True)
   
    def search_guard_asistance(self, location, area, guard):
        query = [
            {"$match": {
                "deleted_at":{"$exists":False},
                "form_id": self.REGISTRO_ASISTENCIA,
                f"answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.f['location']}": location,
                f"answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.f['area']}": area,
                f"answers.{self.f['fecha_cierre_turno']}": {"$exists": False},
                "created_by_id": guard,
            }},
            {"$sort": {"created_at": -1}},
            {"$project": {
                "_id": 1,
            }}
        ]
        resp = self.format_cr(self.cr.aggregate(query))
        format_resp = []
        if resp:
            format_resp = [r.get('_id', r.get('id', '')) for r in resp]
        return format_resp

    def checkout_all(self, record_id=None):
        """
        WORK IN PROGRESS
        """
        if not record_id:
            self.LKFException({'title': 'Error', 'msg': 'No se proporciono el record_id'})

        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.CHECKIN_CASETAS,
                "_id": ObjectId(record_id),
            }},
            {"$limit": 1},
            {"$project": {
                "_id": 0,
                "empleados_dentro": f"$answers.{self.mf['guard_group']}"
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query))
        format_data = {}
        if data:
            format_data = self.unlist(data)
            empleados_dentro = format_data.get('empleados_dentro', [])
            now_datetime = self.today_str('America/Monterrey', date_format='datetime')
            answers = {}
            format_empleados_dentro = {}
            employees_ids = []

            for index, empleado in enumerate(empleados_dentro):
                employees_ids.append(self.unlist(empleado.get('id_usuario', [])))

                if empleado.get('checkin_status') == 'entrada':
                    empleado['checkin_status'] = 'salida'
                    empleado['checkout_date'] = now_datetime
                
                item = {
                    self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID: {
                        self.mf['nombre_guardia_apoyo']: empleado.get('note_guard_close', ''),
                        self.mf['id_usuario']: empleado.get('id_usuario', [])
                    },
                    self.checkin_fields['nombre_suplente']: empleado.get('nombre_suplente', ''),
                    self.checkin_fields['checkin_position']: empleado.get('checkin_position', ''),
                    self.checkin_fields['checkin_status']: empleado.get('checkin_status', ''),
                    self.checkin_fields['checkin_date']: empleado.get('checkin_date', ''),
                    self.checkin_fields['checkout_date']: empleado.get('checkout_date', ''),
                }
                format_empleados_dentro[str(index)] = item

            answers[self.mf['guard_group']] = format_empleados_dentro
            answers[self.checkin_fields['checkin_type']] = 'cerrada'
            answers[self.checkin_fields['boot_checkout_date']] = now_datetime
            # response_checkout_all = self.lkf_api.patch_multi_record(answers=answers, form_id=self.CHECKIN_CASETAS, record_id=[record_id])
            # print('response', simplejson.dumps(response_checkout_all, indent=4))
            print('employees_ids', list(set(employees_ids)))

    def send_sms_masiv(self, para, texto):
        sms_creds = self.lkf_api.get_sms_creds(use_api_key=True, jwt_settings_key=False)
        masiv_user = sms_creds.get('json', {}).get('masiv_user', '')
        masiv_token = sms_creds.get('json', {}).get('masiv_token', '')
        API_URL = "https://api-sms.masivapp.com/send-message"

        token = base64.b64encode(f"{masiv_user}:{masiv_token}".encode()).decode()

        headers = {
            'Authorization': f'Basic {token}',
            'Content-Type': 'application/json'
        }
        
        data = {
            'to': para,
            'text': texto,
            "customdata": "CUS_ID_0125",
            "isLongmessage": True,
        }

        try:
            response = requests.post(API_URL, json=data, headers=headers)

            if response.status_code == 200:
                return response.json()
            else:
                print('Error al enviar SMS', response.status_code, response.text)
                return {
                    'statusCode': response.status_code,
                    'response': response.text
                }

        except Exception as e:
            print('Error al enviar SMS', e)
            return {
                'statusCode': 400,
                'response': str(e)
            }

    def send_email_notification(self, data, asunto_email, enviado_desde=''):
        answers = {}
        metadata = self.lkf_api.get_metadata(form_id=self.ENVIO_DE_NOTIFICACIONES_FORM)
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Addons",
                    "Process": "Creación de envio de correo",
                    "Action": "send_email_and_sms",
                }
            },
        })
        answers.update({
            f"{self.envio_correo_fields['tipo_de_notificacion']}": data['tipo'],
            f"{self.envio_correo_fields['titulo']}": asunto_email,
            f"{self.envio_correo_fields['nombre']}": data['nombre'],
            f"{self.envio_correo_fields['email_from']}": data['email_from'],
            f"{self.envio_correo_fields['email_to']}": data['email_to'],
            f"{self.envio_correo_fields['msj']}": data['mensaje'],
            f"{self.envio_correo_fields['enviado_desde']}": enviado_desde,
        })
        metadata.update({'answers': answers})
        email_response = self.lkf_api.post_forms_answers(metadata)
        return email_response

    def send_sms_notification(self, data, enviado_desde=''):
        answers = {}
        metadata = self.lkf_api.get_metadata(form_id=self.ENVIO_DE_NOTIFICACIONES_FORM)
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Addons",
                    "Process": "Creación de envio de sms",
                    "Action": "send_email_and_sms",
                }
            },
        })
        answers.update({
            f"{self.envio_correo_fields['tipo_de_notificacion']}": data['tipo'],
            f"{self.envio_correo_fields['titulo']}": 'Aviso de Acceso',
            f"{self.envio_correo_fields['nombre']}": data['nombre'],
            f"{self.envio_correo_fields['phone_to']}": data['phone_to'],
            f"{self.envio_correo_fields['msj']}": data['mensaje'],
            f"{self.envio_correo_fields['enviado_desde']}": enviado_desde,
        })
        metadata.update({'answers': answers})
        sms_response = self.lkf_api.post_forms_answers(metadata)
        return sms_response

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


###### Funciones de rondines #####

    def clean_text(self, texto):
        """
        Limpia texto: minúsculas, espacios y puntos por guiones bajos, elimina acentos
        """
        if not isinstance(texto, str):
            return ""
        
        texto = texto.lower()                # Minúsculas
        texto = texto.replace(" ", "_")      # Espacios → guiones bajos
        texto = texto.replace(".", "_")      # Puntos → guiones bajos
        
        # Eliminar acentos
        texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
        
        return texto

    def clean_db(self, status='received', batch_size=300):
        """
        Borra todos los registros con status 'received' en batches
        """
        total_deleted = 0

        while True:
            mango_query = {
                "selector": {
                    "status": status
                },
                "limit": batch_size,
                "fields": ["_id", "_rev"]
            }
            if status == 'all':
                mango_query = {
                    "selector": {
                        "_id": {"$gt":None}
                    },
                    "limit": batch_size,
                    "fields": ["_id", "_rev"]
                }
            result = self.cr_db.find(mango_query)

            docs = list(result)  # 🔥 FIX AQUÍ

            if not docs:
                print("No hay más registros para borrar")
                break

            to_delete = [
                {
                    "_id": doc["_id"],
                    "_rev": doc["_rev"],
                    "_deleted": True
                }
                for doc in docs
            ]

            response = self.cr_db.update(to_delete)

            total_deleted += len(to_delete)

            print(f"Batch eliminado: {len(to_delete)} | Total: {total_deleted}")

        return total_deleted

    def complete_rondines(self, records):
        status = {}
        answers = {}
        bad_items = []
        good_items = []
        
        if not records:
            return {'status_code': 400, 'type': 'error', 'msg': 'No records provided', 'data': {}}
        
        db_name = f'clave_{self.user_id}'
        # self.cr_db = self.get_couch_user_db(db_name)
        for item in records:
            _id = item.get('_id', None)
            _rev = item.get('_rev', None)
            
            if not _id or not _rev:
                bad_items.append(item)
                continue
            
            record = self.get_couch_record(_id=_id, _rev=_rev)
            
            if record.get('status_code') in [400, 404, 461, 462]:
                bad_items.append(item)
                continue
            
            if record.get('status_user') == 'completed':
                good_items.append(_id)
                record['inbox'] = False
                record['status'] = 'received'
                record['updated_at'] = self.today_str( date_format='datetime')
                self.cr_db.save(record)
        
        answers[self.f['estatus_del_recorrido']] = 'realizado'
        if good_items:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=good_items)
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                status = {'status_code': 200, 'type': 'success', 'msg': 'Rondines completed successfully', 'data': {}}
            else: 
                status = {'status_code': 400, 'type': 'error', 'msg': res, 'data': {}}
        if bad_items:
            status.update({'data': {'bad_items': bad_items, 'good_items': good_items}})
        return status
    
    def get_user_catalogs(self):
        soter_catalogs = [
            self.LISTA_INCIDENCIAS_CAT_ID,
            self.SUB_CATEGORIAS_INCIDENCIAS_ID,
            self.CATEGORIAS_INCIDENCIAS_ID,
            self.AREAS_DE_LAS_UBICACIONES_CAT_ID,
            self.UBICACIONES_CAT_ID,
            self.CONFIGURACION_RECORRIDOS_ID,
            self.USUARIOS_ID,
            self.CONF_AREA_EMPLEADOS_CAT_ID,
            self.TIPO_DE_EQUIPO_ID,
            self.LISTA_FALLAS_CAT_ID,
            self.CONF_AREA_EMPLEADOS_AP_CAT_ID,
            self.VISITA_AUTORIZADA_CAT_ID,
            self.ESTADO_ID,
            self.PROVEEDORES_CAT_ID,
            self.LOCKERS_CAT_ID,
            self.TIPO_ARTICULOS_PERDIDOS_CAT_ID,
            self.PASE_ENTRADA_ID,
            self.ACTIVOS_FIJOS_CAT_ID,
        ]
        dbs = {}
        try:
            fields_invertido = {v: k for k, v in self.f.items()}
            for catalog_id in soter_catalogs:
                item = {}
                version = "00.00"
                info_catalog = self.lkf_api.get_catalog_id_fields(catalog_id)
                catalog_name = self.clean_text(info_catalog.get('catalog', {}).get('name', ''))
                catalog_fields = info_catalog.get('catalog', {}).get('fields', [])
                catalog_updated_at = info_catalog.get('catalog', {}).get('updated_at', '')
                
                field_items = {}
                for field in catalog_fields:
                    if not field.get('field_type') in ['catalog']:
                        field_items.update({
                            field.get('field_id'): fields_invertido.get(field.get('field_id'), self.clean_text(field.get('label', '')))
                        })
                
                if catalog_updated_at:
                    date_part = catalog_updated_at[:10]
                    dt = datetime.strptime(date_part, '%Y-%m-%d')
                    version = f"{dt.year % 100:02d}.{dt.month:02d}"
                
                item = {
                    'db_name': f'catalog_records_{info_catalog.get("catalog", {}).get("catalog_id", 0)}',
                    'field_name': field_items,
                    'version': version,
                    'host': '',
                    'filter': ''
                }
                dbs[catalog_name] = item
        except Exception as e:
            return {'status_code': 400, 'msg': 'error', 'data': str(e)}
        return {'status_code': 200, 'msg': 'success', 'data': dbs}
    
    def get_folio_incidencia(self, record_id):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_INCIDENCIAS,
                "_id": ObjectId(record_id)
            }},
            {"$limit": 1},
            {"$sort": {"created_at": -1}},
            {"$project": {
                "_id": 0,
                "folio": "$folio"
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = self.unlist(response)
        return format_response

    def get_couch_record(self, _id=None, _rev=None):
        if not _id:
            return {'status_code': 400, 'type': 'error', 'msg': 'ID is required', 'data': {}}
        
        max_retries = 3
        wait_time = 2

        for attempt in range(max_retries):
            # self.cr_db = self.get_couch_user_db(db_name)
            record = self.cr_db.get(_id, revs_info=True)
            if not record:
                return {'status_code': 404, 'type': 'error', 'msg': 'Record not found', 'data': {}}

            current_rev = record.rev
            all_revs = [r['rev'] for r in record['_revs_info'] if r['status'] == 'available']

            if _rev == current_rev:
                attachments = record.get("_attachments", {})
                print('===> Revisión actual encontrada')
                return record
            elif _rev in all_revs:
                print(f'===> Revisión vieja, ultima revision registrada: {current_rev}')
                return {'status_code': 461, 'type': 'error', 'msg': 'Old revision found', 'data': {}}
            else:
                print(f'===> Revisión aún no propagada (Intento {attempt + 1}/{max_retries})')
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                else:
                    return {'status_code': 462, 'type': 'error', 'msg': 'Revision not yet propagated', 'data': {}}

    def upload_file_from_couchdb(self, image_data, attachment_name, id_forma_seleccionada, id_field):
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, attachment_name)

        with open(temp_file_path, 'wb') as temp_file:
            temp_file.write(image_data)

        rb_file = open(temp_file_path, 'rb')
        dir_file = {'File': rb_file}

        try:
            upload_data = {'form_id': id_forma_seleccionada, 'field_id': id_field}
            upload_url = self.lkf_api.post_upload_file(data=upload_data, up_file=dir_file)
            rb_file.close()
        except Exception as e:
            rb_file.close()
            os.remove(temp_file_path)
            print("Error al subir el archivo:", e)
            return {"error": "Fallo al subir el archivo"}

        try:
            file_url = upload_url['data']['file']
            update_file = {'file_name': attachment_name, 'file_url': file_url}
        except KeyError:
            print('No se pudo obtener la URL del archivo')
            update_file = {"error": "Fallo al obtener la URL del archivo"}
        finally:
            os.remove(temp_file_path)
        return update_file
    
    def build_area_inspection_map(self, data):
        """
        Construye:
        {
            'areas': {'nombre area': form_id},
            'inspection_ids': {
                form_id: [schema_preguntas]
            }
        }
        """
        result = {
            'areas': {},
            'inspection_ids': {}
        }
        for item in data:
            area_name = item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID,{}).get(self.f['nombre_area'])
            for _, value in item.items():
                form_id = self.unlist(value.get(self.f['form_id'] ))
                if form_id:
                    result['areas'][area_name] = form_id
                if form_id not in result['inspection_ids']:
                    result['inspection_ids'][form_id] = self.get_form_question_schema(form_id)
        return result

    def get_form_question_schema(self, form_id):
        """
        Obtiene las pregutnas del formulario. Solo regiresa las pregutnas aceptadas por el rondin
        Args:
            form_id: el id de la forma a buscar y regresar con esuquema de pregutnas 
        Regresa el esquema de preguntas de una forma de Linkaform:
        [
            {
                'pregunta': '...',
                'tipo': 'radio|checkbox|decimal|integer|text',
                'opciones': [...],
                'required': True|False
            }
        ]
        """
        res = self.lkf_api.get_form_id_fields(form_id)
        if not res:
            return []

        form_info = res[0] if isinstance(res, list) else res
        fields = form_info.get('fields', [])

        questions = []
        for field in fields:
            options = field.get('options', [])
            question_schema = {
                'pregunta': field.get('label', ''),
                'tipo': field.get('field_type',''),
                'opciones': [opt.get('label') for opt in options if opt.get('label')],
                'required': field.get('required', False)
            }
            questions.append(question_schema)

        return questions

    def assign_user_inbox(self, data):
        """
        Asigna registro a usuario
        """
        user_id_to_assign = self.unlist(data.get(self.USUARIOS_OBJ_ID, {}).get(self.mf['id_usuario'], ''))
        if not user_id_to_assign:
            self.LKFException('No se encontro id de usuario en el registro a asignar')
        db_name = f'clave_{user_id_to_assign}'
        #sete la base de datos del usuario
        self.cr_db = self.get_couch_user_db(db_name)
        record = self.cr_db.get(self.record_id)
        #test borrar esto
        # if record:
        if False:
            return {'status_code': 202, 'type': 'success', 'msg': 'Ya existe el registro', 'data': {}}

        user_name_to_assign = data.get(self.USUARIOS_OBJ_ID, {}).get(self.mf['nombre_usuario'], '')
        nombre_recorrido = data.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.mf['nombre_del_recorrido'], '')
        ubicacion_recorrido = data.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.Location.f['location'], '')
        
        recorrido_info = self.get_info_recorrido(nombre_recorrido, ubicacion_recorrido)
        print('recorrido_info',recorrido_info)
        print('data',data)
        
        #! Revisar timezone en bitacora si sera necesario
        # user_data = self.lkf_api.get_user_by_id(self.user_id)
        # user_timezone = user_data.get('timezone', 'America/Mexico_City')

        status = {}
        lat = 0.0
        long = 0.0
        if len(self.geolocation) > 1:
            lat = self.geolocation[0]
            long = self.geolocation[1]
        epoc_today = int(time.time())
        #obtiene las areas bien formateadas
        format_check_areas = self.get_area_images(data.get(self.f['areas_del_rondin'], []))
        inpections_by_area = self.build_area_inspection_map(data.get(self.f['areas_del_rondin'], []))

        for i in format_check_areas:
            form_id = inpections_by_area['areas'].get(i['area'])
            i['inspeccion'] = inpections_by_area['inspection_ids'].get(form_id, {})
            i['checked'] = False
            i['checked_at'] = ''
            i['check_area_id'] = ''

        inbox_record = {
            "_id": self.record_id,
            "type": "rondin",
            "inbox": True,
            "status": "synced",
            "folio": getattr(self, 'folio', None),
            "status_user": "new",
            "created_at": epoc_today,
            "updated_at": self.today_str( date_format='datetime'),
            "created_by_id": user_id_to_assign,
            "created_by_name": user_name_to_assign,
            "geolocation": {
                "lat": lat,
                "long": long
            },
            "record": {
                "user_name": user_name_to_assign,
                "nombre_rondin": nombre_recorrido,
                "ubicacion_rondin": ubicacion_recorrido,
                "tipo_rondin": data.get(self.f['tipo_rondin'], 'qr'),
                "duracion_estimada": recorrido_info.get('duracion_estimada', ''),
                "fecha_programada": data.get(self.f['fecha_programacion'], ''),
                "fecha_inicio": "",
                "fecha_finalizacion": "",
                "fecha_pausa": "",
                "fecha_reanudacion": "",
                "ultimo_check_area_id": "",
                "check_areas": format_check_areas,
            }
        }
        try:
            result = self.cr_db.save(inbox_record)
            if result:
                status = {'status_code': 200, 'type': 'success', 'msg': 'Inbox assigned successfully', 'data': {
                    'assigned_user_id': user_id_to_assign,
                    'assigned_user': user_name_to_assign,
                    'bitacora_record_id': self.record_id,
                    'bitacora_ubicacion': ubicacion_recorrido,
                    'bitacora_nombre_rondin': nombre_recorrido,
                    'bitacora_fecha_programada': data.get(self.f['fecha_programacion'], ''),
                }}
        except Exception as e:
            status = {'status_code': 400, 'type': 'error', 'msg': str(e), 'data': {}}
        return status
    
    def get_info_recorrido(self, nombre_recorrido, ubicacion_recorrido):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.CONFIGURACION_DE_RECORRIDOS_FORM,
                f"answers.{self.f['nombre_del_recorrido']}": nombre_recorrido,
                f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": ubicacion_recorrido
            }},
            {"$limit": 1},
            {"$project": {
                "_id": 0,
                "duracion_estimada": f"$answers.{self.f['duracion_estimada']}",
            }}
        ]
        res = self.cr.aggregate(query)
        format_res = {}
        if res:
            res = list(res)
            if res:
                format_res = self.unlist(res)
        return format_res
    
    #### Check de area >>>

    def sync_check_area_to_lkf(self, complete_record):
        """
        Sincroniza un registro de couchdb a linkaform
        """
        status = {}
        rondin_id = complete_record.get('rondin_id', '')
        record_id = complete_record.get('_id', None)
        record = complete_record.get('record', {})
        attachments_result = self.do_attachments(complete_record)
        complete_record = attachments_result['updated_record']
        if isinstance(record, dict) and 'status_code' in record:
            #Para que es esto, preguntar a Paco????
            print('neta aqui?????')
            return record
        # else:
        #     payload.update({
        #         'record_id': record_id,
        #         'rondin_id': rondin_id
        #     })
        response = self.create_check_area(complete_record)
        print('response del check de area', response)
        if response.get('status_code') in [200, 201, 202, 208,]:
            complete_record['status'] = 'received'
            #TODO delete backward compatibility
            complete_record['folio'] = response.get('json', {}).get('folio', '')
            status = {'status_code': 200, 'type': 'success', 'msg': 'Record received successfully', 'data': {}}
        else:
            if response.get('status_code') == 400:
                last_error = response.get('json',{})
            else:
                last_error = response.get('json',{}).get('error', 'sync_check_area_to_lkf: Error creating record.')

            status = {'status_code': response.get('status_code'), 'type': 'success', 'msg': last_error, 'data': {}}
            
            if response.get('status_code') == 400 and response.get('json',{}).get('code',0) == 8:
                print('TODO DESCOMENTARIAR PARA QUE SE MARQUE COMO RECIVIDO')
                complete_record['status'] = 'received'
                complete_record['last_error'] = last_error

                status = {'status_code': 208, 'type': 'success', 'msg': 'El id del registro no es único', 'data': {}}
            else:
                status = {'status_code': response.get('status_code',400), 'type': 'error', 'msg': last_error, 'data': {}}
                complete_record['status'] = 'error'
                complete_record['last_error'] = last_error
        res = self.cr_db.save(complete_record)
        print('status', status)
        return status
    
    def find_check_area_in_rondines(self, check_area_id):
        rondines = self.cr_db.find({
            "selector": {
                "type": "rondin",
                "record.check_areas": {
                    }
                },
            "limit": 1
            },
        )

        rondin = next(iter(rondines), None)
        if not rondin:
            return None

        check_areas = rondin.get('record', {}).get('check_areas', [])
        for item in check_areas:
            if item.get('check_area_id') == check_area_id:
                return {
                    'rondin_id': rondin.get('_id'),
                    'rondin': rondin,
                    'check_area': item
                }

        return None

    def process_single_check_for_rondin(self, rec):
        """
        Procesa un check_area y regresa info util para agruparla por rondin_id.
        """
        _id = rec.get('_id')
        rondin_id = rec.get('rondin_id') or rec.get('record', {}).get('rondin_id', '')

        res = self.sync_check_area_to_lkf(complete_record=rec)

        check_info = {
            "check_id": _id,
            "status_code": res.get('status_code'),
            "ok": res.get('status_code') in [200, 201, 202, 208],
            "record": rec.get('record', {}),
            "folio": rec.get('folio', ''),
            "type": rec.get('type'),
        }

        if not rondin_id:
            check_data = self.find_check_area_in_rondines(_id)
            rondin_id = check_data.get('rondin_id')
        return {
            "rondin_id": rondin_id,
            "check": check_info,
            "raw_result": res
        }

    def process_check_area_stage(self, check_records):
        """
        Procesa todos los checks en paralelo.
        Cuando terminan, los agrupa por rondin_id.
        """
        checks_by_rondin = {}
        if not check_records:
            return checks_by_rondin

        # Tetsting purposes...
        # self.process_single_check_for_rondin(check_records[0])

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(self.process_single_check_for_rondin, rec): rec
                for rec in check_records
            }

            for future in as_completed(futures):
                rec = futures[future]

                try:
                    result = future.result()
                    rondin_id = result.get('rondin_id') or 'unknown'

                    checks_by_rondin.setdefault(rondin_id, []).append(result['check'])

                    with self.results_lock:
                        self.results["result"].append(result["raw_result"])
                        if result["check"]["ok"]:
                            self.results["success"] += 1
                        else:
                            self.results["failed"] += 1
                            self.results["errors"].append({
                                "id": rec.get('_id'),
                                "error": result["raw_result"].get("msg")
                            })

                except Exception as e:
                    with self.results_lock:
                        self.results["failed"] += 1
                        self.results["errors"].append({
                            "id": rec.get('_id'),
                            "error": str(e)
                        })

        return checks_by_rondin

    #### Check de area <<<

    ### Rondines >>>

    def fix_rondines(self):
        fecha_inicio = datetime(2026, 4, 24, 0, 0, 0)
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                "version": {
                    "$gte": 3,
                },
                "created_at": {
                    "$gte": fecha_inicio,
                }
            }},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "answers": 1,
                "other_versions":1 #temporal para debug, borrar
            }}
        ]
        response = self.cr.aggregate(query)
        for r in response:
            print('record', r['_id'])
            self.fix_rondin(r)

    def fix_rondin(self, record):
        merge_areas = {}
        record_id = record['_id']
        def extract_objectid(uri):
            return uri.strip('/').split('/')[-1]
        
        def merge_area_into(merge_areas, area):
            """Merge un dict de área al acumulador, sin sobreescribir con vacíos."""
            key = area.get('incidente_area', '').strip()
            if not key:
                return
            if key not in merge_areas:
                merge_areas[key] = {}
            for field, value in area.items():
                if value not in (None, '', [], {}):
                    merge_areas[key][field] = value

        def process_version_record(ver_record, version_label):
            areas = ver_record.get('areas_del_rondin', [])
            if not areas:
                print(f"  [{version_label}] Sin áreas → skip")
                return
            print(f"  [{version_label}] {len(areas)} áreas encontradas")
            for area in areas:
                merge_area_into(merge_areas, area)

        for v in record.get('other_versions',[]):
            ver_id = extract_objectid(v['uri'])
            ver_record = self.get_version_rec(ver_id)
            if not ver_record:
                print(f"  [v{v['version']}] No encontrado en answer_version")
                continue
            process_version_record(ver_record, f"v{v['version']} id={ver_id}")

            # 2. Incluir el registro actual (form_answers) — puede tener info que no está en ninguna versión
        process_version_record(record, "actual (form_answers)")
        areas_list = []
        for area in merge_areas.values():
            areas_list.append({
                self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                    self.f['nombre_area']: area.get('incidente_area', '').strip(),
                },
                self.f['fecha_hora_inspeccion_area']: area.get('fecha_hora_inspeccion_area', ''),
                self.f['foto_evidencia_area_rondin']: area.get('foto_evidencia_area_rondin', []),
                self.f['comentario_area_rondin']: area.get('comentario_area_rondin', ''),
                self.f['url_registro_rondin']: area.get('url_registro_rondin', ''),
                self.f['duracion_traslado_area']: area.get('duracion_traslado_area', ''),
            })


        update_query = {"_id": ObjectId(record_id)}
        update_payload = {
            "$set": {
                f"answers.{self.f['areas_del_rondin']}": all_areas_sorted
            }
        }


        import json
        print("\n=== DRY RUN payload (primeras 2 áreas) ===")
        preview = {
            "$set": {
                f"answers.{self.f['areas_del_rondin']}": all_areas_sorted[:2]
            }
        }
        # print(json.dumps(preview, indent=2, default=str))

        # Descomentar cuando estés listo:
        result = self.cr.update_one(update_query, update_payload)
        print(f"\nUpdate result: matched={result.matched_count}, modified={result.modified_count}")
        return result

    def get_version_rec(self, record_id):
        self.cr_versions = self.net.get_collections(collection='answer_version')
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                "_id": ObjectId(record_id)
            }},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "answers": 1,
                "other_versions":1
            }}
        ]
        response = self.format_cr(self.cr_versions.aggregate(query))
        format_response = self.unlist(response)
        return format_response

    def get_rondin_checks(self, rondin_id):
        """
        Busca en mongodb todos los checks de area que pertenezcan a un rondin.
        Args: 
            rondin_id (ObjectId|str): ObjectId del rondin a buscar
        Return:
            checks (list): Lista de json con la info del check
        """
        rondin_url = f"https://app.linkaform.com/#/records/detail/{rondin_id}"
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.CHECK_UBICACIONES,
                f"answers.{self.f['bitacora_rondin_url']}": rondin_url
            }},
            {"$sort": {"created_at": 1}},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "answers": 1,
                "created_at": 1,
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        return {self.unlist(x.get('incidente_area', '')): x for x in response}

    def get_incidencias_from_checks(self, checks_for_rondin):
        """
        Extrae y normaliza las incidencias de una lista de checks de área.
        Args:
            checks_for_rondin (list): Lista de checks retornada por get_rondin_checks()
        Returns:
            incidencias (list): Lista de dicts normalizados con info de cada incidencia
        """
        incidencias = []

        for area_name, check in checks_for_rondin.items():
            fecha_check = check.get('created_at', '')
            grupo_incidencias = check.get('grupo_incidencias_check', [])

            if not grupo_incidencias:
                continue

            for incidencia in grupo_incidencias:
                # El catalogo de incidencias viene dentro de LISTA_INCIDENCIAS_CAT_OBJ_ID

                incidencia.update({
                    'area':               area_name,
                    'fecha_incidencia':   fecha_check,
                    })
            incidencias += grupo_incidencias
        return incidencias

    def sync_rondin_to_lkf(self, rondin_id, rondin_record={}):
        """
        Sincroniza la bitácora del rondín hacia Linkaform ya sea usando checks ya procesados. O 
        Actualizar o cerrar el rondin en linkaform.
        Args:
            rondin_id (ObjectId): Id del registro
            rondin_record (json): El registro de couchdb
        Return 
            status (json): json el la respueta del servidor
        """
        status = {}
        bitacora_in_lkf = self.get_bitacora_by_id(rondin_id)
        if not bitacora_in_lkf:
            rondin_record['status'] = 'not_found'
            rondin_record['last_error'] = 'Rondin record not found on users database.'
            self.cr_db.save(rondin_record)
            return {
                'status_code': 404,
                'type': 'error',
                'msg': f'No se encontró bitácora en LKF para rondin_id={rondin_id}',
                'data': {}
            }

        incidencia_for_rondin = []
        # Obtiene los checks que se han contestado del rondin de Mongodb
        checks_for_rondin = self.get_rondin_checks(rondin_id)
        incidencia_for_rondin = self.get_incidencias_from_checks(checks_for_rondin)
        data = rondin_record or {
            '_id': rondin_id,
            'record': {},
            'status_user': ''
        }
        bitacora_response = self.update_bitacora_with_retry(
            bitacora_in_lkf,
            data,
            incidencia_for_rondin,
            checks_for_rondin
        )
        print('bitacora_response',bitacora_response)
        # solo si existe documento rondin y se sincronizó bien, marcar received
        if rondin_record and bitacora_response.get('status_code') in [200, 201, 202]:
            try:
                rondin_record['status'] = 'received'
                rondin_record['inbox'] = False
                print('TODO........ revisar que si el registro tiene 4 areas inspeccionadoas... que se hayan subido 4 areas')
                # se debe de revisar en el rondin_record
                # self.cr_db.save(rondin_record)
            except Exception as e:
                return {
                    'status_code': 409,
                    'type': 'error',
                    'msg': f'Bitácora actualizada pero no se pudo marcar rondín como received: {e}',
                    'data': {'bitacora_response': bitacora_response}
                }

        if bitacora_response.get('status_code') in [200, 201, 202]:
            status = {'status_code': 200, 'type': 'success', 'msg': 'Rondín actualizado correctamente', 'data': {}}
        else:
            status = {'status_code': 400, 'type': 'error', 'msg': bitacora_response, 'data': {}}

        return status

    def process_rondin_stage(self, rondin_records):
        rondin_results = []

        if not rondin_records:
            return rondin_results

        if hasattr(self, 'test') and self.test:
            rondin_results = []
            for rec in rondin_records:
                rondin_id = rec.get('_id')
                rondin_results.append(self.sync_rondin_to_lkf(rondin_id, rec))
        else:

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {}

                for rec in rondin_records:
                    rondin_id = rec.get('_id')
                    futures[executor.submit(
                        self.sync_rondin_to_lkf,
                        rondin_id,
                        rec
                    )] = rec

                for future in as_completed(futures):
                    rec = futures[future]
                    try:
                        result = future.result()
                        rondin_results.append(result)

                        with self.results_lock:
                            self.results["result"].append(result)
                            if result.get("status_code") in [200, 201, 202, 208]:
                                self.results["success"] += 1
                            else:
                                self.results["failed"] += 1
                                self.results["errors"].append({
                                    "id": rec.get('_id'),
                                    "error": result.get("msg")
                                })

                    except Exception as e:
                        with self.results_lock:
                            self.results["failed"] += 1
                            self.results["errors"].append({
                                "id": rec.get('_id'),
                                "error": str(e)
                            })

        return rondin_results

    def update_rondines_from_checks(self, checks_by_rondin):
        """
        Actualiza el Rondin, segun los check que se estan sincronizando. Este caso aplica para cuando un rondin esta pausado o en progreso
        Pero ya cuenta con checks de ubicacion, el rondin se va auto rellenando.
        """
        results = []

        if not checks_by_rondin:
            return results

        #TEST puposes
        # for rondin_id, checks in checks_by_rondin.items():
        #     self.sync_rondin_to_lkf(rondin_id, checks, None)
        # print(stop)
        if hasattr(self, 'test') and self.test:
            for rondin_id, checks in checks_by_rondin.items():
                results.append(self.sync_rondin_to_lkf(rondin_id, checks))
        else:

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(self.sync_rondin_to_lkf, rondin_id, checks): rondin_id
                    for rondin_id, checks in checks_by_rondin.items()
                }

                for future in as_completed(futures):
                    rondin_id = futures[future]
                    try:
                        result = future.result()
                        results.append({
                            'rondin_id': rondin_id,
                            'result': result
                        })

                        with self.results_lock:
                            self.results["result"].append(result)
                            if result.get("status_code") in [200, 201, 202, 208]:
                                self.results["success"] += 1
                            else:
                                self.results["failed"] += 1
                                self.results["errors"].append({
                                    "id": rondin_id,
                                    "error": result.get("msg")
                                })

                    except Exception as e:
                        with self.results_lock:
                            self.results["failed"] += 1
                            self.results["errors"].append({
                                "id": rondin_id,
                                "error": str(e)
                            })

        return results
    
    ### Rondines <<<

    def get_bitacora_by_id(self, record_id):
        try:
            query = [
                {"$match": {
                    "deleted_at": {"$exists": False},
                    "form_id": self.BITACORA_RONDINES,
                    "_id": ObjectId(record_id)
                }},
                {"$project": {
                    "_id": 1,
                    "folio": 1,
                    "answers": 1,
                    "other_versions:":1 #temporal para debug, borrar
                }}
            ]
            response = self.format_cr(self.cr.aggregate(query))
            format_response = self.unlist(response)
        except:
            format_response = []

        return format_response

    def _format_fecha(self, fecha):
        fecha_str = ""
        if fecha:
            try:
                s = fecha.replace("Z", "+00:00")
                dt = datetime.fromisoformat(s)
                fecha_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                fecha_str = ""
        return fecha_str

    def format_ids_incidencias_to_bitacora(self, data):
        fecha_str = self._format_fecha(data.get('fecha_incidencia'))
        res = {
                self.Location.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID: {
                    self.f['nombre_area_salida']: data.get('area'),
                },
                self.f['fecha_hora_incidente_bitacora']: data.get('fecha_incidencia', fecha_str),
                self.LISTA_INCIDENCIAS_CAT_OBJ_ID: {
                    self.f['categoria']: data.get('categoria', ''),
                    self.f['sub_categoria']: data.get('sub_categoria', ''),
                    self.f['incidencia']: data.get('incidencia', ''),
                },
                self.f['incidente_open']: data.get('incidente_open', ''),
                self.f['comentario_incidente_bitacora']: data.get('comentario_incidente_bitacora', ''),
                self.f['incidente_accion']: data.get('incidente_accion', ''),
                self.f['incidente_evidencia']: [i for i in data.get('incidente_evidencia', []) if i.get('file_url', '')],
                self.f['incidente_documento']: [i for i in data.get('incidente_documento', []) if i.get('file_url', '')],
            }
        return res

    def format_incidencias_to_bitacora(self, bitacora_in_lkf, new_incidencias):
        """
        Formate las incidencias para injectarlas al registro de bitacora de rondines
        Si el registro de Bitacora de rondines tiene incidencias existente las toma encuenta
        Args:
            bitacora_in_lkf (json): El rondin con el cual se esta trabajando
            new_incidencias (json): Json de las incidencias entradas en los checks
        Return:
            incidencias_list (list): Lista con json en el formto de ids para dar de alta en el rondin
        """
        incidencias_list = []
        incidencias_existentes = bitacora_in_lkf.get('bitacora_rondin_incidencias', [])
        for incidencia in new_incidencias:
            fecha_str = self._format_fecha(incidencia.get('fecha_incidencia'))
            ya_existe = False
            for inc_existente in incidencias_existentes:
                if (inc_existente.get('incidencia') == incidencia.get('incidencia') and
                    inc_existente.get('categoria') == incidencia.get('categoria') and
                    inc_existente.get('nombre_area_salida') == incidencia.get('area') and
                    inc_existente.get('fecha_hora_incidente_bitacora') == fecha_str):
                        ya_existe = True
                        break
            
            if not ya_existe:
                new_item = self.format_ids_incidencias_to_bitacora(incidencia)
                incidencias_list.append(new_item)
        
        for incidencia in incidencias_existentes:
            new_item = self.format_ids_incidencias_to_bitacora(incidencia)
            incidencias_list.append(new_item)
        return incidencias_list

    def bitacora_set_area_format(self, bitacora, check):
        """
        Formatea un resgistro con leyendas a el fromato que ocupa el grupo repetitvo de la bitacora de rondines
        Args:
            bitacora (json): El rondin que se esta trabaajndo
            check (json): El check del area
        Return 
            res (json): El json con ids de cada set del grupo repetitivo del rondin
        """
        start_date = bitacora.get('fecha_inicio_rondin', [])
        ts = check.get('created_at', check.get('checked_at', check.get('fecha_hora_inspeccion_area')))
        timezone_str = check.get('timezone') or self.user.get('timezone')
        fecha_str = ""
        if ts:
            try:
                target_tz = pytz.timezone(timezone_str)
                if isinstance(ts, str):
                    dt_aware = target_tz.localize(datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"))
                else:
                    dt_aware = datetime.fromtimestamp(ts, tz=target_tz)
                fecha_str = dt_aware.strftime("%Y-%m-%d %H:%M:%S")
            except (pytz.exceptions.UnknownTimeZoneError, Exception):
                fecha_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        res = self._lables_to_ids(check)
        res ={  self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                    self.mf['nombre_area']: self.unlist(check.get('incidente_area', '')),
                    },
                self.f['fecha_inspeccion_area']: check.get('fecha_inspeccion_area', check.get('created_at')),
                self.f['foto_evidencia_area_rondin']: check.get('foto_evidencia_area', []),
                self.f['comentario_area_rondin']: check.get('comentario_check_area', check.get('comentario_area_rondin', '')),
                self.f['url_registro_rondin']: f"https://app.linkaform.com/#/records/detail/{check.get('_id')}",
                self.f['duracion_traslado_area']: self.date_difference_minutes(start_date, ts),
                }
        return res

    def create_check_area(self, data):
        """
        Registra Area, realiza check de area
        """
        # metadata = self.lkf_api.get_metadata(form_id=self.CHECK_UBICACIONES)
        record = data.get('record',{})
        answers = {}
        metadata = self.lkf_api.get_metadata(form_id=self.CHECK_UBICACIONES) #TODO: Modularizar id
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": "Accesos",
                    "Process": "Creación de check area",
                    "Action": "create_check_area",
                    "File": "accesos/app.py"
                }
            },
        })
        metadata.update({"id": data.id})
        if isinstance(data.get('geolocation'), dict):
            metadata.update({'geolocation': [data.get('geolocation').get('long'), data.get('geolocation').get('lat')]})

        metadata['start_date'] = record.get('checked_at', data.get('created_at', metadata['start_timestamp']))
        metadata['start_timestamp'] = self.get_epoch(metadata['start_date'] )
        metadata['end_timestamp'] = self.get_epoch(data.get('updated_at', metadata['end_timestamp']))
        metadata['timezone'] = data.get('timezone') or  metadata.get('timezone') or self.user.get('timezone')
        print('revisar que ponga la fecha de inspeccion')
        if data.get('rondin_id'):
            rondin_id = data.get('rondin_id')
            answers[self.f['bitacora_rondin_url']] = f"https://app.linkaform.com/#/records/detail/{rondin_id}"

        if data.get('rondin_name'):
            rondin_name = data.get('rondin_name')
            answers[self.CONFIGURACION_RECORRIDOS_OBJ_ID] = {
                self.mf['nombre_del_recorrido']: rondin_name
            }
        #---Define Answers
        answers[self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID]={}
        answers[self.f['check_status']] = "continuar_siguiente_punto_de_inspección"
        answers[self.f['fecha_inspeccion_area']] = record.get('checked_at', data.get('created_at', metadata['start_timestamp']))
        for key, value in record.items():
            if key == 'tag_id':
                answers[self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID].update({
                    self.f['area_tag_id']: value,
                    self.Location.f['location']: [record.get('ubicacion', '')],
                    self.Location.f['area']: [record.get('area', '')],
                    self.f['tipo_de_area']: [record.get('tipo_de_area', '')],
                    self.f['area_foto']: [record.get('foto_del_area', '')],
                })
            elif key == 'evidencia_incidencia':
                answers[self.f['foto_evidencia_area']] = value
            elif key == 'documento_incidencia':
                answers[self.f['documento_check']] = value
            elif key == 'incidencias':
                incidencias = record.get('incidencias', [])
                if incidencias:
                    incidencias_list = []
                    for incidencia in incidencias:
                        print('revisxart linea 1285 comentario_incidente_bitacora que llave trae incidencia')
                        item = {}
                        if incidencia.get('categoria'):
                            item = {self.LISTA_INCIDENCIAS_CAT_OBJ_ID: {
                                self.f['categoria']: incidencia.get('categoria', ''),
                                self.f['sub_categoria']: incidencia.get('sub_categoria', ''),
                                self.f['incidencia']: incidencia.get('incidencia', ''),
                            }}
                        item.update({
                            self.f['incidente_open']: incidencia.get('incidente_open', ''),
                            self.f['comentario_incidente_bitacora']: incidencia.get('comentario', ''),
                            self.f['incidente_accion']: incidencia.get('accion', ''),
                            self.f['incidente_evidencia']: incidencia.get('evidencia', ''),
                            self.f['incidente_documento']: incidencia.get('documento', ''),
                        })
                        incidencias_list.append(item)
                    answers[self.f['grupo_incidencias_check']] = incidencias_list
            elif key == 'comentario_check_area':
                answers[self.f['comentario_check_area']] = value
            elif key == 'status_check_area':
                answers[self.f['check_status']] = value
            else:
                continue
            
        
        metadata.update({'answers':answers})
        res = self.lkf_api.post_forms_answers(metadata)
        return res
    
    def delete_rondines(self, records):
        status = {}
        answers = {}
        bad_items = []
        good_items = []
        
        if not records:
            return {'status_code': 400, 'type': 'error', 'msg': 'No records provided', 'data': {}}
        
        db_name = f'clave_{self.user_id}'
        # self.cr_db = self.get_couch_user_db(db_name)
        for item in records:
            _id = item.get('_id', None)
            _rev = item.get('_rev', None)
            
            if not _id or not _rev:
                bad_items.append(item)
                continue
            
            record = self.get_couch_record(_id=_id, _rev=_rev)
            
            if record.get('status_code') in [400, 404, 461, 462]:
                bad_items.append(item)
                continue
            
            good_items.append(_id)
            self.cr_db.delete(record)
        
        answers[self.f['estatus_del_recorrido']] = 'cancelado'
        if good_items:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=good_items)
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                status = {'status_code': 200, 'type': 'success', 'msg': 'Rondines deleted successfully', 'data': {}}
            else: 
                status = {'status_code': 400, 'type': 'error', 'msg': res, 'data': {}}
        if bad_items:
            status.update({'data': {'bad_items': bad_items, 'good_items': good_items}})
        return status
  
    def get_user_data(self, user_id):
        query = [{"$match": {
            "deleted_at": {"$exists": False},
            "form_id": 129958, #TODO: Modularizar id
            f"answers.{self.mf['id_usuario']}": user_id
        }},
        {"$limit": 1},
        {"$sort": {"created_at": -1}},
        {"$project": {
            "_id": 0,
            "id": f"$answers.{self.mf['id_usuario']}",
            "name": f"$answers.{self.mf['nombre_usuario']}",
            "email": f"$answers.{self.mf['email_visita_a']}",
        }}]
        reponse = self.format_cr(self.cr.aggregate(query))
        format_response = self.unlist(reponse)
        return format_response
    
    def reasignar_rondines(self, records, user_to_assign):
        status = {}
        answers = {}
        bad_items = []
        good_items = []
        
        user_id = user_to_assign.get('id', 0)
        name = user_to_assign.get('name', '')
        user_data = self.get_user_data(user_id=user_id)
        email = user_data.get('email', '')
        
        if not records:
            return {'status_code': 400, 'type': 'error', 'msg': 'No records provided', 'data': {}}
        
        db_name = f'clave_{self.user_id}'
        # self.cr_db = self.get_couch_user_db(db_name)
        for item in records:
            _id = item.get('_id', None)
            _rev = item.get('_rev', None)
            
            if not _id or not _rev:
                bad_items.append(item)
                continue
            
            record = self.get_couch_record(_id=_id, _rev=_rev)
            
            if record.get('status_code') in [400, 404, 461, 462]:
                bad_items.append(item)
                continue
            
            if record:
                good_items.append(_id)
                record['inbox'] = False
                record['status_user'] = 'deleted'
                record['status'] = 'received'
                self.cr_db.save(record)
        
        answers[self.USUARIOS_OBJ_ID] = {
            self.mf['nombre_usuario']: name,
            self.mf['id_usuario']: [user_id],
            self.mf['email_visita_a']: [email],
        }
        if good_items:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=good_items)
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                status = {'status_code': 200, 'type': 'success', 'msg': 'Rondines assigned successfully', 'data': {}}
            else: 
                status = {'status_code': 400, 'type': 'error', 'msg': res, 'data': {}}
        if bad_items:
            status.update({'data': {'bad_items': bad_items, 'good_items': good_items}})
        return status

    def get_active_guards(self):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.REGISTRO_ASISTENCIA,
                f"answers.{self.f['fecha_inicio_turno']}": {"$exists": True},
                f"answers.{self.f['fecha_cierre_turno']}": {"$exists": False},
            }},
            {"$project": {
                "_id": 0,
                "created_by_id": 1,
                "created_by_name": 1,
                "created_by_email": 1
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = []
        if response:
            for item in response:
                new_item = {
                    'guard_id': item.get('created_by_id', 0),
                    'guard_name': item.get('created_by_name', ''),
                    'guard_email': item.get('created_by_email', ''),
                }
                format_response.append(new_item)
        response = {'status_code': 200, 'type': 'success', 'msg': 'Active guards retrieved successfully', 'data': format_response}
        return response
    
    ### revisar si esto no esta repitdio >>>
    def _process_single_check_record(self, record):
        record_id = record.get('record_id', None)

        # Filter file lists to ensure file_url exists
        file_keys = ['foto_del_area', 'evidencia_incidencia', 'documento_incidencia']
        for key in file_keys:
            if key in record and isinstance(record[key], list):
                record[key] = [
                    item for item in record[key] 
                    if item.get('file_url')
                ]

        if 'incidencias' in record and isinstance(record['incidencias'], list):
            for incidencia in record['incidencias']:
                incidencia_file_keys = ['evidencia', 'documento']
                for key in incidencia_file_keys:
                    if key in incidencia and isinstance(incidencia[key], list):
                        incidencia[key] = [
                            item for item in incidencia[key] 
                            if item.get('file_url')
                        ]

        response = {}
        try:
            response = self.create_check_area(record)
        except Exception as e:
            self.LKFException({'title': 'Error inesperado', 'msg': str(e)})
        
        if record_id:
            record = self.cr_db.get(record_id)
            if record:
                if response.get('status_code') in [200, 201, 202]:
                    record['status'] = 'received'
                    record['folio'] = response.get('json', {}).get('folio', '')
                    self.cr_db.save(record)
                else:
                    record['status'] = 'error'
                    record['last_error'] = response['json']['error']
                    self.cr_db.save(record)

    def create_checks_in_lkf(self, records):
        with ThreadPoolExecutor(max_workers=15) as executor:
            executor.map(self._process_single_check_record, records)

    ### revisar si esto no esta repitdio <<<

    def _process_attachment_upload(self, check_id, name, existing_urls, field_id=None):
        # Check if already uploaded
        """
        revisa si y ya existe la url, si no sube la foto a linkaform
        """
        current_url = existing_urls.get(name, '')
        if current_url and current_url.startswith('http'):
            return None

        attachment = self.cr_db.get_attachment(check_id, name)
        data = attachment.read()
        field_id = None
        if not field_id:
            if name.endswith('.png') or name.endswith('.jpg') or name.endswith('.jpeg'):
                field_id = self.f['foto_evidencia_area']
            else:
                field_id = self.f['documento_check']
        upload_image = self.upload_file_from_couchdb(data, name, self.CHECK_UBICACIONES, field_id)
        return upload_image
    
    def validate_areas_completadas(self, areas_completadas, areas_formateadas, data):
        """
        Actualiza data['record']['check_areas'] con el status correspondiente.

        Valida que cada area completada en CouchDB (móvil) esté correctamente
        reflejada en las areas formateadas para LKF.
        Si en el rondin de couchdb, esta como checkada y no esta en linkaform, la marca como not found
        Si esta en linkaform y en el rondin de couchdb, actualiza con el status de lkf
        Si esta nueva, pausada, o en progreso en el rondin... No espera q este en linkaform

        Args:
            areas_completadas (list): check_areas con status_check=completed de CouchDB
            areas_formateadas (list): answers[self.f['areas_del_rondin']] ya construido
            data (dict): documento raíz de CouchDB (se modifica in-place)
        Returns:
            data (dict): documento actualizado con status en cada check_area
        """
        # Construir set de nombres que sí quedaron en LKF
        # incidente_area puede ser list o str
        nombres_en_lkf = set()
        for area in areas_formateadas:
            nombre = area.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['nombre_area'])
            nombre = self.unlist(nombre)
            was_checked = area.get(self.f['fecha_inspeccion_area'])
            if nombre and was_checked:
                nombres_en_lkf.add(nombre)

        # Actualizar status en cada check_area de CouchDB
        check_area_ids = [x['check_area_id'] for x in data['record'].get('check_areas', []) if x['check_area_id']]
        check_areas_status = self.get_check_status(check_area_ids)
        for check_area in data['record'].get('check_areas', []):
            status_user = check_area.get('status_user',check_area.get('status_check'))
            check_area_id = check_area.get('check_area_id')
            nombre = check_area.get('area', '')

            if status_user == 'completed' and check_area_id not in check_area_id:
                check_area['status'] = 'not_found'
            elif check_area_id in list(check_areas_status.keys()):
                check_area['status'] = check_areas_status[check_area_id]
        
        return data

    def get_check_status(self, check_id_list):
        records_rondin = self.cr_db.find({
                    "selector": {
                         "_id": {"$in": check_id_list}
                    },
                        "fields": ["_id","status"]
                })
        return {x['_id']:x.get('status') for x in records_rondin }

    def update_bitacora(self, bitacora_in_lkf, data, incidencia_for_rondin, checks_for_rondin):
        """
        Actualiza la bitacora de Rondines
        Args:
            incidencia_for_rondin: Incidencias  de Areas en Linkaform (mongodb)
            checks_for_rondin: Check de Areas en Linkaform (mongodb)
        """
        answers={}
        res = {}
        areas_list = []
        conf_recorrido = {}
        estatus_bitacora_in_couch = data.get('status_user', '')
        incidencias_list = self.format_incidencias_to_bitacora(bitacora_in_lkf, incidencia_for_rondin)
        answers[self.f['bitacora_rondin_incidencias']] = incidencias_list
        
        # Va a iterar las areas acutales si existe el nombre de la area en el rondin
        # quiere decir que ya previamente se habia inspeccionado, se actualzia el area
        # y se quita de las areas nuevas
        print('updating bitacora')
        print('hay que revisar con gerardo como le hacemos si hay un error en una area')
        print('de donde toma el la info... del check del area o del check del rondin')
        print('cual va a ser el single source of true')

        bitacora_in_lkf['areas_del_rondin'] = bitacora_in_lkf.get('areas_del_rondin',[])
        
        for item in bitacora_in_lkf['areas_del_rondin']:
            nombre_area = item.get('incidente_area')
            if checks_for_rondin.get(nombre_area):
                item.update(checks_for_rondin.pop(nombre_area))

        areas_completadas = [
                area for area in data['record'].get('check_areas', [])
                if area.get('status_check') == 'completed'
            ] 
       # Despues de quitar las areas existentes, agrega las areas nuevas
        for nombre_area, new_item in checks_for_rondin.items():
            bitacora_in_lkf['areas_del_rondin'].append(new_item)

        # Acomoda los checks (sets del grupo) por orden cronologico
        bitacora_in_lkf['areas_del_rondin'] = sorted(
            bitacora_in_lkf['areas_del_rondin'],
            key=lambda x: x.get('fecha_hora_inspeccion_area') or 'zzzz'
        )


        for key, value in bitacora_in_lkf.items():
            if key == 'new_user_complete_name':
                answers[self.USUARIOS_OBJ_ID] = {
                    self.f['new_user_complete_name']: value,
                    self.f['new_user_id']: [self.user['user_id']],
                    self.f['new_user_email']: [self.user['email']]
                }
            elif key == 'fecha_programacion':
                answers[self.f['fecha_programacion']] = value
            elif key == 'fecha_inicio_rondin':
                answers[self.f['fecha_inicio_rondin']] = value
            elif key == 'fecha_fin_rondin':
                answers[self.f['fecha_fin_rondin']] = value
            elif key == 'estatus_del_recorrido' and value:
                answers[self.f['estatus_del_recorrido']] = value
            elif key == 'incidente_location':
                conf_recorrido.update({
                    self.f['ubicacion_recorrido']: value
                })
            elif key == 'nombre_del_recorrido':
                conf_recorrido.update({
                    self.f['nombre_del_recorrido']: value
                })
            elif key == 'estatus_del_recorrido':
                answers[self.f['estatus_del_recorrido']] = value
            elif key == 'areas_del_rondin':
                answers[self.f['areas_del_rondin']] = [ self.bitacora_set_area_format(bitacora_in_lkf, check) for check in value ]
                # new_item = self.bitacora_set_area_format(bitacora_in_lkf, check)
                # for item in value:
        

        # Validar contra lo que el móvil reporta como completado
        data = self.validate_areas_completadas(
            areas_completadas=areas_completadas,
            areas_formateadas=answers[self.f['areas_del_rondin']],
            data=data
        )
        print('data rondin_id', data.get('_id'))
        if estatus_bitacora_in_couch == 'in_progress': 
            answers[self.f['estatus_del_recorrido']] = 'en_proceso' 
        elif estatus_bitacora_in_couch == 'completed': 
            answers[self.f['estatus_del_recorrido']] = 'realizado' 
        elif estatus_bitacora_in_couch == 'cancel': 
            answers[self.f['estatus_del_recorrido']] = 'cancelado' 
        else: 
            answers[self.f['estatus_del_recorrido']] = 'realizado'

        answers[self.CONFIGURACION_RECORRIDOS_OBJ_ID] = conf_recorrido
        if not answers.get(self.f['fecha_inicio_rondin']):
            answers[self.f['fecha_inicio_rondin']] = data.get('record', {}).get('fecha_inicio', '')

        comentarios_in_couch = data.get('record', {}).get('comentarios_rondin', [])
        comentarios_in_lkf = bitacora_in_lkf.get('grupo_comentarios_generales', [])
        comentarios_existentes = set()
        comentarios_finales = []
        
        for comentario in comentarios_in_lkf:
            fecha = comentario.get('grupo_comentarios_generales_fecha', '')
            texto = comentario.get('grupo_comentarios_generales_texto', '')
            comentarios_existentes.add((fecha, texto))
        
        for comentario in comentarios_in_lkf:
            nuevo_comentario = {
            self.f['grupo_comentarios_generales_fecha']: comentario.get('grupo_comentarios_generales_fecha', ''),
            self.f['grupo_comentarios_generales_texto']: comentario.get('grupo_comentarios_generales_texto', '')
            }
            comentarios_finales.append(nuevo_comentario)
        
        for comentario in comentarios_in_couch:
            fecha = comentario.get('fecha', '')
            texto = comentario.get('texto', '')
            
            if (fecha, texto) not in comentarios_existentes:
                nuevo_comentario = {
                    self.f['grupo_comentarios_generales_fecha']: fecha,
                    self.f['grupo_comentarios_generales_texto']: texto
                }
                comentarios_finales.append(nuevo_comentario)
        
        answers[self.f['grupo_comentarios_generales']] = comentarios_finales
        if answers:
            metadata = self.lkf_api.get_metadata(form_id=self.BITACORA_RONDINES)
            metadata.update(self.get_record_by_folio(bitacora_in_lkf.get('folio'), self.BITACORA_RONDINES, select_columns={'_id': 1}, limit=1))

            metadata.update({
                'properties': {
                    "device_properties": {
                        "system": "Addons",
                        "process":"Actualizacion de Bitacora", 
                        "accion":'rondines_cache', 
                        "folio": bitacora_in_lkf.get('folio'), 
                        "archive": "rondines_cache.py"
                    }
                },
                'answers': answers,
                '_id': bitacora_in_lkf.get('_id')
            })
            res = self.net.patch_forms_answers(metadata)
            print('res',res)
            if res.get('status_code') == 202:
                data['status'] = 'received'
                data['inbox'] = False
                self.cr_db.save(data)
        return res
        
    def update_bitacora_with_retry(self, bitacora_in_lkf, data, incidencia_for_rondin, checks_for_rondin, max_retries=5, base_wait=2):
        """
        Reintenta update_bitacora en caso de error 208 (registro ocupado).
        - base_wait: espera inicial en segundos antes del primer intento
        - Backoff exponencial + jitter en cada reintento
        """
        for attempt in range(max_retries):
            # Espera antes de cada intento (incluyendo el primero)
            wait = base_wait * (2 ** attempt) + random.uniform(0, 1)
            wait = .1 
            print('cambiar TODO BORRAR EL WAIT.1')
            print(f'Esperando {wait:.1f}s antes del intento {attempt + 1}/{max_retries}...')
            time.sleep(wait)

            response = self.update_bitacora(bitacora_in_lkf, data, incidencia_for_rondin, checks_for_rondin)

            if response.get('status_code') == 208:
                print(f'Registro ocupado (208), reintentando...')
                continue

            # Cualquier otra respuesta (éxito o error diferente) se retorna directo
            return response

        return {'status_code': 408, 'type': 'error', 'msg': 'Max retries exceeded after 208 conflicts', 'data': {}}
    
    def sync_incidence_to_lkf(self, record):
        status = {}
        record_id = record.pop('_id', None)
        record = record.get('record', {})
        folio = self.get_folio_incidencia(record_id)
        payload = {k: record[k] for k in self.incidence_filter.keys() if k in record}
            
        if isinstance(record, dict) and 'status_code' in record:
            return record
        elif isinstance(record, dict) and folio:
            folio = folio.get('folio', '')
            response = self.update_incidence(payload, folio)
        else:
            payload.update({'record_id': record_id})
            response = self.create_incidence(payload)

        record = self.cr_db.get(record_id)
        if response.get('status_code') in [200, 201, 202]:
            record['status'] = 'synced'
            record['updated_at'] = self.today_str( date_format='datetime')
            self.cr_db.save(record)
            status = {'status_code': 200, 'type': 'success', 'msg': 'Record synced successfully', 'data': {}}
        else:
            record['last_error'] = record.get('json',{}).get('error','Error 1250 al sincronizar incidencia')
            record['status'] = 'error'
            record['updated_at'] = self.today_str( date_format='datetime')
            self.cr_db.save(record)
            status = {'status_code': 400, 'type': 'error', 'msg': response, 'data': {}}
        return status

    def _handle_result(self, res, _id):
        """Registra el resultado de una operación y retorna True/False."""
        if res.get('status_code') not in (200, 201, 202, 208):
            with self.results_lock:
                self.results["failed"] += 1
                self.results["errors"].append({"id": _id, "error": res.get('msg')})
            return False
        with self.results_lock:
            self.results["success"] += 1
        return True

    def is_valid_url(self, value):
        return isinstance(value, str) and value.startswith('http')

    def get_extension(self, filename):
        return os.path.splitext(filename or '')[1].lower()

    def infer_field_id(self, filename):
        ext = self.get_extension(filename)
        if ext in self.IMAGE_EXTENSIONS:
            return self.f['foto_evidencia_area']
        return self.f['documento_check']

    def find_pending_media_nodes(self, node, found=None, path='record'):
        """
        Recorre recursivamente dicts/lists y encuentra diccionarios que:
        - tengan file_path
        - no tengan file_url válido
        Regresa una lista con referencias al nodo original.
        """
        if found is None:
            found = []

        if isinstance(node, dict):
            has_file_path = bool(node.get('file_path'))
            has_valid_url = self.is_valid_url(node.get('file_url', ''))

            if has_file_path and not has_valid_url:
                found.append({
                    'node': node,
                    'path': path
                })

            for key, value in node.items():
                self.find_pending_media_nodes(value, found, f'{path}.{key}')

        elif isinstance(node, list):
            for idx, item in enumerate(node):
                self.find_pending_media_nodes(item, found, f'{path}[{idx}]')
        return found

    def _process_attachment_upload_universal(self, doc_id, media_node):
        """
        media_node es el diccionario original dentro de record.
        Si encuentra attachment en CouchDB, lo sube a LKF y actualiza el mismo dict.
        """
        attachment_name = media_node.get('name') or media_node.get('file_name')
        file_url = media_node.get('file_url', '')
        file_path = media_node.get('file_path', '')

        if not attachment_name:
            return {
                'success': False,
                'error': 'No se encontró name ni file_name',
                'node': media_node
            }

        if self.is_valid_url(file_url):
            return {
                'success': True,
                'skipped': True,
                'reason': 'Ya tenía file_url',
                'node': media_node
            }

        # try:
        if True:
            attachment = self.cr_db.get_attachment(doc_id, attachment_name)
            if not attachment:
                return {
                    'success': False,
                    'error': f'No se encontró attachment en CouchDB: {attachment_name}',
                    'node': media_node
                }

            data = attachment.read()
            field_id = self.infer_field_id(attachment_name)

            upload_result = self.upload_file_from_couchdb(
                data,
                attachment_name,
                self.CHECK_UBICACIONES,
                field_id
            )

            if upload_result.get('error'):
                return {
                    'success': False,
                    'error': upload_result['error'],
                    'node': media_node
                }

            # Actualiza el mismo nodo dentro de record
            media_node['file_name'] = upload_result.get('file_name', attachment_name)
            media_node['file_url'] = upload_result.get('file_url', '')
            return {
                'success': True,
                'file_name': media_node['file_name'],
                'file_url': media_node['file_url'],
                'file_path': file_path,
                'node': media_node
            }

        # except Exception as e:
        #     return {
        #         'success': False,
        #         'error': str(e),
        #         'node': media_node
        #     }

    def do_attachments(self, record):
        """
        Sube todos los attachments que se le pasen a Linkaform utilizando hilos
        """
        media = []
        pending_nodes = self.find_pending_media_nodes(record.get('record'))
        if not pending_nodes:
            return {
                'updated_record': record,
                'uploaded': [],
                'errors': [],
                'total_found': 0
            }
        uploaded = []
        errors = []

        max_workers = 30
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._process_attachment_upload_universal, record['_id'], item['node'])
                for item in pending_nodes
            ]

            for future in as_completed(futures):
                result = future.result()
                if result.get('success'):
                    if not result.get('skipped'):
                        uploaded.append(result)
                else:
                    errors.append(result)

        print('pending_nodes', pending_nodes)
        return {
            'updated_record': record,
            'uploaded': uploaded,
            'errors': errors,
            'total_found': len(pending_nodes)
        }

    def get_area_model(self, record):
        """
        Traduce un record de CouchDB al formato answers de LKF.
        record['record'] contiene los datos del área capturada en la app.
        """
        attachments_result = self.do_attachments(record)
        if attachments_result.get('updated_record'):
            record = attachments_result['updated_record']

        data = record.get('record', {})
        rec_id = record['_id']

        nombre_area =  data.get('incidente_area')
        area_catalogo =  data.get('area_catalogo')
        if not nombre_area and not area_catalogo:
            return {"error":"Nombre de Area Requerido"}

        answers = {}
        # Catálogo de ubicación 
        answers[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID] = {
            self.f['location'] : data.get('incidente_location', ''),
        }
        if area_catalogo:
            answers[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID].update({self.f['nombre_area'] : area_catalogo})

        # Nombre de Area
        answers[self.configuracion_area['nombre_nueva_area']] = nombre_area

        # Catalogo tipo de area
        answers[self.TIPO_AREA_OBJ_ID] = {
            self.f['tipo_de_area'] : data.get('tipo_area', ''),
            }

        existing_urls = {} #obtener las existntes

        #foto del area
        answers[self.configuracion_area['foto_area']] = data.get('area_foto',[])

        #TagId
        answers[self.configuracion_area['tag_id']] = data.get('area_tag_id', '')

        # Comentario del área
        answers[self.configuracion_area['comentarios']] = data.get('comentario', '')

        return answers

    def config_area(self, record):
        if not record:
            return {'status_code': 400, 'type': 'error', 'msg': 'No record provided', 'data': {}}

        _id = record.id
        _rev = record.rev

        if not _id or not _rev:
            return {'status_code': 400, 'type': 'error', 'msg': 'Missing _id or _rev', 'data': {}}

        answers = self.get_area_model(record)
        

        metadata = self.lkf_api.get_metadata(form_id=self.CONFIGURACION_AREA_FORM)
        metadata.update({'answers': answers})

        res = self.lkf_api.post_forms_answers(metadata)

        if res.get('status_code') in (200, 201, 202):
            self.cr_db.delete(record)
            res = {'status_code': 200, 'type': 'success', 'msg': 'Area synced', 'data': {}}
        else:
            record['status'] = 'error'
            if res['status_code'] == 400:
                last_error = res.get('json',{})
            else:
                last_error = res.get('json',{}).get('error','Error al crear la configuracon del area')
            if isinstance(last_error, dict):
                record['status'] = 'error'
                last_error = last_error.get('exception', last_error)

            res = {'status_code': 400, 'type': 'error', 'msg': last_error, 'data': {}}
            record['updated_at'] = self.today_str( date_format='datetime')
            record['last_error'] = last_error
            self.cr_db.save(record)

        return res

    def group_records_by_type(self, records):
        """
        Regresa un diccionario con los registros agrupados por type.
        Ej:
        {
            "rondin": [rec1, rec2],
            "check_area": [rec3],
            "area": [rec4, rec5],
            "unknown": [rec6]
        }
        """
        grouped = {}

        for rec in records:
            r_type = rec.get('type') or 'unknown'
            if r_type not in grouped:
                grouped[r_type] = []
            grouped[r_type].append(rec)

        return grouped

    def process_stage_in_parallel(self, records, handler, max_workers=10):
        """
        Procesa en paralelo los tipos de registros segun su tipo 
        """
        stage_results = []

        if not records:
            return stage_results

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(handler, rec): rec
                for rec in records
            }

            for future in as_completed(futures):
                rec = futures[future]
                try:
                    result = future.result()
                    stage_results.append(result)

                    with self.results_lock:
                        self.results["result"].append(result)
                        if result.get("status_code") in [200, 201, 202, 208]:
                            self.results["success"] += 1
                        else:
                            self.results["failed"] += 1
                            self.results["errors"].append({
                                "id": rec.get('_id'),
                                "error": result.get("msg")
                            })

                except Exception as e:
                    with self.results_lock:
                        self.results["failed"] += 1
                        self.results["errors"].append({
                            "id": rec.get('_id'),
                            "error": str(e)
                        })

        return stage_results

    def sync_records(self, app_records=[]):
        """
        Obtiene todos los registros de cocuhdb que esten con "status_user": "completed" y los 
        procesa segun sea el tipo.
        """
        record_list = []
        records = self.cr_db.find({
            "selector": {
                "status_user": "completed",
                "status": "synced",
            },
            "limit": 1000
        })
        record_list += list(records)

        #TODO DELETE una vez que ya se hayan migrado todas las apps
        # backward compatibility: checks viejos
        records_check = self.cr_db.find({
            "selector": {
                "$or": [
                    {"status_check": "completed"},
                    {"status_check": "in_progress"}, #bug hay que quitar esto.. 
                ],
                 "$or": [
                        {"status": {"$exists": False}},
                        {"status": "synced"},
                        # {"status": "error"}
                    ],

            },
            # "limit": 1000
            "limit": 1000
        })
        record_list += list(records_check)

        #TODO DELETE una vez que ya se hayan migrado todas las apps
        # backward compatibility: checks viejos
        records_rondin = self.cr_db.find({
            "selector": {
                 "status_rondin": "completed",
                "$or": [
                        {"status": {"$exists": False}},
                        {"status": "synced"}
                    ],
            },
            "limit": 1000
        })
        record_list += list(records_rondin)
        # quitar duplicados
        unique_records = {}
        for rec in record_list:
            unique_records[rec.get('_id')] = rec
        record_list = list(unique_records.values())

        if not record_list:
            print("No records to sync")
            return

        self.results = {
            "success": 0,
            "failed": 0,
            "errors": [],
            "result": []
        }
        self.results_lock = threading.Lock()

        grouped_records = self.group_records_by_type(record_list)
        area_results = self.process_stage_in_parallel(
            grouped_records.get('area', []),
            self.process_area_record,
            max_workers=10
        )

        #todo
        if  grouped_records.get('incidencia'):
            response = acceso_obj.sync_incidence_to_lkf(record=record)

        # 1. primero checks
        print('va a hacer los check de area')
        checks_by_rondin = self.process_check_area_stage(
            grouped_records.get('check_area', [])
        )
        print('ya hizo los checks')
        # checks_by_rondin = independent_results.get('check_area')
        # 2. luego rondines explícitos
        print('rondines/....' )
        rondin_results = self.process_rondin_stage(
            grouped_records.get('rondin', [])
        )

        # 3. lo que sobró en checks_by_rondin son rondines que no venían como registro
        missing_rondin_results = self.update_rondines_from_checks(
            checks_by_rondin
        )

        return {
            "results": self.results,
            "rondin_results": rondin_results,
            "missing_rondin_results": missing_rondin_results,
        }

    def process_area_record(self, rec):
        return self.config_area(rec)    

    def process_checks(self, checks_details, rondin_id, rondin_name):
        #! Se crean los payloads para crear los checks en Linkaform
        payloads = []
        for i in checks_details:
            check_evidencias = i.get('record', {}).get('evidencia_incidencia', [])
            check_documentos = i.get('record', {}).get('documento_incidencia', [])
            check_incidencias = i.get('record', {}).get('incidencias', [])

            # Build a map of file_name -> file_url to check existing URLs
            existing_urls = {}
            for item in check_evidencias + check_documentos:
                existing_urls[item.get('file_name')] = item.get('file_url', '')
            for inc in check_incidencias:
                for item in inc.get('evidencia', []) + inc.get('documento', []):
                    existing_urls[item.get('file_name')] = item.get('file_url', '')

            attachments = i.get('_attachments', {})
            media = self.do_attachments(attachments)
            if attachments:
                for m in media:
                    m_name = m.get('file_name')
                    m_url = m.get('file_url')
                    
                    for item in check_evidencias:
                        if item.get('file_name') == m_name:
                            item['file_url'] = m_url
                            
                    for item in check_documentos:
                        if item.get('file_name') == m_name:
                            item['file_url'] = m_url
                            
                    for incidencia in check_incidencias:
                        for item in incidencia.get('evidencia', []):
                            if item.get('file_name') == m_name:
                                item['file_url'] = m_url
                        for item in incidencia.get('documento', []):
                            if item.get('file_name') == m_name:
                                item['file_url'] = m_url

                i['updated_at'] = self.today_str( date_format='datetime')
                self.cr_db.save(i)

            record = i.get('record', {})
            payload = {k: record[k] for k in self.check_area_filter.keys() if k in record}
            payload.update({
                'record_id': i.get('_id'),
                'rondin_id': rondin_id,
                'rondin_name': rondin_name
            })
            payloads.append(payload)
        return payloads
        
