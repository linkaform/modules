# -*- coding: utf-8 -*-
import pytz
import sys, simplejson, json, pytz, base64, requests

from datetime import datetime, timedelta, time, date
from math import ceil
from bson import ObjectId
#borrar cuando nos llevemos update_pass
from copy import deepcopy


from linkaform_api import base, generar_qr
from lkf_addons.addons.accesos.app import Accesos


class Accesos( Accesos):
    print('Entra a acceos utils')

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.f.update({
            'duracion_rondin':'6639b47565d8e5c06fe97cf3',
            'duracion_traslado_area':'6760a9581e31b10a38a22f1f',
            'fecha_inspeccion_area':'6760a908a43b1b0e41abad6b',
            'fecha_inicio_rondin':'6760a8e68cef14ecd7f8b6fe',
            'status_rondin':'6639b2744bb44059fc59eb62',
            'grupo_areas_visitadas':'66462aa5d4a4af2eea07e0d1',
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
            'configuracion_de_accesos': '696e6dda9517e760679e71eb'
        })

        #BORRAR
        self.CONFIGURACION_RECORRIDOS = self.lkm.catalog_id('configuracion_de_recorridos')
        self.CONFIGURACION_RECORRIDOS_ID = self.CONFIGURACION_RECORRIDOS.get('id')
        self.CONFIGURACION_RECORRIDOS_OBJ_ID = self.CONFIGURACION_RECORRIDOS.get('obj_id')
        self.REGISTRO_ASISTENCIA = self.lkm.form_id('registro_de_asistencia','id')
        self.FORMATO_VACACIONES = self.lkm.form_id('formato_vacaciones_aviso','id')

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
            'incidente_comentario': '681145323d9b5fa2e16e35cc',
            'incidente_area': '663e5d44f5b8a7ce8211ed0f',
            'incidente_location': '663e5c57f5b8a7ce8211ed0b',
            'incidente_evidencia': '681145323d9b5fa2e16e35cd',
            'incidente_documento': '685063ba36910b2da9952697',
            'url_registro_rondin': '6750adb2936622aecd075607',
            'bitacora_rondin_incidencias': '686468a637d014b9e0ab5090',
            'tipo_de_incidencia': '663973809fa65cafa759eb97',
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
        })
        
        self.checkin_fields.update({
            'nombre_suplente':'6927a1176c60848998a157a2'
        })

        self.pase_entrada_fields.update({
            'grupo_vehiculos':'663e446cadf967542759ebba',
        })

    def _do_access(self, access_pass, location, area, data):
        '''
        Registra el acceso del pase de entrada a ubicación.
        solo puede ser ejecutado después de revisar los accesos
        '''
        employee =  self.get_employee_data(email=self.user.get('email'), get_one=True)
        metadata = self.lkf_api.get_metadata(form_id=self.BITACORA_ACCESOS)
        metadata.update({
            'properties': {
                "device_properties":{
                    "System": "Script",
                    "Module": "Accesos",
                    "Process": "Ingreso de Personal",
                    "Action": 'Do Access',
                    "File": "accesos/app.py"
                }
            },
        })
        # metadata['folio'] = self.create_poruction_lot_number()

        try:
            pase = {
                    f"{self.mf['nombre_visita']}": access_pass['nombre'],
                    f"{self.mf['curp']}":access_pass['curp'],
                    ### Campos Select
                    f"{self.mf['empresa']}":[access_pass.get('empresa'),],
                    f"{self.pase_entrada_fields['perfil_pase_id']}": [access_pass['tipo_de_pase'],],
                    # f"{self.pase_entrada_fields['status_pase']}":[access_pass['estatus'],],
                    f"{self.pase_entrada_fields['status_pase']}":['Activo',],
                    f"{self.pase_entrada_fields['foto_pase_id']}": access_pass.get("foto",[]), #[access_pass['foto'],], #.get('foto','')
                    f"{self.pase_entrada_fields['identificacion_pase_id']}": access_pass.get("identificacion",[]) #[access_pass['identificacion'],], #.get('identificacion','')
                    }
        except Exception as e:
            self.LKFException({"msg":f"Error al crear registro ingreso, no se encontro: {e}"}) 

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
                comment_list.append(
                    {
                        self.bitacora_fields['comentario']:c.get('comentario_pase'),
                        self.bitacora_fields['tipo_comentario'] :c.get('tipo_de_comentario').lower().replace(' ', '_')
                    }
                )
            for c in comments_pase:
                comment_list.append(
                    {
                        self.bitacora_fields['comentario']:c.get('comentario_pase'),
                        self.bitacora_fields['tipo_comentario'] :c.get('tipo_de_comentario').lower().replace(' ', '_')
                    }
                )
            answers.update({self.bitacora_fields['grupo_comentario']:comment_list})

        visit_list = data.get('visita_a',[])
        if visit_list:
            visit_list2 = []
            for c in visit_list:
                visit_list2.append(
                   { f"{self.bitacora_fields['visita']}":{ 
                       self.bitacora_fields['visita_nombre_empleado']:c.get('nombre'),
                       self.mf['id_usuario'] :[c.get('user_id')],
                       self.bitacora_fields['visita_departamento_empleado']:[c.get('departamento')],
                       self.bitacora_fields['puesto_empleado']:[c.get('puesto')],
                       self.mf['email_visita_a'] :[c.get('email')]
                   }}
                )
            answers.update({self.bitacora_fields['visita_a']:visit_list2})

        metadata.update({'answers':answers})
        response_create = self.lkf_api.post_forms_answers(metadata)
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
            address = access_pass.get("address")
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
            # f"answers.{self.pase_entrada_fields['visita_a']}.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_empleado']}": employee.get('worker_name') or '',
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
            this_user = self.get_employee_data(email=email, get_one=True)
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

    def get_list_bitacora2(self, location=None, area=None, prioridades=[], dateFrom='', dateTo='', filterDate=""):
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
            'foto_url': {"$arrayElemAt": [f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['foto']}.file_url", 0]},
            'equipos':f"$answers.{self.mf['grupo_equipos']}",
            'grupo_areas_acceso': f"$answers.{self.mf['grupo_areas_acceso']}",
            'id_gafet': f"$answers.{self.GAFETES_CAT_OBJ_ID}.{self.gafetes_fields['gafete_id']}",
            'id_locker': f"$answers.{self.LOCKERS_CAT_OBJ_ID}.{self.lockers_fields['locker_id']}",
            'identificacion':  {"$first":f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['identificacion']}"},
            'pase_id':{"$toObjectId":f"$answers.{self.mf['codigo_qr']}"},
            'motivo_visita':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}.{self.mf['motivo']}",
            'nombre_area_salida':f"$answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.mf['nombre_area_salida']}",
            'nombre_visitante':f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['nombre_visita']}",
            'contratista':f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['empresa']}",
            'perfil_visita':{'$arrayElemAt': [f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['nombre_perfil']}",0]},
            'status_gafete':f"$answers.{self.mf['status_gafete']}",
            'status_visita':f"$answers.{self.mf['tipo_registro']}",
            'ubicacion':f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}",
            'vehiculos':f"$answers.{self.mf['grupo_vehiculos']}",
            'visita_a': f"$answers.{self.mf['grupo_visitados']}"
            }
        lookup = {
         'from': 'form_answer',
         'localField': 'pase_id',
         'foreignField': '_id',
         "pipeline": [
                {'$match':{
                    "deleted_at":{"$exists":False},
                    "form_id": self.PASE_ENTRADA,
                    }
                },
                {'$project':{
                    "_id":0, 
                    'motivo_visita':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}.{self.mf['motivo']}",
                    'grupo_areas_acceso': f"$answers.{self.mf['grupo_areas_acceso']}",                    
                    }
                },
                ],
         'as': 'pase',
        }
       
        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            {'$lookup': lookup},
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
        for r in records:
            pase = r.pop('pase')
            pase_id = r.pop('pase_id')
            # r.pop('pase_id')
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
        return  records

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
            
            if fecha_actual < fecha_visita_tz - timedelta(minutes=30):
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
                "alertas": "$personalizaciones.alertas",
            }}
        ]
        data = self.format_cr_result(self.cr.aggregate(query),  get_one=True)
        format_data = {}

        if data:
            exclude_inputs = data.get('exclude_inputs', [])
            format_exclude_inputs = self.unlist([i for i in exclude_inputs])

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
            link_pass= f"{link_info['link']}?id={record_id}&user={link_info['creado_por_id']}&docs={docs}"
            answers[self.pase_entrada_fields['link']] = link_pass
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

        ### Setting defaults
        if not access_pass.get('tipo_visita_pase') or access_pass['tipo_visita_pase'] :
            access_pass['tipo_visita_pase'] = 'fecha_fija'

        if not  access_pass.get('fecha_desde_visita') or access_pass['fecha_desde_visita'] == "":
            access_pass['fecha_desde_visita'] =  now_datetime
        
        if not access_pass.get('fecha_desde_hasta') or access_pass['fecha_desde_hasta'] == "":
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
                    areas_list.append(
                        {
                            self.pase_entrada_fields['commentario_area']:c.get('commentario_area'),
                            self.pase_entrada_fields['area_catalog_normal'] :{self.mf['nombre_area']: c.get('nombre_area')}
                        }
                    )
                answers.update({self.pase_entrada_fields['grupo_areas_acceso']:areas_list})

        # Perfil de Pase
        answers[self.CONFIG_PERFILES_OBJ_ID] = {
            self.mf['nombre_perfil'] : perfil_pase
        }
        if answers[self.CONFIG_PERFILES_OBJ_ID].get(self.mf['nombre_permiso']) and \
           type(answers[self.CONFIG_PERFILES_OBJ_ID][self.mf['nombre_permiso']]) == str:
            answers[self.CONFIG_PERFILES_OBJ_ID][self.mf['nombre_permiso']] = [answers[self.CONFIG_PERFILES_OBJ_ID][self.mf['nombre_permiso']],]

        #---Valor

        # Crea invitacion de calendario
        if created_from in ('pase_de_entrada_app', 'pase_de_entrada_web') or True:
            #TODO FLUJO DE AUTORIZACION DE PASES
            answers.update(self.access_pass_create_ics(access_pass, answers, ics_invitation))
            answers[self.pase_entrada_fields['catalago_autorizado_por']] = self.autorizar_pase_acceso(answers)


        answers[self.pase_entrada_fields['status_pase']] = self.access_pass_set_status(answers)
        metadata.update({'answers':answers})
        res = self.lkf_api.post_forms_answers(metadata)
        
        qrcode_to_google_pass = ''
        id_forma = ''
        # if res.get("status_code") ==200 or res.get("status_code")==201:
        #     res = self.access_pass_google_pass(res, access_pass)
        return res

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
        try:
            perfiles = self.get_pefiles_walkin(location)
        except:
            perfiles = []
        res = {
            'Areas': areas,
            'Visita_a': visita_a,
            'Perfiles': perfiles
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
                'ubicaciones': f"$answers.{self.pase_entrada_fields['ubicaciones']}"                
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
        if not x:
            self.LKFException({'title':'Advertencia', 'msg':'Este pase fue eliminado o no pertenece a esta organizacion.'})
        return x

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
                print(response.json())
            else:
                print('Error al enviar SMS', response.status_code, response.text)

        except Exception as e:
            print('Error al enviar SMS', e)

    def send_email_and_sms(self, data):
        answers = {}
        phone_to = data['phone_to']
        mensaje = data['mensaje']
        titulo = 'Aviso desde Soter - Accesos'

        metadata = self.lkf_api.get_metadata(form_id=self.ENVIO_DE_CORREOS)
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Addons",
                    "Process": "Creación de envio de correo",
                    "Action": "send_email_and_sms",
                }
            },
        })

        #---Define Answers
        answers.update({
            f"{self.envio_correo_fields['email_from']}": data['email_from'],
            f"{self.envio_correo_fields['titulo']}": titulo,
            f"{self.envio_correo_fields['nombre']}": data['nombre'],
            f"{self.envio_correo_fields['email_to']}": data['email_to'],
            f"{self.envio_correo_fields['msj']}": mensaje,
            f"{self.envio_correo_fields['enviado_desde']}": 'Accesos Aviso',
        })

        metadata.update({'answers': answers})

        email_status = 'Correo: No se realizo la peticion.'
        email_response = self.lkf_api.post_forms_answers(metadata)
        if email_response.get('status_code') == 201:
            email_status = 'Correo: Enviado correctamente'
        else:
            email_status = 'Correo: Hubo un error...'

        message_status = 'Mensaje: No se realizo la peticion.'
        if phone_to:
            sms_response = self.send_sms_masiv(phone_to, mensaje)
            if hasattr(sms_response, "status") and sms_response.status in ["queued", "sent", "delivered"]:
                message_status = 'Mensaje: Enviado correctamente'
            else:
                message_status = 'Mensaje: Hubo un error...'
        
        return {
            "email_status": email_status,
            "message_status": message_status
        }

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
            pdf_to_img = self.update_pass_img(qr_code)
            if pdf_to_img:
                answers.update({self.pase_entrada_fields['pdf_to_img']: pdf_to_img})

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
                res['json'].update({'pdf_to_img': pdf_to_img if pdf_to_img else pass_selected.get('pdf_to_img')})
                res['json'].update({'pdf': pdf})
                return res
            else: 
                return res
        else:
            self.LKFException('No se mandarón parametros para actualizar')
