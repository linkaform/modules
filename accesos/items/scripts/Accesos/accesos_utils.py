# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, time, date
from linkaform_api import base
from lkf_addons.addons.accesos.app import Accesos
import sys, simplejson, json, pytz
import pytz
from math import ceil


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
            'comment_checkin': '66a5b9bed0c44910177eb724',
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

        # self.bitacora_fields.update({
        #     "catalogo_pase_entrada": "66a83ad652d2643c97489d31",
        #     "gafete_catalog": "66a83ace56d1e741159ce114"
        # })

        # self.consecionados_fields.update({
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
            'email_alerta': '695d36605f78faab793f497e'
        })
        
        self.checkin_fields.update({
            'nombre_suplente':'6927a1176c60848998a157a2'
        })
        
        self.checkin_fields.update({
            'nombre_suplente':'6927a1176c60848998a157a2'
        })

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
            booth_status['checkin_id'] = last_chekin['_id']
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

    def get_page_stats(self, booth_area, location, page=''):
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
                    f"answers.{self.consecionados_fields['status_concesion']}": "abierto",
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
        return res

    def get_employee_data(self, name=None, user_id=None, username=None, email=None,  get_one=False):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.EMPLEADOS,
            }
        if name:
            match_query.update(self._get_match_q(self.f['worker_name'], name))
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
                    }
            },
            {'$sort':{'updated_at':-1}},
            {'$group':{
                '_id':{
                    'user_id':'$user_id',
                    },
                'name':{'$last':'$name'},
                'location':{'$last':'$location'},
                'area':{'$last':'$area'},
                'checkin_date':{'$last':'$checkin_date'},
                'checkout_date':{'$last':'$checkout_date'},
                'checkin_status':{'$last':'$checkin_status'},
                'checkin_position':{'$last':'$checkin_position'},
                'folio':{'$last':'$folio'},
                'id_register':{'$last':'$_id'},
                'nombre_suplente':{'$last':'$nombre_suplente'}
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

        #! Se crea el check in.
        resp_create = self.lkf_api.post_forms_answers(data)
        if resp_create.get('status_code') == 201:
            resp_create['json'].update({'boot_status':{'guard_on_duty':user_data['name']}})
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
                resp_create.update({'registro_de_asistencia': 'Correcto'})
            else:
                resp_create.update({'registro_de_asistencia': 'Error'})
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
            if employee:
                record_id = self.search_guard_asistance(location, area, self.unlist(employee.get('usuario_id')))
                asistencia_answers = {
                    self.f['foto_cierre_turno']: fotografia,
                    self.checkin_fields['checkin_type']: 'cerrar_turno',
                }
                res = self.lkf_api.patch_multi_record(answers=asistencia_answers, form_id=self.REGISTRO_ASISTENCIA, record_id=record_id)
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
                answers[self.consecionados_fields['ubicacion_catalog_concesion']] = { self.mf['ubicacion']: value}
            elif  key == 'area_paqueteria':
                 answers[self.consecionados_fields['area_catalog_concesion']] = { self.mf['nombre_area_salida']: value}
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
                {'$sort':{'folio':-1}},
            )
        else:
            query.append(
                {'$sort':{'folio':-1}},
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

    def get_pdf_seg(self, qr_code, template_id=491, name_pdf='Pase de Entrada'):
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
                {'$sort':{'folio':-1}},
            )
        else:
            query.append(
                {'$sort':{'folio':-1}},
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

    # def get_page_stats(self, booth_area, location, page=''):
    #     print('entra a get_booth_stats')
    #     print('booth_area', booth_area)
    #     print('location', location)
    #     today = datetime.today().strftime("%Y-%m-%d")
    #     res={}

    #     if page == 'Turnos':
    #         #Visitas dentro, Gafetes pendientes y Vehiculos estacionados
    #         match_query_visitas = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_ACCESOS,
    #             f"answers.{self.bitacora_fields['status_visita']}": "entrada",
    #             f"answers.{self.bitacora_fields['catalogo_pase_entrada']}.{self.pase_entrada_fields['status_pase']}": {"$in": ["Activo"]},
    #             f"answers.{self.bitacora_fields['caseta_entrada']}": booth_area,
    #             f"answers.{self.bitacora_fields['ubicacion']}": location,
    #         }

    #         proyect_fields_visitas = {
    #             '_id': 1,
    #             'vehiculos': {"$ifNull": [f"$answers.{self.mf['grupo_vehiculos']}", []]},
    #             'id_gafete': f"$answers.{self.bitacora_fields['gafete_catalog']}.{self.gafetes_fields['gafete_id']}",
    #             'status_gafete': f"$answers.{self.mf['status_gafete']}"
    #         }

    #         group_by_visitas = {
    #             '_id': None,
    #             'total_visitas_dentro': {'$sum': 1},
    #             'total_vehiculos_dentro': {'$sum': {'$size': '$vehiculos'}},
    #             'gafetes_info': {
    #                 '$push': {
    #                     'id_gafete':'$id_gafete',
    #                     'status_gafete':'$status_gafete'
    #                 }
    #             }
    #         }

    #         query_visitas = [
    #             {'$match': match_query_visitas},
    #             {'$project': proyect_fields_visitas},
    #             {'$group': group_by_visitas}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_visitas))
    #         total_vehiculos_dentro = resultado[0]['total_vehiculos_dentro'] if resultado else 0
    #         total_visitas_dentro = resultado[0]['total_visitas_dentro'] if resultado else 0
    #         gafetes_info = resultado[0]['gafetes_info'] if resultado else []
    #         gafetes_pendientes = sum(1
    #             for gafete in gafetes_info
    #                 if gafete.get('id_gafete') and gafete.get('status_gafete', '').lower() != 'entregado'
    #         )
            
    #         res['total_vehiculos_dentro'] = total_vehiculos_dentro
    #         res['in_invitees'] = total_visitas_dentro
    #         res['gafetes_pendientes'] = gafetes_pendientes

    #         #Articulos concesionados
    #         match_query_concesionados = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.CONCESSIONED_ARTICULOS,
    #             f"answers.{self.consecionados_fields['catalogo_ubicacion_concesion']}.{self.mf['ubicacion']}": location,
    #         }

    #         proyect_fields_concesionados = {
    #             '_id': 1,
    #         }

    #         group_by_concesionados = {
    #             '_id': None,
    #             'articulos_concesionados': {'$sum': 1}
    #         }

    #         query_concesionados = [
    #             {'$match': match_query_concesionados},
    #             {'$project': proyect_fields_concesionados},
    #             {'$group': group_by_concesionados}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_concesionados))
    #         articulos_concesionados = resultado[0]['articulos_concesionados'] if resultado else 0
            
    #         res['articulos_concesionados'] = articulos_concesionados

    #         #Incidentes pendientes
    #         match_query_incidentes = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_INCIDENCIAS,
    #             f"answers.{self.incidence_fields['area_incidencia_catalog']}.{self.incidence_fields['area_incidencia']}": booth_area,
    #             f"answers.{self.incidence_fields['ubicacion_incidencia_catalog']}.{self.incidence_fields['ubicacion_incidencia']}": location
    #         }

    #         proyect_fields_incidentes = {
    #             '_id': 1,
    #             'acciones_tomadas_incidencia': f"$answers.{self.incidence_fields['acciones_tomadas_incidencia']}",
    #         }

    #         group_by_incidentes = {
    #             '_id': None,
    #             'incidentes_pendientes': {'$sum': {'$cond': [{'$or': [{'$eq': [{'$size': {'$ifNull': ['$acciones_tomadas_incidencia', []]}}, 0]},{'$eq': ['$acciones_tomadas_incidencia', None]}]}, 1, 0]}}
    #         }

    #         query_incidentes = [
    #             {'$match': match_query_incidentes},
    #             {'$project': proyect_fields_incidentes},
    #             {'$group': group_by_incidentes}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_incidentes))
    #         incidentes_pendientes = resultado[0]['incidentes_pendientes'] if resultado else 0
            
    #         res['incidentes_pendites'] = incidentes_pendientes
    #     elif page == 'Accesos' or page == 'Bitacoras':
    #         #Visitas en el dia, personal dentro, vehiculos dentro y salidas registradas
    #         match_query_visitas = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_ACCESOS,
    #             # f"answers.{self.bitacora_fields['status_visita']}": "entrada",
    #             f"answers.{self.bitacora_fields['catalogo_pase_entrada']}.{self.pase_entrada_fields['status_pase']}": {"$in": ["Activo"]},
    #             f"answers.{self.bitacora_fields['caseta_entrada']}": booth_area,
    #             f"answers.{self.bitacora_fields['ubicacion']}": location,
    #             f"answers.{self.mf['fecha_entrada']}": {"$gte": today,"$lt": f"{today}T23:59:59"}
    #         }

    #         proyect_fields_visitas = {
    #             '_id': 1,
    #             'vehiculos': {"$ifNull": [f"$answers.{self.mf['grupo_vehiculos']}", []]},
    #             'perfil': f"$answers.{self.bitacora_fields['catalogo_pase_entrada']}.{self.mf['nombre_perfil']}",
    #             'status_visita': f"$answers.{self.bitacora_fields['status_visita']}"
    #         }

    #         group_by_visitas = {
    #             '_id': None,
    #             'visitas_en_dia': {'$sum': 1},
    #             'total_vehiculos_dentro': {'$sum': {'$size': '$vehiculos'}},
    #             'detalle_visitas': {
    #                 '$push': {
    #                     'perfil': '$perfil',
    #                     'status_visita': '$status_visita'
    #                 }
    #             }
    #         }

    #         query_visitas = [
    #             {'$match': match_query_visitas},
    #             {'$project': proyect_fields_visitas},
    #             {'$group': group_by_visitas}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_visitas))
    #         # print('resultadooooooooooooooooo',resultado)
    #         total_vehiculos_dentro = resultado[0]['total_vehiculos_dentro'] if resultado else 0
    #         visitas_en_dia = resultado[0]['visitas_en_dia'] if resultado else 0
    #         detalle_visitas = resultado[0]['detalle_visitas'] if resultado else []
    #         personal_dentro = sum(1 for visita in detalle_visitas if visita['perfil'][0].lower() != "visita general")
    #         salidas = sum(1 for visita in detalle_visitas if visita['status_visita'].lower() == "salida")

    #         res['total_vehiculos_dentro'] = total_vehiculos_dentro
    #         res['visitas_en_dia'] = visitas_en_dia
    #         res['personal_dentro'] = personal_dentro
    #         res['salidas_registradas'] = salidas
    #     elif page == 'Incidencias':
    #         #Incidentes por dia
    #         match_query_incidentes = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_INCIDENCIAS,
    #             f"answers.{self.incidence_fields['area_incidencia_catalog']}.{self.incidence_fields['area_incidencia']}": booth_area,
    #             f"answers.{self.incidence_fields['area_incidencia_catalog']}.{self.incidence_fields['area_incidencia']}": booth_area,
    #             f"answers.{self.incidence_fields['ubicacion_incidencia_catalog']}.{self.incidence_fields['ubicacion_incidencia']}": location,
    #             f"answers.{self.incidence_fields['fecha_hora_incidencia']}": {"$gte": today,"$lt": f"{today}T23:59:59"}
    #         }

    #         proyect_fields_incidentes = {
    #             '_id': 1,
    #         }

    #         group_by_incidentes = {
    #             '_id': None,
    #             'incidentes_x_dia': {'$sum': 1}
    #         }

    #         query_incidentes = [
    #             {'$match': match_query_incidentes},
    #             {'$project': proyect_fields_incidentes},
    #             {'$group': group_by_incidentes}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_incidentes))
    #         incidentes_x_dia = resultado[0]['incidentes_x_dia'] if resultado else 0

    #         res['incidentes_x_dia'] = incidentes_x_dia

    #         #Fallas pendientes
    #         match_query_fallas = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_FALLAS,
    #             f"answers.{self.fallas_fields['falla_ubicacion_catalog']}.{self.fallas_fields['falla_caseta']}": booth_area,
    #             f"answers.{self.fallas_fields['falla_ubicacion_catalog']}.{self.fallas_fields['falla_ubicacion']}": location,
    #             f"answers.{self.fallas_fields['falla_estatus']}": 'abierto',
    #             # f"answers.{self.incidence_fields['fecha_hora_incidencia']}": {"$gte": today,"$lt": f"{today}T23:59:59"}
    #         }

    #         proyect_fields_fallas = {
    #             '_id': 1,
    #         }

    #         group_by_fallas = {
    #             '_id': None,
    #             'fallas_pendientes': {'$sum': 1}
    #         }

    #         query_fallas = [
    #             {'$match': match_query_fallas},
    #             {'$project': proyect_fields_fallas},
    #             {'$group': group_by_fallas}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_fallas))
    #         fallas_pendientes = resultado[0]['fallas_pendientes'] if resultado else 0

    #         res['fallas_pendientes'] = fallas_pendientes
    #     elif page == 'Articulos':
    #         #Articulos concesionados pendientes
    #         match_query_concesionados = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.CONCESSIONED_ARTICULOS,
    #             f"answers.{self.consecionados_fields['catalogo_ubicacion_concesion']}.{self.mf['ubicacion']}": location,
    #             f"answers.{self.consecionados_fields['status_concesion']}": "abierto",
    #         }

    #         proyect_fields_concesionados = {
    #             '_id': 1,
    #         }

    #         group_by_concesionados = {
    #             '_id': None,
    #             'articulos_concesionados_pendientes': {'$sum': 1}
    #         }

    #         query_concesionados = [
    #             {'$match': match_query_concesionados},
    #             {'$project': proyect_fields_concesionados},
    #             {'$group': group_by_concesionados}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_concesionados))
    #         articulos_concesionados_pendientes = resultado[0]['articulos_concesionados_pendientes'] if resultado else 0
            
    #         res['articulos_concesionados_pendientes'] = articulos_concesionados_pendientes

    #         #Articulos perdidos
    #         match_query_perdidos = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_OBJETOS_PERDIDOS,
    #             f"answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.perdidos_fields['ubicacion_perdido']}": location,
    #             f"answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.perdidos_fields['area_perdido']}": booth_area,
    #         }

    #         proyect_fields_perdidos = {
    #             '_id': 1,
    #             'status_perdido': f"$answers.{self.perdidos_fields['estatus_perdido']}",
    #         }

    #         group_by_perdidos = {
    #             '_id': None,
    #             'perdidos_info': {
    #                 '$push': {
    #                     'status_perdido':'$status_perdido'
    #                 }
    #             }
    #         }

    #         query_perdidos = [
    #             {'$match': match_query_perdidos},
    #             {'$project': proyect_fields_perdidos},
    #             {'$group': group_by_perdidos}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_perdidos))
    #         perdidos_info = resultado[0]['perdidos_info'] if resultado else []

    #         articulos_perdidos = 0
    #         for perdido in perdidos_info:
    #             status_perdido = perdido.get('status_perdido', '').lower()
    #             if status_perdido not in ['entregado', 'donado']:
    #                 articulos_perdidos += 1

    #         res['articulos_perdidos'] = articulos_perdidos

    #     # res ={
    #     #         "in_invitees":0,
    #     #         "articulos_concesionados":0,
    #     #         "incidentes_pendites": incidentes_pendientes,
    #     #         "vehiculos_estacionados": total_vehiculos,
    #     #         "gefetes_pendientes": 0,
    #     #     }
    #     return res
    
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

    def create_access_pass(self, location, access_pass):
        #---Define Metadata
        metadata = self.lkf_api.get_metadata(form_id=self.PASE_ENTRADA)
        metadata.update({
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

        #---Define Answers
        answers = {}
        perfil_pase = access_pass.get('perfil_pase')
        location_name = access_pass.get('ubicacion')
        if not location:
            location = location_name
        address = self.get_location_address(location_name=location_name)
        access_pass['direccion'] = [address.get('address', '')]
        user_data = self.lkf_api.get_user_by_id(self.user.get('user_id'))
        timezone = user_data.get('timezone','America/Monterrey')
        now_datetime =self.today_str(timezone, date_format='datetime')
        employee = self.get_employee_data(email=self.user.get('email'), get_one=True)
        company = employee.get('company', 'Soter')
        nombre_visita_a = employee.get('worker_name')

        if(access_pass.get('site', '') == 'accesos'):
            nombre_visita_a = access_pass.get('visita_a')
            access_pass['ubicaciones'] = [location]

        answers[self.UBICACIONES_CAT_OBJ_ID] = {}
        # answers[self.UBICACIONES_CAT_OBJ_ID][self.f['location']] = location
        # if access_pass.get('selected_visita_a'):
        #     nombre_visita_a = access_pass.get('selected_visita_a')
        if access_pass.get('custom') == True :
            answers[self.pase_entrada_fields['tipo_visita_pase']] = access_pass.get('tipo_visita_pase',"")
            answers[self.pase_entrada_fields['fecha_desde_visita']] = access_pass.get('fecha_desde_visita',"")
            answers[self.pase_entrada_fields['fecha_desde_hasta']] = access_pass.get('fecha_desde_hasta',"")
            answers[self.pase_entrada_fields['config_dia_de_acceso']] = access_pass.get('config_dia_de_acceso',"")
            answers[self.pase_entrada_fields['config_dias_acceso']] = access_pass.get('config_dias_acceso',"")
            answers[self.pase_entrada_fields['catalago_autorizado_por']] =  {self.pase_entrada_fields['autorizado_por']:nombre_visita_a}
            answers[self.pase_entrada_fields['status_pase']] = access_pass.get('status_pase',"").lower()
            answers[self.pase_entrada_fields['empresa_pase']] = access_pass.get('empresa',"")
            # answers[self.pase_entrada_fields['ubicacion_cat']] = {self.mf['ubicacion']:access_pass['ubicacion'], self.mf['direccion']:access_pass.get('direccion',"")}
            answers[self.pase_entrada_fields['tema_cita']] = access_pass.get('tema_cita',"") 
            answers[self.pase_entrada_fields['descripcion']] = access_pass.get('descripcion',"") 
            answers[self.pase_entrada_fields['config_limitar_acceso']] = access_pass.get('config_limitar_acceso',"") 

        else:
            answers[self.mf['fecha_desde_visita']] = now_datetime
            answers[self.mf['tipo_visita_pase']] = 'fecha_fija'
        answers[self.pase_entrada_fields['tipo_visita']] = 'alta_de_nuevo_visitante'
        answers[self.pase_entrada_fields['walkin_nombre']] = access_pass.get('nombre')
        answers[self.pase_entrada_fields['walkin_email']] = access_pass.get('email', '')
        answers[self.pase_entrada_fields['walkin_empresa']] = access_pass.get('empresa')
        answers[self.pase_entrada_fields['walkin_fotografia']] = access_pass.get('foto')
        answers[self.pase_entrada_fields['walkin_identificacion']] = access_pass.get('identificacion')
        answers[self.pase_entrada_fields['walkin_telefono']] = access_pass.get('telefono', '')
        answers[self.pase_entrada_fields['status_pase']] = access_pass.get('status_pase',"").lower()
        
        if access_pass.get('ubicaciones'):
            ubicaciones = access_pass.get('ubicaciones',[])
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

        print(access_pass.get('areas'))

        #Visita A
        answers[self.mf['grupo_visitados']] = []
        nombre_visita_a = access_pass.get('visita_a') if not nombre_visita_a else nombre_visita_a
        if access_pass.get('selected_visita_a'):
            nombre_visita_a = access_pass.get('selected_visita_a')
        visita_set = {
            self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID:{
                self.mf['nombre_empleado'] : nombre_visita_a,
                }
            }
        options_vistia = {
              "group_level": 3,
              "startkey": [location, nombre_visita_a],
              "endkey": [location, f"{nombre_visita_a}\n",{}],
            }
        cat_visita = self.catalogo_view(self.CONF_AREA_EMPLEADOS_CAT_ID, self.PASE_ENTRADA, options_vistia)
        if len(cat_visita) > 0:
            cat_visita =  {key: [value,] for key, value in cat_visita[0].items() if value}
        else:
            selector = {}
            selector.update({f"answers.{self.mf['nombre_empleado']}": nombre_visita_a})
            fields = ["_id", f"answers.{self.mf['nombre_empleado']}", f"answers.{self.mf['email_visita_a']}", f"answers.{self.mf['id_usuario']}"]

            mango_query = {
                "selector": selector,
                "fields": fields,
                "limit": 1
            }

            row_catalog = self.lkf_api.search_catalog(self.CONF_AREA_EMPLEADOS_CAT_ID, mango_query)
            if row_catalog:
                visita_set[self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID].update({
                    self.mf['nombre_empleado']: nombre_visita_a,
                    self.mf['email_visita_a']: [row_catalog[0].get(self.mf['email_visita_a'], "")],
                    self.mf['id_usuario']: [row_catalog[0].get(self.mf['id_usuario'], "")],
                })

        visita_set[self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID].update(cat_visita)
        answers[self.mf['grupo_visitados']].append(visita_set)

        # Perfil de Pase
        answers[self.CONFIG_PERFILES_OBJ_ID] = {
            self.mf['nombre_perfil'] : perfil_pase,
        }
        if answers[self.CONFIG_PERFILES_OBJ_ID].get(self.mf['nombre_permiso']) and \
           type(answers[self.CONFIG_PERFILES_OBJ_ID][self.mf['nombre_permiso']]) == str:
            answers[self.CONFIG_PERFILES_OBJ_ID][self.mf['nombre_permiso']] = [answers[self.CONFIG_PERFILES_OBJ_ID][self.mf['nombre_permiso']],]

        #---Valor
        metadata.update({'answers':answers})
        res = self.lkf_api.post_forms_answers(metadata)
        qrcode_to_google_pass = ''
        id_forma = ''
        if res.get("status_code") ==200 or res.get("status_code")==201:
            qrcode_to_google_pass = res.get('json', {}).get('id', '')
            link_info=access_pass.get('link', "")
            docs=""
            
            if link_info:
                for index, d in enumerate(link_info["docs"]): 
                    if(d == "agregarIdentificacion"):
                        docs+="iden"
                    elif(d == "agregarFoto"):
                        docs+="foto"
                    if index==0 :
                        docs+="-"
                link_pass= f"{link_info['link']}?id={res.get('json')['id']}&user={link_info['creado_por_id']}&docs={docs}"
                id_forma = self.PASE_ENTRADA
                id_campo = self.pase_entrada_fields['archivo_invitacion']

                tema_cita = access_pass.get("tema_cita")
                descripcion = access_pass.get("descripcion")
                fecha_desde_visita = access_pass.get("fecha_desde_visita")
                fecha_desde_hasta = access_pass.get("fecha_desde_hasta")
                creado_por_email = access_pass.get("link", {}).get("creado_por_email")
                ubicacion = access_pass.get("ubicacion")
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
                    "empresa": company,
                    "all_data": access_pass
                }

                google_wallet_pass_url = self.create_class_google_wallet(data=data_to_google_pass, qr_code=qrcode_to_google_pass)
                access_pass_custom.update({
                    "google_wallet_pass_url": google_wallet_pass_url,
                })
                
                self.update_pass(access_pass=access_pass_custom, folio=res.get("json")["id"])
            
        return res

    def update_full_pass(self, access_pass,folio=None, qr_code=None, location=None):
        answers = {}
        perfil_pase = access_pass.get('perfil_pase', 'Visita General')
        user_data = self.lkf_api.get_user_by_id(self.user.get('user_id'))
        this_user = self.get_employee_data(user_id=self.user.get('user_id'), get_one=True)
        this_user_name = this_user.get('worker_name', '')
        timezone = user_data.get('timezone','America/Monterrey')
        now_datetime =self.today_str(timezone, date_format='datetime')
        answers[self.mf['grupo_visitados']] = []
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
            elif key == 'visita_a': 
                #Visita A
                answers[self.mf['grupo_visitados']] = []
                visita_a = access_pass.get('visita_a')
                visita_set = {
                    self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID:{
                        self.mf['nombre_empleado'] : visita_a,
                        }
                    }
                options_vistia = {
                      "group_level": 3,
                      "startkey": [location, visita_a],
                      "endkey": [location, f"{visita_a}\n",{}],
                    }
                cat_visita = self.catalogo_view(self.CONF_AREA_EMPLEADOS_CAT_ID, self.PASE_ENTRADA, options_vistia)
                if len(cat_visita) > 0:
                    cat_visita =  {key: [value,] for key, value in cat_visita[0].items() if value}
                visita_set[self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID].update(cat_visita)
                answers[self.mf['grupo_visitados']].append(visita_set)
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

    def assets_access_pass(self, location):
        ### Areas
        catalog_id = self.AREAS_DE_LAS_UBICACIONES_CAT_ID
        form_id = self.PASE_ENTRADA
        group_level = 2
        options = {
              "group_level": group_level,
              "startkey": [
                location
              ],
              "endkey": [
                f"{location}\n",
                {}
              ]
            }
        areas = self.lkf_api.catalog_view(catalog_id, form_id, options) 
        ### Aquien Visita
        catalog_id = self.CONF_AREA_EMPLEADOS_CAT_ID
        visita_a = self.lkf_api.catalog_view(catalog_id, form_id, options) 
        # visita_a = [r.get('key')[group_level-1] for r in visita_a]
        ### Pases de accesos
        res = {
            'Areas': areas,
            'Visita_a': visita_a,
            'Perfiles': self.get_pefiles_walkin(location),
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
            data = self.lkf_api.patch_multi_record( answers = answers, form_id=self.CHECKIN_CASETAS, record_id=[record_id])
            if data.get('status_code') in [200, 201, 202]:
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
                response_asistencia = self.lkf_api.post_forms_answers(metadata)
                if response_asistencia.get('status_code') in [200, 201, 202]:
                    data.update({'registro_de_asistencia': 'Correcto'})
                else:
                    data.update({'registro_de_asistencia': 'Error'})
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
            if employee:
                record_id = self.search_guard_asistance(location, area, self.unlist(employee.get('usuario_id')))
                asistencia_answers = {
                    self.f['foto_cierre_turno']: fotografia,
                    self.checkin_fields['checkin_type']: 'cerrar_turno',
                }
                res = self.lkf_api.patch_multi_record(answers=asistencia_answers, form_id=self.REGISTRO_ASISTENCIA, record_id=record_id)
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

        if location:
            match_query.update({
                f"answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['ubicacion']}": location,
            })
        if area:
            match_query.update({
                f"answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_area']}": area,
            })
        
        query = [
            {'$match': match_query},
            {'$unwind': f"$answers.{self.f['guard_group']}"},
            {'$match': {
                f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.mf['id_usuario']}": {"$exists":True},
                f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.mf['id_usuario']}": {"$in": [user_id]},
                f"answers.{self.f['guard_group']}.{self.f['checkout_date']}": {"$in": [None, ""]}
            }},
            {'$sort': {'created_at': -1}},
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
            {'$sort': {'updated_at': -1}},
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
