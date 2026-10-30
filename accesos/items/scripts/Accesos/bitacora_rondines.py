# coding: utf-8
import datetime, sys, simplejson, traceback
from copy import deepcopy
from bson import ObjectId


from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos


class Accesos(Accesos):
    
    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)
        self.f.update({
            'porcentaje_obtenido_bitacora': '689a7ecfbf2b4be31039388e',
            'cantidad_areas_inspeccionadas': '68a7b68a22ac030a67b7f8f8'
        })

    def calcluta_tiempo_traslados(self):
        fecha_inicio = self.date_2_epoch(self.answers.get(self.f['fecha_inicio_rondin']))
        areas_visitadas = self.answers.get(self.f['areas_del_rondin'], [])
        areas_con_fecha = [
            (self.date_2_epoch(a.get(self.f['fecha_inspeccion_area'])), a)
            for a in areas_visitadas
        ]
        areas_con_fecha = sorted(
            [(epoch, a) for epoch, a in areas_con_fecha if epoch],
            key=lambda x: x[0]
        )

        if not areas_con_fecha:
            return True

        if not fecha_inicio:
            fecha_inicio = areas_con_fecha[0][0]
            self.answers[self.f['fecha_inicio_rondin']] = datetime.datetime.fromtimestamp(fecha_inicio).strftime('%Y-%m-%d %H:%M:%S')

        first_epoch = areas_con_fecha[0][0]
        for epoch, area in areas_con_fecha:
            area[self.f['duracion_traslado_area']] = round((epoch - first_epoch) / 60, 2)

        fecha_final = areas_con_fecha[-1][0]
        cantidad_inspeccionadas = len(areas_con_fecha)

        self.answers[self.f['duracion_rondin']] = round((fecha_final - fecha_inicio) / 60, 2)
        self.answers[self.f['porcentaje_obtenido_bitacora']] = (
            str(round((cantidad_inspeccionadas / len(areas_visitadas)) * 100, 2)) + '%'
        )
        self.answers[self.f['cantidad_areas_inspeccionadas']] = (
            f"{cantidad_inspeccionadas}/{len(areas_visitadas)}"
        )
        self.answers[self.f['areas_del_rondin']] = [a for _, a in areas_con_fecha] + [
            a for epoch, a in zip(
                [self.date_2_epoch(a.get(self.f['fecha_inspeccion_area'])) for a in areas_visitadas],
                areas_visitadas
            ) if not epoch
        ]

        if self.answers.get(self.f['estatus_del_recorrido']) in ['realizado', 'cerrado']:
            fecha_final_str = datetime.datetime.fromtimestamp(fecha_final).strftime('%Y-%m-%d %H:%M:%S')
            self.answers[self.f['fecha_fin_rondin']] = fecha_final_str
        return True
    
    def get_and_set_areas_recorrido(self):
        location = self.answers.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.Location.f['location'], '')
        area = self.answers.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.Location.f['area'], '')
        name_rondin = self.answers.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.mf['nombre_del_recorrido'], '')
        match = {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.CONFIGURACION_DE_RECORRIDOS_FORM,
                f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": location,
                f"answers.{self.mf['nombre_del_recorrido']}": name_rondin
            }}
        if area:
            match["$match"].update(
                {f"answers.{self.Location.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.Location.f['area_salida']}": area}
                )
        query = [
            match,
            {"$project": {
                "_id": 0,
                "rondin_areas": f"$answers.{self.f['grupo_de_areas_recorrido']}",
                "tipo_asignacion": f"$answers.{self.rondin_keys['tipo_asignacion']}",
                "grupo_asignado_a": f"$answers.{self.rondin_keys['grupo_asignado_a']}",
                "id_grupo": f"$answers.{self.GRUPOS_CAT_OBJ_ID}.{self.mf['id_grupo']}",
                "nombre_grupo": f"$answers.{self.GRUPOS_CAT_OBJ_ID}.{self.mf.get('nombre_grupo', '638a9ab3616398d2e392a9fa')}",
                "roles": f"$answers.{self.f['grupo_roles']}"
            }}
        ]
        res = self.cr.aggregate(query)
        format_res = list(res)
        if format_res:
            areas_recorrido = self.unlist(format_res)
            self.answers[self.f['areas_del_rondin']] = areas_recorrido.get('rondin_areas', [])
            self.answers[self.rondin_keys['tipo_asignacion']] = areas_recorrido.get('tipo_asignacion')
            self.answers[self.rondin_keys['grupo_asignado_a']] = areas_recorrido.get('grupo_asignado_a', [])
            self.answers[self.f['grupo_roles']] = areas_recorrido.get('roles', [])
            grupo = {}
            if areas_recorrido.get('id_grupo'):
                id_cfg = areas_recorrido['id_grupo']
                grupo[self.mf['id_grupo']] = id_cfg if isinstance(id_cfg, list) else [id_cfg]
            if areas_recorrido.get('nombre_grupo'):
                grupo[self.mf.get('nombre_grupo', '638a9ab3616398d2e392a9fa')] = areas_recorrido['nombre_grupo']
            if grupo:
                self.answers[self.GRUPOS_CAT_OBJ_ID] = grupo
            return True
        print(f'Warning: no se encontro configuracion de recorrido form_id={self.CONFIGURACION_DE_RECORRIDOS_FORM} location={location!r} area={area!r} nombre={name_rondin!r}')
        return False

    def get_active_guards_in_location(self, location, rol=None):
        match = {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.REGISTRO_ASISTENCIA,
                f"answers.{self.f['start_shift']}": {"$exists": True},
                f"answers.{self.f['end_shift']}": {"$exists": False},
                f"answers.{self.Employee.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.f['ubicacion']}": location,
            }}
        if rol:
            match["$match"].update(
                {f"answers.{self.f['grupo_roles']}.{self.ROL_CATALOG_OBJ_ID}.{self.f['rol']}": {"$in": rol}}
                )
        query = [
            match,
            {"$project": {
                "_id": 1,
                "created_at": 1,
                "created_by_id": 1,
                "created_by_email": 1,
                "created_by_name": 1,
            }},
            {"$sort": {
                "created_at": -1
            }},
            {"$limit": 1}

        ]
        response = self.format_cr(self.cr.aggregate(query), get_one=True)
        return response
    
    def normalize_user(self, raw_user) -> dict:
        """
        Normaliza un usuario independientemente de si viene de:
        - un catálogo de LinkaForm (keys = field_obj_ids)
        - la API de grupos (keys = name, email, id, username)
        """
        # Caso 1: viene de catálogo LinkaForm
        # raw_user = { '677ffe8c...': { '62c5ff40...': 'Jose Patricio', '638a9a77...': [...], ... } }
        # se asume que id_usuario y email_vista_a son una lista
        if self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID in raw_user:
            catalog = raw_user[self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID]
            return {
                self.mf['nombre_usuario']: catalog.get(self.mf['nombre_empleado']),
                self.mf['id_usuario']:     catalog.get(self.mf['id_usuario']),
                self.mf['email_visita_a']: catalog.get(self.mf['email_visita_a']),
            }

        # Caso 2: viene de get_group_users() → { 'name': ..., 'email': ..., 'id': ... }
        # se configura en lista id_usuario y email
        if 'name' in raw_user or 'email' in raw_user:
            return {
                self.mf['nombre_usuario']: raw_user.get('name'),
                self.mf['id_usuario']:     [raw_user.get('id')],
                self.mf['email_visita_a']: [raw_user.get('email')],
            }

        return {}

    def resolve_group_id(self):
        """
        Id del grupo asignado. Si la configuracion solo guardo el nombre del grupo se busca por nombre;
        los grupos de la cuenta llevan el id de la cuenta como sufijo (p.ej. 'Guardias' -> 'Guardias_10').
        """
        grupo = self.answers.get(self.GRUPOS_CAT_OBJ_ID, {}) or {}
        id_grupo = self.unlist(grupo.get(self.mf['id_grupo'], ''))
        if id_grupo:
            return id_grupo
        nombre = self.unlist(grupo.get(self.mf.get('nombre_grupo', '638a9ab3616398d2e392a9fa'), ''))
        if not nombre:
            return ''
        sufijos = {str(self.user.get(k)) for k in ('parent_id', 'account_id') if self.user.get(k)}
        candidatos = {str(nombre).lower()} | {f"{nombre}_{s}".lower() for s in sufijos}
        for grupo_lkf in self.lkf_api.get_updated_groups(0) or []:
            if str(grupo_lkf.get('name', '')).lower() in candidatos:
                return grupo_lkf.get('id')
        return ''

    def get_group_name(self, id_grupo):
        """Nombre del grupo como lo guarda el catalogo (sin el sufijo `_<cuenta>` del grupo de LinkForm)."""
        for grupo_lkf in self.lkf_api.get_updated_groups(0) or []:
            if str(grupo_lkf.get('id')) == str(id_grupo):
                nombre = str(grupo_lkf.get('name', ''))
                for s in {str(self.user.get(k)) for k in ('parent_id', 'account_id') if self.user.get(k)}:
                    if nombre.endswith(f'_{s}'):
                        return nombre[:-len(s) - 1]
                return nombre
        return ''

    def set_group_inbox(self):
        """
        Rondin asignado a un grupo: el registro queda SIN usuario y se deja en el inbox (CouchDB) de cada
        usuario del grupo. Al iniciarlo uno, claim_rondin (sanic) lo asigna y lo quita de los demas.
        """
        id_grupo = self.resolve_group_id()
        if not id_grupo:
            print('Warning: el recorrido esta asignado a un grupo pero no se encontro el grupo')
            return False
        grupo = dict(self.answers.get(self.GRUPOS_CAT_OBJ_ID, {}) or {})
        grupo[self.mf['id_grupo']] = [id_grupo]  # catalog-detail va como lista, igual que id_usuario
        nombre_key = self.mf.get('nombre_grupo', '638a9ab3616398d2e392a9fa')
        if not grupo.get(nombre_key):
            nombre = self.get_group_name(id_grupo)
            if nombre:
                grupo[nombre_key] = nombre
        self.answers[self.GRUPOS_CAT_OBJ_ID] = grupo
        users = [self.normalize_user(u) for u in (self.lkf_api.get_group_users(id_grupo) or [])]
        users = [u for u in users if self.unlist(u.get(self.mf['id_usuario']))]
        if not users:
            print(f'Warning: el grupo {id_grupo} no tiene usuarios')
            return False
        ids = [self.unlist(u.get(self.mf['id_usuario'])) for u in users]
        for user in users:
            uid = self.unlist(user.get(self.mf['id_usuario']))
            try:
                #! El hook corre en cada guardado mientras siga programado: no se duplica el inbox.
                if self.get_couch_user_db(f"clave_{uid}").get(str(self.record_id)):
                    continue
                data = deepcopy(self.answers)
                data[self.USUARIOS_OBJ_ID] = user
                res = self.assign_user_inbox(data)
                if res.get('status_code') != 200:
                    print(f'Warning: no se pudo enviar el rondin {self.record_id} al inbox de {uid}: {res}')
                    continue
                #! assign_user_inbox deja self.cr_db en la db de ese usuario: se marca el doc como de grupo para que la app lo reclame al iniciar.
                doc = self.cr_db.get(str(self.record_id))
                doc['record'].update({'tipo_asignacion': 'grupo', 'grupo_users': ids})
                self.cr_db.save(doc)
            except Exception:
                print(f'Warning: error al enviar el rondin {self.record_id} al inbox de {uid}:', traceback.format_exc())
        return True

    def get_and_set_user(self):
        tipo_asignacion = self.answers.get(self.rondin_keys['tipo_asignacion'])
        grupo_asignado_a = self.answers.get(self.rondin_keys['grupo_asignado_a']) or []
        print('tipo_asignacion = ', tipo_asignacion)
        if tipo_asignacion == 'grupo':
            #! Un solo registro: se entrega al inbox de cada usuario del grupo y se asigna a quien lo inicie (claim_rondin en sanic).
            return self.set_group_inbox()
        if tipo_asignacion and tipo_asignacion in ('persona_especifica',):
            if not grupo_asignado_a:
                print('Warning: no se encontraron usuarios para asignar el recorrido')
                return False
            new_metadata = deepcopy(self.current_record)
            new_metadata.pop('answers')
            new_metadata.pop('_id')
            for raw_user in grupo_asignado_a:
                user = self.normalize_user(raw_user)
                child_anwers = deepcopy(self.answers)
                child_anwers[self.rondin_keys['registro_padre']] = self.record_id
                if not self.answers.get(self.USUARIOS_OBJ_ID):
                    self.answers[self.USUARIOS_OBJ_ID] = user
                else:
                    child_anwers[self.USUARIOS_OBJ_ID] = user
                    parent_record = f"{self.settings.config['PROTOCOL']}://{self.settings.config['HOST']}/#/records/detail/" + self.record_id
                    child_anwers[self.rondin_keys['registro_padre']] = parent_record
                    new_metadata['answers'] = child_anwers
                    res = self.lkf_api.post_forms_answers(new_metadata)
        else:
            location = self.answers.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.Location.f['location'])
            roles_raw = self.answers.get(self.f['grupo_roles'], [])
            rol = [
                r.get(self.ROL_CATALOG_OBJ_ID, {}).get(self.f['rol'])
                for r in roles_raw
                if r.get(self.ROL_CATALOG_OBJ_ID, {}).get(self.f['rol'])
            ]
            user_info = self.get_active_guards_in_location(location, rol=rol)
            if not user_info:
                print(f'Warning: no hay guardia activo en location={location!r} roles={rol!r}')
                return False
            
            self.answers[self.USUARIOS_OBJ_ID] = {
                self.mf['nombre_usuario']: user_info.get('created_by_name', ''),
                self.mf['id_usuario']: [user_info.get('created_by_id', '')],
                self.mf['email_visita_a']: [user_info.get('created_by_email', '')],
            }
        return True

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    
    #! Validacion para answers vacio
    if not acceso_obj.answers:
        acceso_obj.answers = acceso_obj.current_record.get('answers', {})
    
    #-FILTROS
    if acceso_obj.current_record.get('_id'):
        if isinstance(acceso_obj.current_record.get('_id'), dict):
            acceso_obj.record_id = acceso_obj.current_record.get('_id').get('$oid')
        else:
            acceso_obj.record_id = acceso_obj.current_record.get('_id')
    else:
        acceso_obj.record_id = str(ObjectId())
    acceso_obj.calcluta_tiempo_traslados()

    # Revisa si este registro es creado por un registro padre
    # Si es creado por un registro padre, no asinga registro ni busca areas, es una copia
    is_child_record = acceso_obj.answers.get(acceso_obj.rondin_keys['registro_padre'])
    if not is_child_record:
        # Si no es un registro hijo, es un registro padre, lo que quiere decir que es el orginal
        if acceso_obj.answers.get(acceso_obj.mf['estatus_del_recorrido']) == 'programado':
            if not acceso_obj.answers.get(acceso_obj.f['areas_del_rondin']):
                try:
                    acceso_obj.get_and_set_areas_recorrido()
                except Exception:
                    print('ERROR get_and_set_areas_recorrido:', traceback.format_exc())
            if not acceso_obj.answers.get(acceso_obj.USUARIOS_OBJ_ID):
                try:
                    acceso_obj.get_and_set_user()
                except Exception:
                    print('ERROR get_and_set_user:', traceback.format_exc())
    print('answers=', simplejson.dumps(acceso_obj.answers, indent=3))
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': acceso_obj.answers,
        "metadata":{
                'id': acceso_obj.record_id
            }

    }))

