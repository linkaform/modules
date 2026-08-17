# -*- coding: utf-8 -*-
import sys, simplejson, json
from custom_utils import Custom
from datetime import datetime, timedelta
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.host_lkf = "https://app.linkaform.com"
        self.script_id_delete_inbox = 145572
        self.map_username_id = self.get_id_user_catalog()

    def get_id_user_catalog(self):
        # Consultar el catalogo de usuarios
        records_catalog = self.lkf_api.search_catalog(self.CATALOG_ID_USUARIOS_INVITADOS)
        
        return {
            rec.get('69df18efff8ef34560975100'): rec.get('69df18efff8ef345609750fe')
            for rec in records_catalog
        }

    def calcular_semana(self, f):
        """
        Calcula la semana del mes para una fecha dada.
        """
        primer_dia = f.replace(day=1)

        dias_hasta_lunes = (7 - primer_dia.weekday()) % 7
        primer_lunes = primer_dia + timedelta(days=dias_hasta_lunes)

        # Antes del primer lunes
        if f < primer_lunes:
            return 0, None, None, None

        numero_semana = ((f - primer_lunes).days // 7) + 1

        fecha_inicio = primer_lunes + timedelta(days=(numero_semana - 1) * 7)
        fecha_fin = fecha_inicio + timedelta(days=6)
        fecha_semana_anterior = fecha_inicio - timedelta(days=7)

        return numero_semana, fecha_inicio, fecha_fin, fecha_semana_anterior

    def semana_del_mes_lunes(self, fecha=None):
        """
        Calcula el año, mes y número de semana de una fecha bajo las siguientes reglas:
        1. Las semanas comienzan en lunes.
        2. La semana 1 es el primer lunes del mes.
        3. Si una fecha cae antes del primer lunes del mes, se considera parte
           de la última semana del mes anterior.
        """
        if fecha is None:
            fecha = datetime.today()
        
        name_month = [
            'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 
            'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
        ]

        semana, fecha_inicio, fecha_fin, fecha_semana_anterior = self.calcular_semana(fecha)

        # Si es semana 0 ir al mes anterior
        if semana == 0:
            ultimo_dia_mes_anterior = fecha.replace(day=1) - timedelta(days=1)
            return self.semana_del_mes_lunes(ultimo_dia_mes_anterior)

        return {
            "anio": fecha.year,
            "mes": name_month[ fecha.month - 1 ],
            "semana": semana,
            "fecha_inicio": f"{fecha_inicio.strftime('%Y-%m-%d')} 00:00:00",
            "fecha_fin": f"{fecha_fin.strftime('%Y-%m-%d')} 23:59:59",
            "fecha_semana_anterior": f"{fecha_semana_anterior.strftime('%Y-%m-%d')} 00:00:00",
        }

    def get_records_programacion(self, data_fecha):

        # Semana 1 nomas para mis pruebas
        # data_fecha['semana'] = '1'

        query = {
            'form_id': self.FORM_ID_PROGRAMACION,
            'deleted_at': {'$exists': False},
            f'answers.{self.field_anio}': str( data_fecha['anio'] ),
            f'answers.{self.field_mes}': data_fecha['mes'],
            f'answers.{self.field_semana}': f"semana_{data_fecha['semana']}",
        }
        print(f"\n query programacion = {simplejson.dumps(query, indent=2)} \n")

        records_programacion = lkf_obj.cr.aggregate([
            {"$match": query},
            {"$project": {
                "planta": f"$answers.696130ce57ba2b8308adef4c.{self.field_planta}",
                "areas_programar": f"$answers.{self.field_grupo_areas}"
            }},
            {"$unwind": "$areas_programar"},
            {"$project": {
                "planta": "$planta",
                "area": f"$areas_programar.{self.obj_plantas_areas}.{self.field_area}",
                "email_jefe_area": {
                    "$arrayElemAt": [f"$areas_programar.{self.obj_plantas_areas}.{self.field_email_jefe_area}", 0]
                },
                "usuario_a_asignar_nombre": f"$areas_programar.{self.obj_usuarios}.{self.field_responsable}",
                "usuario_a_asignar_username": {
                    "$arrayElemAt": [f"$areas_programar.{self.obj_usuarios}.{self.field_username}", 0]
                }
            }}
        ])

        # print('+++ records_programacion =',list(records_programacion))

        return records_programacion

    def get_device_properties(self):
        return {
            "device_properties": {
                "system": "SCRIPT",
                "process": "Ejecutar programacion", 
                "accion": "Crear registros de Recorrido",
                "archive": "ejecuta_programacion.py"
            }
        }

    def similar_fields(self, data_fecha, data_programacion):
        username_asignacion = data_programacion.get('usuario_a_asignar_username')
        id_user_asignacion = self.map_username_id.get(username_asignacion)
        # print(f'username_asignacion = {username_asignacion} id_user_asignacion = {id_user_asignacion}')

        data_user_asign = {
            self.field_responsable: data_programacion.get('usuario_a_asignar_nombre'),
            self.field_email: [username_asignacion],
            self.field_username: [username_asignacion],
        }

        if id_user_asignacion:
            data_user_asign[ self.field_user_id ] = [id_user_asignacion]

        # print(simplejson.dumps(data_user_asign, indent=4))

        return {
            self.obj_plantas_areas: {
                self.field_planta: data_programacion.get('planta'),
                self.field_area: data_programacion.get('area'),
                self.field_email_jefe_area: [data_programacion.get('email_jefe_area')],
            },
            self.obj_usuarios: data_user_asign,
            "abcde0001000000000000020": "programado",
            "fffff0001000000000000001": data_fecha.get('fecha_inicio'),
            "fffff0001000000000000002": data_fecha.get('fecha_fin'),
        }

    def create_records_recorridos(self, form_recorrido, answers_recorrido):
        metadata = lkf_obj.lkf_api.get_metadata(form_recorrido)
        metadata['properties'] = self.get_device_properties()
        metadata['answers'] = answers_recorrido
        return metadata

        # resp_create = self.lkf_api.post_forms_answers(metadata)
        # print('     - resp_create =',resp_create)

    def create_record_conversion(self, data_programacion, data_fecha):
        answers_recorrido_conversion = self.similar_fields(data_fecha, data_programacion)
        return self.create_records_recorridos(self.FORM_ID_CONVERSION, answers_recorrido_conversion)

    def create_record_molino(self, data_programacion, data_fecha):
        answers_recorrido_molino = self.similar_fields(data_fecha, data_programacion)
        # answers_recorrido_molino[ self.obj_usuarios ].pop( self.field_username, None )
        return self.create_records_recorridos(self.FORM_ID_MOLINOS, answers_recorrido_molino)

    def delete_record_from_inbox(self, records_delete_inbox):
        cr_couch = self.lkf_api.couch
        for user_id, list_records in records_delete_inbox.items():
            print(f'... Borrando inbox user= {user_id} records= {list_records}',)
            cr_db = cr_couch.set_db(f'user_inbox_{user_id}')
            mango_query = {
                "selector": {"record_json._id": {"$in": list_records}},
                "limit":20,"skip":0
            }
            records = cr_db.find(mango_query)

            records_inbox = [rec_inbox for rec_inbox in records]
            if not records_inbox:
                continue

            # print('records_inbox =',records_inbox)
            res = self.lkf_api.delete_users_inbox(user_id, records_inbox, threading=False)
            print(f"-- -- -- resp_delete_inbox = {res}")

    def get_records_to_unbox(self, fecha_inicio, fecha_semana_anterior):
        print(f'buscando registros desde {fecha_semana_anterior} hasta {fecha_inicio}')
        records_to_outbox = self.get_records(
            form_id=[self.FORM_ID_CONVERSION, self.FORM_ID_MOLINOS],
            query_answers={
                "answers.fffff0001000000000000002": {
                    "$gte": fecha_semana_anterior,
                    "$lt": fecha_inicio
                }
                ,"properties.device_properties.archive": "ejecuta_programacion.py" # esto para mis pruebas
            },
            select_columns=['folio', '_id', 'user_id']
        )

        group_inbox = {}
        for rec in records_to_outbox:
            group_inbox.setdefault( rec['user_id'], [] ).append( str(rec['_id']) )
        return group_inbox

    def get_exists_records(self, data_fecha):
        records_exists = self.get_records(
            form_id=[self.FORM_ID_CONVERSION, self.FORM_ID_MOLINOS],
            query_answers={
                "answers.abcde0001000000000000020": "programado",
                "answers.fffff0001000000000000001": data_fecha.get('fecha_inicio'),
                "answers.fffff0001000000000000002": data_fecha.get('fecha_fin'),
            },
            select_columns=['folio', 'answers']
        )
        map_records_exists = {}
        for rec in records_exists:
            answers_rec = rec.get('answers', {})
            data_planta_area = answers_rec.get(self.obj_plantas_areas, {})
            map_records_exists\
                .setdefault( data_planta_area.get(self.field_planta), {} )\
                .setdefault( data_planta_area.get(self.field_area), [] )\
                .append( answers_rec.get( self.obj_usuarios, {} ).get( self.field_responsable ) )
        return map_records_exists

    def ejecuta_programacion(self):
        # Se obtienen los datos de la fecha actual. anio, mes y semana
        # data_fecha = self.semana_del_mes_lunes( datetime.strptime('2026-07-20', '%Y-%m-%d') ) # PARA MIS PRUEBAS
        data_fecha = self.semana_del_mes_lunes()
        print('++ data_fecha =', simplejson.dumps(data_fecha, indent=4))
        
        # forzando fechas nomas para mis pruebas
        # data_fecha['fecha_inicio'] = "2026-05-04 00:00:00"
        # data_fecha['fecha_semana_anterior'] = "2026-04-27 00:00:00"
        
        # Se borran los registros de Inbox si ya pasó la fecha limite
        records_unboxing = self.get_records_to_unbox(data_fecha['fecha_inicio'], data_fecha['fecha_semana_anterior'])
        # print('records_unboxing =', records_unboxing)
        self.delete_record_from_inbox( records_unboxing )
        
        # Se consultan los registros de programacion
        records_programacion = self.get_records_programacion(data_fecha)

        # Se consultan los registros que ya fueron creados para la misma fecha inicio y fin
        records_recorrido_exists = self.get_exists_records(data_fecha)

        # Se va a crear un registro por cada set del grupo Areas a programar
        list_records_to_create = []
        for programacion in records_programacion:
            _planta = programacion.get('planta')
            _area = programacion.get('area')
            _responsable = programacion.get('usuario_a_asignar_nombre')
            print(f"\n ===== Creando registro Planta: {_planta} Area: {_area} =====")

            # Para no duplicar los registros si el script que llegara a reprocesar hay que validar si ya existe el registro
            if _responsable in records_recorrido_exists.get(_planta, {}).get(_area, []):
                print(f'... ya existe registro para: {_responsable}')
                continue

            if programacion.get('planta') in ('Molino', 'Molino Proyectos'):
                list_records_to_create.append( self.create_record_molino(programacion, data_fecha) )
            else:
                list_records_to_create.append( self.create_record_conversion(programacion, data_fecha) )

        resp_create_all_records = self.lkf_api.post_forms_answers_list(list_records_to_create)
        print('\n   - resp_create_all_records =',resp_create_all_records)


if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    answers = lkf_obj.answers

    lkf_obj.ejecuta_programacion()