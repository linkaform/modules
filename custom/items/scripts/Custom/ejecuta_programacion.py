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
        return {
            self.obj_plantas_areas: {
                self.field_planta: data_programacion.get('planta'),
                self.field_area: data_programacion.get('area'),
            },
            self.obj_usuarios: {
                self.field_responsable: data_programacion.get('usuario_a_asignar_nombre'),
                self.field_email: [data_programacion.get('usuario_a_asignar_username')],
                self.field_username: [data_programacion.get('usuario_a_asignar_username')],
            },
            "abcde0001000000000000020": "programado",
            "fffff0001000000000000001": data_fecha.get('fecha_inicio'),
            "fffff0001000000000000002": data_fecha.get('fecha_fin'),
        }

    def create_records_recorridos(self, form_recorrido, answers_recorrido):
        metadata = lkf_obj.lkf_api.get_metadata(form_recorrido)
        metadata['properties'] = self.get_device_properties()
        metadata['answers'] = answers_recorrido

        resp_create = self.lkf_api.post_forms_answers(metadata)
        print('     - resp_create =',resp_create)

    def create_record_conversion(self, data_programacion, data_fecha):
        answers_recorrido_conversion = self.similar_fields(data_fecha, data_programacion)
        self.create_records_recorridos(self.FORM_ID_CONVERSION, answers_recorrido_conversion)

    def create_record_molino(self, data_programacion, data_fecha):
        answers_recorrido_molino = self.similar_fields(data_fecha, data_programacion)
        # answers_recorrido_molino[ self.obj_usuarios ].pop( self.field_username, None )
        self.create_records_recorridos(self.FORM_ID_MOLINOS, answers_recorrido_molino)

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

    def ejecuta_programacion(self):
        # Se obtienen los datos de la fecha actual. anio, mes y semana
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

        # Se va a crear un registro por cada set del grupo Areas a programar
        for programacion in records_programacion:
            print(f"\n ===== Creando registro Planta: {programacion.get('planta')} Area: {programacion.get('area')} =====")
            if programacion.get('planta') == 'Molino':
                self.create_record_molino(programacion, data_fecha)
            else:
                self.create_record_conversion(programacion, data_fecha)


if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    answers = lkf_obj.answers

    lkf_obj.ejecuta_programacion()