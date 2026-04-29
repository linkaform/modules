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
            return 0, None, None

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
        data_fecha['semana'] = '1'

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

    def create_record_convercion(self, data_programacion, data_fecha):
        answers_recorrido = {
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

        metadata = lkf_obj.lkf_api.get_metadata(self.FORM_ID_CONVERSION)
        metadata['properties'] = {
            "device_properties":{
                "system": "SCRIPT",
                "process": "Ejecutar programacion", 
                "accion": "Crear registros de Recorrido",
                "archive": "ejecuta_programacion.py"
            }
        }
        metadata['answers'] = answers_recorrido

        resp_create = self.lkf_api.post_forms_answers(metadata)
        print('     - resp_create =',resp_create)

    def create_record_molino(self, data_programacion):
        answers_recorrido = {
            self.obj_plantas_areas: {
                self.field_planta: data_programacion.get('planta'),
                self.field_area: data_programacion.get('area'),
                self.field_responsable: [data_programacion.get('usuario_a_asignar_nombre')],
                self.field_username: [data_programacion.get('usuario_a_asignar_username')],
            }
        }

    def delete_record_from_inbox(self, str_record_id, user_id_inbox):
        print('... Borrando de inbox =',str_record_id)
        url = self.host_lkf + '/api/infosync/scripts/run/'
        data = json.dumps({
            "script_id": self.script_id_delete_inbox, 
            "record_id": str_record_id, 
            "user_id": user_id_inbox
        })
        resp_run = self.net.do_post(url, data, use_jwt=True, jwt_settings_key='JWT_KEY')
        print('resp delete inbox',resp_run)

    def get_records_to_unbox(self, fecha_inicio, fecha_semana_anterior):
        print(f'buscando registros desde {fecha_semana_anterior} hasta {fecha_inicio}')
        return self.get_records(
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

    def ejecuta_programacion(self):
        # Se obtienen los datos de la fecha actual. anio, mes y semana
        data_fecha = self.semana_del_mes_lunes()
        print('++ data_fecha =', simplejson.dumps(data_fecha, indent=4))
        

        # Se borran los registros de Inbox si ya pasó la fecha limite

        # forzando fechas nomas para mis pruebas
        data_fecha['fecha_inicio'] = "2026-05-04 00:00:00"
        data_fecha['fecha_semana_anterior'] = "2026-04-27 00:00:00"

        records_unboxing = self.get_records_to_unbox(data_fecha['fecha_inicio'], data_fecha['fecha_semana_anterior'])
        # print('records_unboxing =',list(records_unboxing))
        for record_unbox in records_unboxing:
            self.delete_record_from_inbox( str(record_unbox['_id']), record_unbox['user_id'] )


        stop
        


        # Se consultan los registros de programacion
        records_programacion = self.get_records_programacion(data_fecha)

        # Se va a crear un registro por cada set del grupo Areas a programar
        for programacion in records_programacion:
            print(f"\n ===== Creando registro Planta: {programacion.get('planta')} Area: {programacion.get('area')} =====")
            if programacion.get('planta') == 'Molino':
                self.create_record_molino(programacion)
            else:
                self.create_record_convercion(programacion, data_fecha)


if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    answers = lkf_obj.answers

    lkf_obj.ejecuta_programacion()