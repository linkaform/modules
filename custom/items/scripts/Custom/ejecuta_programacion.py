# -*- coding: utf-8 -*-
import sys, simplejson
from custom_utils import Custom
from datetime import datetime, timedelta
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.FORM_ID_PROGRAMACION = 150324
        self.field_anio = "69dfc83706c1197cec034b39"
        self.field_mes = "69d82f9651b77ef10d63b785"
        self.field_semana = "69d82edb3203a903fb3fd9fd"

        self.field_planta = "696130ce57ba2b8308adef4d"
        self.field_area = "696133f1829d117f5e819e8d"
        self.field_responsable = "638a9a7767c332f5d459fc81"
        self.field_grupo_areas = "69dfc84f6748944372b3d533"

    def calcular_semana(self, f):
        """
        Calcula la semana del mes para una fecha dada, considerando que la semana 1 inicia en el primer lunes del mes.
        Retorna 0 si la fecha cae antes del primer lunes.
        """
        primer_dia = f.replace(day=1)
        dias_hasta_lunes = (7 - primer_dia.weekday()) % 7
        primer_lunes = primer_dia + timedelta(days=dias_hasta_lunes)
        return 0 if f < primer_lunes else ((f - primer_lunes).days // 7) + 1

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

        semana = self.calcular_semana(fecha)

        # Si es semana 0 ir al mes anterior
        if semana == 0:
            ultimo_dia_mes_anterior = fecha.replace(day=1) - timedelta(days=1)
            return self.semana_del_mes_lunes(ultimo_dia_mes_anterior)

        return {
            "anio": fecha.year,
            "mes": name_month[ fecha.month - 1 ],
            "semana": semana
        }

    def get_records_programacion(self, data_fecha):
        query = {
            'form_id': self.FORM_ID_PROGRAMACION,
            'deleted_at': {'$exists': False},
            f'answers.{self.field_anio}': str( data_fecha['anio'] ),
            f'answers.{self.field_mes}': data_fecha['mes'],
            f'answers.{self.field_semana}': f"semana_{data_fecha['semana']}",
        }
        print(f"\n query programacion = {simplejson.dumps(query, indent=2)} \n")

    def ejecuta_programacion(self):
        # Se obtienen los datos de la fecha actual. anio, mes y semana
        data_fecha = self.semana_del_mes_lunes()
        print('++ data_fecha =', data_fecha)

        # Se consultan los registros de programacion
        self.get_records_programacion(data_fecha)

if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    answers = lkf_obj.answers

    lkf_obj.ejecuta_programacion()