# coding: utf-8
#####
# Script para establecer la hora de finalización del servicio
# Forma: Orden de Instalación
#####
import sys, simplejson, json, pytz
from linkaform_api import settings
from account_settings import *
from datetime import datetime

from mantenimiento_utils import Mantenimiento

class Mantenimiento(Mantenimiento):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
    
    def update_end_date(self, answers):
        fecha_inicio_str = answers.get(self.f['hora_inicio_instalacion'])
        if fecha_inicio_str:
            fecha_inicio_str = fecha_inicio_str.replace('/', '-')
        fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d %H:%M:%S")
        print(f"Fecha inicio: {fecha_inicio}")

        timezone = pytz.timezone("America/Mexico_City")
        fecha_inicio = timezone.localize(fecha_inicio)

        fecha_actual = datetime.now(timezone)
        fecha_finalizacion = fecha_actual.strftime("%Y-%m-%d %H:%M:%S")
        print(f"Fecha finalizacion: {fecha_finalizacion}")

        diferencia_minutos = (fecha_actual - fecha_inicio).total_seconds() / 60
        diferencia_minutos = round(diferencia_minutos)

        answers[self.f['hora_final_instalacion']] = fecha_finalizacion
        answers[self.f['total_minutos_instalacion']] = diferencia_minutos

        return answers

if __name__ == "__main__":
    mantenimiento_obj = Mantenimiento(settings, sys_argv=sys.argv)
    mantenimiento_obj.console_run()
    
    replace_answers = mantenimiento_obj.update_end_date(mantenimiento_obj.answers)

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': replace_answers
    }))