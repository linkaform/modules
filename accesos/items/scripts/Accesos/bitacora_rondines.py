# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos


class Accesos(Accesos):

    def calcluta_tiempo_traslados(self):
        print('calculadndo tiemps')
        print('answesrs', simplejson.dumps(self.answers, indent=3))
        fecha_inicio = self.answers[self.f['fecha_inicio_rondin']]
        print('fecha_inicio=',fecha_inicio)
        fecha_inicio = self.date_2_epoch(self.answers[self.f['fecha_inicio_rondin']])
        print('epoch fecha_inicio=',fecha_inicio)
        areas_visitadas = self.answers[self.f['grupo_areas_visitadas']]
        fecha_inspeccion = 0
        duracion_total = 0
        for area in areas_visitadas:
            print('area', area)
            fecha_inspeccion = area.get(self.f['fecha_inspeccion_area'])
            print('fecha_inspeccion', fecha_inspeccion)
            fecha_inspeccion = self.date_2_epoch(fecha_inspeccion)
            print('epoch fecha_inspeccion', fecha_inspeccion)
            print('area', area)
            if not fecha_inspeccion:
                continue
            duracion = fecha_inspeccion - fecha_inicio
            print('duracion', duracion)
            area[self.f['duracion_traslado_area']] = round(duracion / 60,2)
            duracion_total += duracion
        if fecha_inspeccion:
            print('duracion_total', duracion_total)
            self.answers[self.f['duracion_rondin']] = round(duracion_total / 60,2)
        return True


if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    acceso_obj.calcluta_tiempo_traslados()
    print('answers', acceso_obj.answers)
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': acceso_obj.answers
    }))

