# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos


class Accesos(Accesos):

    def calcluta_tiempo_traslados(self):
        fecha_inicio = self.answers[self.f['fecha_inicio_rondin']]
        fecha_inicio = self.date_2_epoch(self.answers[self.f['fecha_inicio_rondin']])
        areas_visitadas = self.answers[self.f['grupo_areas_visitadas']]
        fecha_inspeccion = 0
        duracion_total = 0
        last_inspection = fecha_inicio
        set_dates = {}
        for idx, area in enumerate(areas_visitadas):
            fecha_inspeccion = self.date_2_epoch(fecha_inspeccion)
            set_dates[idx] = fecha_inspeccion
        print('set dates', set_dates)
        areas_dates = set_dates.values()
        areas_dates.sort()
        print('area dates', areas_dates)
        for secs in areas_dates:
            print('secs', secs)
            duracion = 0
            fecha_inspeccion = area.get(self.f['fecha_inspeccion_area'])
            if fecha_inspeccion:
                if fecha_inspeccion > last_inspection:
                    last_inspection = fecha_inspeccion
                duracion = (fecha_inspeccion - fecha_inicio)/60
            area[self.f['duracion_traslado_area']] = round(duracion)
            duracion_total += duracion
        if last_inspection > fecha_inicio:
            self.answers[self.f['duracion_rondin']] = round((last_inspection - fecha_inicio)/60,2)
        return True


if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    acceso_obj.calcluta_tiempo_traslados()
    # print('answers', acceso_obj.answers)
    # sys.stdout.write(simplejson.dumps({
    #     'status': 101,
    #     'replace_ans': acceso_obj.answers
    # }))

