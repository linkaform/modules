# coding: utf-8
import datetime

import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos


class Accesos(Accesos):
    
    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.f.update({
            'porcentaje_obtenido_bitacora': '689a7ecfbf2b4be31039388e',
            'cantidad_areas_inspeccionadas': '68a7b68a22ac030a67b7f8f8'
        })

    def calcluta_tiempo_traslados(self):
        fecha_inicio = self.answers[self.f['fecha_inicio_rondin']]
        fecha_inicio = self.date_2_epoch(self.answers[self.f['fecha_inicio_rondin']])
        areas_visitadas = self.answers[self.f['grupo_areas_visitadas']]
        fecha_inspeccion = 0
        duracion_total = 0
        cantidad_de_inspecciones = 0
        areas_procesadas = False
        fecha_final = None
        for area in areas_visitadas:
            fecha_inspeccion = area.get(self.f['fecha_inspeccion_area'])
            fecha_inspeccion = self.date_2_epoch(fecha_inspeccion)
            if not fecha_inspeccion:
                continue
            fecha_final = fecha_inspeccion
            cantidad_de_inspecciones += 1
            areas_procesadas = True
            duracion = fecha_inspeccion - fecha_inicio
            area[self.f['duracion_traslado_area']] = round(duracion / 60,2)
            duracion_total = duracion
        if areas_procesadas:
            self.answers[self.f['duracion_rondin']] = round(duracion_total / 60,2)
            self.answers[self.f['porcentaje_obtenido_bitacora']] = str(round((cantidad_de_inspecciones / len(areas_visitadas)) * 100, 2)) + '%'
            self.answers[self.f['cantidad_areas_inspeccionadas']] = str(cantidad_de_inspecciones) + '/' + str(len(areas_visitadas))
        fecha_final_str = datetime.datetime.fromtimestamp(fecha_final).strftime('%Y-%m-%d %H:%M:%S') if fecha_final else ''
        if self.answers.get(self.f['estatus_del_recorrido']) in ['realizado', 'cerrado'] and fecha_final_str:
            self.answers[self.f['fecha_fin_rondin']] = fecha_final_str
        return True

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    acceso_obj.calcluta_tiempo_traslados()
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': acceso_obj.answers
    }))

