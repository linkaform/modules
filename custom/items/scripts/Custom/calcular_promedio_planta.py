# -*- coding: utf-8 -*-
import sys, simplejson
from custom_utils import Custom
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.field_grp_areas = '69b1904d8e72a1e497e828cd'
        self.field_calificacion = '69f144001bcc2d93430cbebc'
        self.field_promedio = '6a3c3181d90132cc79655770'

    def calcular_promedio(self):
        """
        Calcula el promedio de las calificaciones registradas en las áreas
        contenidas en field_grp_areas.
        """
        areas = self.answers.get(self.field_grp_areas, [])

        calificaciones = [
            area.get(self.field_calificacion)
            for area in areas
            if area.get(self.field_calificacion) 
        ]

        promedio = round(
            sum(calificaciones) / len(calificaciones),
            2
        ) if calificaciones else 0

        self.answers[self.field_promedio] = promedio

        sys.stdout.write(simplejson.dumps({
            'status': 101,
            'replace_ans': self.answers
        }))

if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()
    lkf_obj.calcular_promedio()