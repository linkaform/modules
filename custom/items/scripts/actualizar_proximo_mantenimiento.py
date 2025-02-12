# -*- coding: utf-8 -*-
import sys, simplejson, copy
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from custom_utils import Custom

from account_settings import *

class Custom(Custom):
    
    def actualizar_proximo_mantenimiento(self, equipo, fecha):
        serie = equipo[self.CATALOGO_EQUIPOS_OBJ_ID][self.f['serie_orden_trabajo']]
        frecuencia = self.unlist(equipo[self.CATALOGO_EQUIPOS_OBJ_ID][self.f['frecuencia_mantenimniento']])
        equipo = self.get_equipo(serie)
        equipo_id = equipo['_id']
        frecuencia_obj = frecuencia.split(' ')
        frecuencia_obj = frecuencia_obj[0]
        frecuencia_obj = int(frecuencia_obj)
        fecha_n = datetime.strptime(fecha, '%Y-%m-%d')
        fecha_n = fecha_n.date()
        print('fecha_n', fecha_n)
        fecha_proximo_mantenimiento = fecha_n + relativedelta(months=frecuencia_obj)
        print('***fecha_proximo_mantenimiento', fecha_proximo_mantenimiento)
        answers = {
            self.f['fecha_ultimo_mantenimiento']: fecha_n.strftime('%Y-%m-%d'),
            self.f['fecha_proximo_mantenimiento']: fecha_proximo_mantenimiento.strftime('%Y-%m-%d'),
        }
        result = self.lkf_api.patch_multi_record(answers, self.INVENTARIO_DE_EQUIPOS, record_id=[equipo_id, ])
    
    
        
    def get_equipo(self, serie):
        match_query ={ 
         'form_id': self.INVENTARIO_DE_EQUIPOS,  
         'deleted_at' : {'$exists':False},
         f'answers.{self.f["serie_orden_trabajo"]}': serie
         }
        equipo = self.format_cr(self.cr.find(match_query), get_one=True)
        return equipo
    
 

if __name__ == '__main__':
    #print("custom_scripts", 1)
    module_obj = Custom(settings, sys_argv=sys.argv, use_api=True)
    module_obj.console_run()

    #print("custom_scripts", 2)
    # fecha = module_obj.answers[module_obj.f['fecha_orden_trabajo']]
    if module_obj.form_id == module_obj.ORDEN_DE_TRABAJO:
        fecha = module_obj.answers[module_obj.f['fecha_orden_trabajo']]
        equipos = module_obj.answers[module_obj.f['grupo_equipos_orden_trabajo']]
    else:
        #### de mi form_id obtenido ahora quiero sacarle el campo Fecha del servicio
        #### y luego de eso quiero que me actualice el campo Fecha del servicio
        fecha = module_obj.answers[module_obj.f['fecha_servicio']]
        equipos = [module_obj.answers, ]
    
    for equipo in equipos:
        module_obj.actualizar_proximo_mantenimiento(equipo, fecha)