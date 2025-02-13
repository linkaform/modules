# -*- coding: utf-8 -*-
import sys, simplejson, copy
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from custom_utils import Custom

from account_settings import *

class Custom(Custom):
    
    def actualizar_proximo_mantenimiento(self, equipo, cliente_destino):
        
        serie = equipo[self.CATALOGO_EQUIPOS_OBJ_ID][self.f['serie_orden_trabajo']]
        cliente_destino = cliente_destino[self.f['cliente_destino_name']]
        print('cliente_a', cliente_destino)
        equipo = self.get_equipo(serie)
        equipo_id = equipo['_id']
        print('equipo3', equipo)
        # frecuencia_obj = frecuencia.split(' ')
        # frecuencia_obj = frecuencia_obj[0]
        # frecuencia_obj = int(frecuencia_obj)
        # fecha_n = datetime.strptime(fecha, '%Y-%m-%d')
        # fecha_n = fecha_n.date()
        # fecha_proximo_mantenimiento = fecha_n + relativedelta(months=frecuencia_obj)
        answers = {
             self.f['cliente_origen']: { 
                 self.f['cliente_origen_name']: cliente_destino,
             },
        #     self.f['fecha_proximo_mantenimiento']: fecha_proximo_mantenimiento.strftime('%Y-%m-%d'),
        }
        result = self.lkf_api.patch_multi_record(answers, self.INVENTARIO_DE_EQUIPOS, record_id=[equipo_id, ])
    
if __name__ == '__main__':
    #print("custom_scripts", 1)
    module_obj = Custom(settings, sys_argv=sys.argv, use_api=True)
    module_obj.console_run()

    #print("custom_scripts", 2)
    equipos = module_obj.answers[module_obj.f['grupo_movimiento_equipos']]
    cliente_destino = module_obj.answers[module_obj.f['cliente_destino']]
    
    for equipo in equipos:
        module_obj.actualizar_proximo_mantenimiento(equipo, cliente_destino)