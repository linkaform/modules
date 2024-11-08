# -*- coding: utf-8 -*-
import sys, simplejson, math
import json
import math
from datetime import timedelta, datetime

from linkaform_api import settings, base
from account_settings import *

print('inicia....')
#Se agrega path para que obtenga el archivo de Stock de este modulo
sys.path.append('/srv/scripts/addons/modules/accesos/items/scripts/Accesos')
from accesos_utils import Accesos

if __name__ == "__main__":
    acc_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    acc_obj.console_run()

    data = acc_obj.data
    data = data.get('data',[])
    location = data.get('location', 'Planta Monterrey')
    
    option = data.get('option','get_report')

    if option == 'get_report':
        records = acc_obj.get_cantidades_de_pases()
        todos_los_pases = acc_obj.format_cantidades_de_pases(records)
        todos_los_pases_x_contratista = acc_obj.get_pases_x_contratista('Pelotas Dragón',records)
        todos_los_pases_x_perfil = acc_obj.get_pases_x_perfil(records)
        todos_los_pases_x_persona = acc_obj.get_pases_x_persona(records)

        print('records////////', records)
        print('lista de pases//////////', todos_los_pases)
        print('lista de pases x contratista/////////', todos_los_pases_x_contratista)
        print('lista de pases x perfil/////////', todos_los_pases_x_perfil)
        print('lista de pases x persona/////////', todos_los_pases_x_persona)
        
        # script_obj.HttpResponse({
        #     "": "",
        # })


    elif option == 'get_catalog':
        pass
        # script_obj.HttpResponse({
        #     "" : ""
        # })