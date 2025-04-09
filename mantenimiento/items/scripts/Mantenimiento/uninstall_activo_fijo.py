# coding: utf-8
import sys, simplejson, json, pytz
from linkaform_api import settings
from account_settings import *
from datetime import datetime

from mantenimiento_utils import Mantenimiento

class Mantenimiento(Mantenimiento):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.load(module='Activo_Fijo', module_class='Vehiculo', import_as='ActivoFijo', **self.kwargs)
        self.load(module='Employee', **self.kwargs)

    def search_activo_fijo(self, answers):
        nombre_equipo = answers.get(self.ActivoFijo.ACTIVOS_FIJOS_CAT_OBJ_ID, {}).get(self.ActivoFijo.f['modelo'], '')

        match_query = {
            "deleted_at": {"$exists": False},
            "form_id": 130519,
            f"answers.{self.ActivoFijo.f['nombre_equipo']}": nombre_equipo
        }

        query = [
            {'$match': match_query},
        ]

        activo_fijos = self.format_cr(self.cr.aggregate(query))
        if activo_fijos:
            activo_fijo = activo_fijos[0]
            for activo in activo_fijos:
                print(activo)
            return True, activo_fijo
        
    def edit_activo_fijo(self, data):
        folio = data.get('folio')
        answers = {}

        answers[self.CLIENTE_CAT_OBJ_ID] = {
            self.f['nombre_comercial']: '',
            self.f['email']: []
        }
        answers[self.CONTACTO_CAT_OBJ_ID] = {
            self.f['address_name']: '',
            self.f['address_geolocation']: [],
            self.f['phone']: None,
            self.f['email']: []
        }
        answers[self.ActivoFijo.f['estatus']] = 'disponible'
        answers[self.ActivoFijo.f['estado']] = 'no_operativo'
        answers[self.Employee.CONF_AREA_EMPLEADOS_CAT_OBJ_ID] = {
            self.Employee.f['nombre_empleado']: ''
        }

        if answers or folio:
            print('Desintalando activo fijo...')
            res = self.lkf_api.patch_multi_record( answers = answers, form_id=130519, folios=[folio])
            print(res)

if __name__ == "__main__":
    mantenimiento_obj = Mantenimiento(settings, sys_argv=sys.argv)
    mantenimiento_obj.console_run()

    exists, data = mantenimiento_obj.search_activo_fijo(mantenimiento_obj.answers)
    if exists:
        print('Activo Fijo existe...')
        mantenimiento_obj.edit_activo_fijo(data)
    else:
        print('Activo Fijo no existe...')

    # sys.stdout.write(simplejson.dumps({
    #     'status': 101,
    #     'replace_ans': replace_answers
    # }))