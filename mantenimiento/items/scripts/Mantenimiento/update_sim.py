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

        self.f.update({
            "nueva_sim": "67f9346f8d6298ee9c6792de",
        })

    def search_activo_fijo(self, name=''):
        match_query = {
            "deleted_at": {"$exists": False},
            "form_id": self.ActivoFijo.ACTIVOS_FIJOS,
            f"answers.{self.ActivoFijo.f['nombre_equipo']}": name
        }

        proyect_fields = {
            '_id': 1,
            'folio': '$folio',
        }

        query = [
            {'$match': match_query},
            {'$project': proyect_fields},
        ]

        activo_fijos = self.format_cr(self.cr.aggregate(query))
        folio = ''
        if activo_fijos:
            folio = activo_fijos[0].get('folio', '') if activo_fijos else None
        return folio


    def update_sim(self, info={}, folio=[]):
        answers = {}
        answers[self.f['activo_sim']] = info.get(self.f['nueva_sim'])

        status_code = 404
        if answers or folio:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.ActivoFijo.ACTIVOS_FIJOS, folios=[folio])
            status_code = res.get('status_code')

        return status_code

if __name__ == "__main__":
    mantenimiento_obj = Mantenimiento(settings, sys_argv=sys.argv)
    mantenimiento_obj.console_run()

    nombre_equipo = mantenimiento_obj.answers.get(mantenimiento_obj.ActivoFijo.ACTIVOS_FIJOS_CAT_OBJ_ID, {}).get(mantenimiento_obj.ActivoFijo.f['modelo'], '')
    folio = mantenimiento_obj.search_activo_fijo(nombre_equipo)
    if folio:
        status_code = mantenimiento_obj.update_sim(info=mantenimiento_obj.answers, folio=folio)
    else:
        print('No se encontró el activo fijo con el nombre proporcionado.')
        status_code = 404

    sys.stdout.write(simplejson.dumps({
        'status': status_code,
        'message': 'Sim actualizado correctamente' if status_code == 202 else 'Error al actualizar la sim',
    }))