# coding: utf-8
import sys, simplejson
from datetime import datetime, timedelta
import pytz

from accesos_utils import Accesos

from account_settings import *

class Accesos(Accesos):

    def descartar_transportistas_vencidos(self):
        """
        Descarta automáticamente los registros de bitácora de transportistas
        que llevan más de 24 horas sin llegar a estatus 'terminado' (y que
        todavía no estén 'descartado') — mismo criterio de antigüedad que el
        badge de horas transcurridas en el kanban (basado en fecha_hora_ingreso).
        """
        f = self.bitacora_transportista_fields
        tz_name = self.user.get('timezone', 'America/Mexico_City')
        limite = datetime.now(pytz.timezone(tz_name)) - timedelta(hours=24)
        limite_formatted = limite.strftime('%Y-%m-%d %H:%M:%S')

        query = [
            {'$match': {
                'deleted_at': {'$exists': False},
                'form_id': self.BITACORA_TRANSPORTISTAS,
                f'answers.{f["estatus"]}': {'$nin': ['terminado', 'descartado', 'programado']},
                f'answers.{f["fecha_hora_ingreso"]}': {'$type': 'string', '$ne': '', '$lt': limite_formatted},
            }},
            {'$project': {
                '_id': 1,
                'folio': 1,
            }},
        ]
        data = self.format_cr(self.cr.aggregate(query))
        if not data:
            return 'No hay registros de bitácora de transportistas para descartar'

        record_ids = list({d.get('_id') for d in data})
        folios = [d.get('folio') for d in data]
        print('==============log: Bitácoras de transportistas descartadas por vencimiento: ', folios)

        answers = {f['estatus']: 'descartado'}
        res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_TRANSPORTISTAS, record_id=record_ids)
        if res.get('status_code') in [201, 202]:
            return res
        else:
            return self.LKFException({'title': 'Error al descartar bitácoras de transportistas vencidas', 'msg': res})


if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()

    response = script_obj.descartar_transportistas_vencidos()
    script_obj.HttpResponse({"data": response})
