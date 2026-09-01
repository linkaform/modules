# coding: utf-8
import sys, simplejson, json, pytz
from linkaform_api import settings
from account_settings import *
from datetime import datetime, timedelta

from accesos_utils import Accesos

class Accesos(Accesos):
    
    def get_attendance_records(self, guardias_dentro):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.REGISTRO_ASISTENCIA,
                "created_by_id": {"$in": guardias_dentro},
                f"answers.{self.f['start_shift']}": {"$exists": True},
                f"answers.{self.f['end_shift']}": {"$exists": False}
            }},
            {"$project": {
                "_id": 1,
                "timezone": 1,
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query))
        return data
    
    def do_checkout_all(self):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.CHECKIN_CASETAS,
                f"answers.{self.checkin_fields['checkin_type']}": "abierta"
            }},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "timezone": 1,
                f"{self.checkin_fields['checkin_type']}": f"$answers.{self.checkin_fields['checkin_type']}",
                f"{self.mf['guard_group']}": f"$answers.{self.mf['guard_group']}"
            }}
        ]
        data = list(self.cr.aggregate(query))

        checkin_responses_details = []
        guardias_dentro = []
        for item in data:
            ahora = datetime.now(pytz.timezone(item.get('timezone', 'America/Mexico_City'))).replace(tzinfo=None)
            count_guardias_dentro = 0
            item['_id'] = str(item['_id'])
            registro_de_guardias = item.get(f'{self.mf["guard_group"]}', [])
            format_guardias_dentro = {}
            for index, guardia in enumerate(registro_de_guardias):
                checkin_date = guardia.get(self.checkin_fields['checkin_date'])
                if isinstance(checkin_date, str):
                    checkin_date = datetime.strptime(checkin_date, '%Y-%m-%d %H:%M:%S')
                if checkin_date \
                    and ahora > checkin_date + timedelta(hours=10) \
                    and guardia.get(self.checkin_fields['checkin_status']) == 'entrada':
                    guardias_dentro.append(self.unlist(guardia.get(self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID, {}).get(self.mf['id_usuario'], [])))
                    guardia[self.checkin_fields['checkin_status']] = 'salida'
                    guardia[self.checkin_fields['checkout_date']] = ahora.strftime('%Y-%m-%d %H:%M:%S')
                if guardia.get(self.checkin_fields['checkin_status']) == 'entrada':
                    count_guardias_dentro += 1
                format_guardias_dentro[index] = guardia
            if count_guardias_dentro == 0:
                item[self.checkin_fields['checkin_type']] = 'cerrada'
                item[self.checkin_fields['boot_checkout_date']] = ahora.strftime('%Y-%m-%d %H:%M:%S')

            answers = {}
            answers[self.checkin_fields['checkin_type']] = item.get(self.checkin_fields['checkin_type'])
            answers[self.mf["guard_group"]] = format_guardias_dentro
            folio = item.get('folio')
            if len(guardias_dentro) > 0:
                response = self.lkf_api.patch_multi_record(answers=answers, form_id=self.CHECKIN_CASETAS, folios=[folio,])
                print('=============log:', response)
                checkin_responses_details.append({
                    'response': response,
                    'item': item
                })

        guardias_dentro = list(set(guardias_dentro))
        data = self.get_attendance_records(guardias_dentro)
        asistencia_responses_details = []
        if data:
            for item in data:
                timezone = item.get('timezone', 'America/Mexico_City')
                ahora = datetime.now(pytz.timezone(timezone)).replace(tzinfo=None)
                answers = {
                    self.f['fecha_cierre_turno']: ahora.strftime('%Y-%m-%d %H:%M:%S'),
                    self.f['comment_checkout']: 'Cierre de turno automatico - 10 horas'
                }
                response = self.lkf_api.patch_multi_record(answers=answers, form_id=self.REGISTRO_ASISTENCIA, record_id=[item.get('_id'),])
                print('=============log:', response)
                asistencia_responses_details.append({
                    'response': response,
                    'item': item
                })
        return {
            'checkin_responses_details': checkin_responses_details,
            'asistencia_responses_details': asistencia_responses_details
        }

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    
    response = acceso_obj.do_checkout_all()

    sys.stdout.write(simplejson.dumps({
        'response': response
    }))