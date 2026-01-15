# coding: utf-8
import sys, simplejson
from datetime import datetime, timedelta
import pytz

from accesos_utils import Accesos

from account_settings import *

class Accesos(Accesos):

    def deactivate_passes(self):
        now = datetime.now(pytz.timezone('America/Mexico_City'))
        now_formatted = now.strftime("%Y-%m-%d %H:%M:%S")
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.PASE_ENTRADA,
                f"answers.{self.pase_entrada_fields['status_pase']}": "activo",
            }},
            {"$addFields": {
                "date_from": f"$answers.{self.pase_entrada_fields['fecha_desde_visita']}",
                "date_to": f"$answers.{self.pase_entrada_fields['fecha_hasta_pase']}",
            }},
            {"$addFields": {
                "effective_end_date": {
                    "$cond": {
                        "if": {
                            "$or": [
                                {"$eq": ["$date_to", None]},
                                {"$eq": ["$date_to", ""]},
                                {"$eq": ["$date_to", []]},
                                {"$ne": [{"$type": "$date_to"}, "string"]}
                            ]
                        },
                        "then": "$date_from",
                        "else": "$date_to"
                    }
                }
            }},
            {"$match": {
                "effective_end_date": {"$lt": now_formatted}
            }},
            {"$project": {
                "_id": 1,
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query))
        if data:
            answers = {}
            record_ids = set()
            for d in data:
                record_ids.add(d.get('_id'))
            format_record_ids = list(record_ids)

            answers[self.pase_entrada_fields['status_pase']] = 'vencido'
            print('==============log: Pases vencidos: ', format_record_ids)
            if answers:
                res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.PASE_ENTRADA, record_id=format_record_ids)
                if res.get('status_code') == 201 or res.get('status_code') == 202:
                    return res
                else:
                    return self.LKFException({"title": "Error al actualizar pases", "msg": res})
        else:
            return "No hay pases vencidos"

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()
    
    response = script_obj.deactivate_passes()
    script_obj.HttpResponse({"data": response})