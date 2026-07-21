import sys, simplejson
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

def validar_mejora_area():
    utc_now = datetime.now(timezone.utc)
    cdmx_now = utc_now.astimezone(ZoneInfo("America/Mexico_City"))
    num_day = cdmx_now.day

    print('+++ +++ utc_now =',utc_now)
    print('+++ +++ cdmx_now =',cdmx_now)

    if num_day > 25:
        msg_error_app = {
            "folio":{
                "msg": ["No se pueden crear registros después del 25 de cada mes."],
                "label": "Folio",
                "error":[]
            }
        }
        raise Exception(simplejson.dumps(msg_error_app))

if __name__ == '__main__':
    validar_mejora_area()