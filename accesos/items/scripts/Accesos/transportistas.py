# coding: utf-8
import dis
import sys, simplejson
from bson import ObjectId
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def get_andenes(self):
        query = [
            {'$match': {
                'form_id': self.AREAS_DE_LAS_UBICACIONES,
                'deleted_at': {'$exists': False},
                f'answers.{self.Location.TIPO_AREA_OBJ_ID}.{self.f["tipo_de_area"]}': 'Andén',
            }},
            {'$project': {
                '_id': 0,
                'area': f'$answers.{self.f["area"]}',
            }},
            {'$sort': {'area': 1}},
        ]
        resultado = self.format_cr(self.cr.aggregate(query))
        return [r['area'] for r in resultado if r.get('area')]

    def get_horarios_data(self, dia=None):
        """
        Devuelve la concurrencia por hora del día para graficar horarios de mayor
        afluencia, similar al gráfico de Google Maps.

        Args:
            dia: 0=lunes … 6=domingo. None = todos los días acumulados.

        Returns:
            list de dicts con 'hora' (str HH:00) y 'count' (int),
            cubriendo el rango 06:00–21:00.

        Nota: cada pase cuenta en todas las horas que abarca su rango
        hora_inicial→hora_final (excluyendo la hora de salida).
        """
        f = self.pass_fields_transportista

        from datetime import date
        hoy = date.today()
        cuatrimestre = (hoy.month - 1) // 4
        mes_inicio = cuatrimestre * 4 + 1
        mes_fin = mes_inicio + 3
        fecha_inicio = f'{hoy.year}-{mes_inicio:02d}-01'
        fecha_fin = f'{hoy.year}-{mes_fin:02d}-31'

        match_query = {
            'form_id': self.PASE_ENTRADA_TRANSPORTISTA,
            'deleted_at': {'$exists': False},
            f'answers.{f["fecha_pase_transportista_desde"]}': {'$lte': fecha_fin},
            '$or': [
                {f'answers.{f["fecha_pase_transportista_hasta"]}': {'$gte': fecha_inicio}},
                {f'answers.{f["fecha_pase_transportista_hasta"]}': {'$exists': False}},
                {f'answers.{f["fecha_pase_transportista_hasta"]}': ''},
            ],
        }

        res = self.cr.find(match_query, {
            'hora_inicial': f'$answers.{f["hora_inicial"]}',
            'hora_final':   f'$answers.{f["hora_final"]}',
            'fecha_desde':  f'$answers.{f["fecha_pase_transportista_desde"]}',
            'fecha_hasta':  f'$answers.{f["fecha_pase_transportista_hasta"]}',
        })

        DIAS_SEMANA = ['lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo']
        conteo = {h: 0 for h in range(24)}

        for record in self.format_cr(res):
            if dia is not None:
                from datetime import datetime, timedelta
                fecha_desde_str = record.get('fecha_desde', '')
                fecha_hasta_str = record.get('fecha_hasta', '') or fecha_desde_str
                try:
                    d_ini = datetime.strptime(fecha_desde_str[:10], '%Y-%m-%d').date()
                    d_fin = datetime.strptime(fecha_hasta_str[:10], '%Y-%m-%d').date()
                    dias_rango = (d_fin - d_ini).days + 1
                    dia_en_rango = any(
                        (d_ini + timedelta(days=i)).weekday() == dia
                        for i in range(min(dias_rango, 7))
                    )
                    if not dia_en_rango:
                        continue
                except (ValueError, AttributeError):
                    continue

            hora_ini_str = record.get('hora_inicial', '')
            hora_fin_str = record.get('hora_final', '')
            if not hora_ini_str or not hora_fin_str:
                continue

            try:
                h_ini = int(hora_ini_str.split(':')[0])
                h_fin = int(hora_fin_str.split(':')[0])
                for h in range(h_ini, h_fin):
                    conteo[h] += 1
            except (ValueError, AttributeError):
                continue

        resultado = [
            {'hora': f'{h:02d}:00', 'count': conteo[h]}
            for h in range(0, 24)
        ]

        max_count = max((h['count'] for h in resultado), default=1) or 1
        for h in resultado:
            nivel = round(h['count'] / max_count * 100)
            if nivel == 0:
                h['nivel'] = 'sin_concurrencia'
            elif nivel <= 33:
                h['nivel'] = 'poco_concurrido'
            elif nivel <= 66:
                h['nivel'] = 'concurrido'
            else:
                h['nivel'] = 'muy_concurrido'

        dia_label = DIAS_SEMANA[dia] if dia is not None else 'todos'
        return {'dia': dia_label, 'horarios': resultado}

    def get_pass_transportista(self, record_id=None, token=None):
        f = self.pass_fields_transportista
        match = {
            'form_id': self.PASE_ENTRADA_TRANSPORTISTA,
            'deleted_at': {'$exists': False},
        }
        if record_id:
            match['_id'] = ObjectId(record_id)
        elif token:
            match[f'answers.{f["token_transportista"]}'] = token
        else:
            self.LKFException({'title': 'Se requiere record_id o token', 'status_code': 400})
        query = [
            {'$match': match},
            {'$project': {
                '_id': 1,
                'folio': '$folio',
                'tipo_de_operacion': f'$answers.{f["tipo_de_operacion"]}',
                'ubicacion':  f'$answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.mf["ubicacion"]}',
                'material':   f'$answers.{f["material"]}',
                'cantidad':   f'$answers.{f["cantidad"]}',
                'fecha_desde':  f'$answers.{f["fecha_pase_transportista_desde"]}',
                'fecha_hasta':  f'$answers.{f["fecha_pase_transportista_hasta"]}',
                'hora_inicial': f'$answers.{f["hora_inicial"]}',
                'hora_final':   f'$answers.{f["hora_final"]}',
                'anden':      f'$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf["nombre_area"]}',
                'direccion':  f'$answers.{f["direccion_de_recoleccion"]}',
                'documentos': f'$answers.{f["documentos_para_ocr"]}',
                'qr':         f'$answers.{f["qr_del_pase_transportista"]}',
                'estado_transportista':         f'$answers.{f["estado_transportista"]}',
            }},
        ]
        return self.format_cr(self.cr.aggregate(query), get_one=True)

    def generate_submit_token_transportista(self, record_id):
        f = self.pass_fields_transportista
        token = str(ObjectId())
        answers = {f['token_transportista']: token}
        res = self.lkf_api.patch_multi_record(
            answers=answers,
            form_id=self.PASE_ENTRADA_TRANSPORTISTA,
            record_id=[record_id],
        )
        if res.get('status_code') not in [201, 202]:
            self.LKFException({'title': 'Error al generar token transportista', 'msg': res})
        return {'token': token, 'record_id': record_id}

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv)
    script_obj.console_run()
    data = script_obj.data.get('data', {})
    option = data.get("option", '')
    payload = data.get("payload", {})
    record_id = data.get("record_id", None)
    token = data.get("token", None)

    dispatcher = {
        "create_pass_transportista": lambda: script_obj.create_pass_transportista(payload),
        "get_andenes": lambda: script_obj.get_andenes(),
        "get_horarios_data": lambda: script_obj.get_horarios_data(dia=data.get('dia')),
        "get_pass_transportista": lambda: script_obj.get_pass_transportista(record_id, token),
        "generate_submit_token_transportista": lambda: script_obj.generate_submit_token_transportista(record_id),
    }

    action = dispatcher.get(option)
    if action:
        response = action()
        print(simplejson.dumps(response, indent=4))
    else:
        response = {"error": "Opción no válida"}

    script_obj.HttpResponse({"data": response})