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
                'created_at': 1,
                'folio':          '$folio',
                'creado_desde':   f'$answers.{self.pase_entrada_fields["creado_desde"]}',
                'tipo_de_operacion': f'$answers.{f["tipo_de_operacion"]}',

                # quien crea el pase
                'nombre_crea_el_pase':   f'$answers.{f["nombre_crea_el_pase"]}',
                'email_crea_el_pase':    f'$answers.{f["email_crea_el_pase"]}',
                'telefono_crea_el_pase': f'$answers.{f["telefono_crea_el_pase"]}',

                # transportista que recibe
                'proveedor':          f'$answers.{f["proveedor"]}',
                'proveedor_email':    f'$answers.{f["proveedor_email"]}',
                'proveedor_telefono': f'$answers.{f["proveedor_telefono"]}',

                # material
                'proveedor_cliente_material': f'$answers.{f["proveedor_cliente_material"]}',
                'orden_de_compra':            f'$answers.{f["orden_de_compra"]}',
                'documentos': f'$answers.{f["documentos_para_ocr"]}',
                'materiales': {'$map': {
                    'input': f'$answers.{f["grupo_materiales"]}',
                    'as':    'item',
                    'in': {
                        'tipo':       f'$$item.{f["tipo"]}',
                        'cantidad':   f'$$item.{f["cantidad"]}',
                        'volumen':    f'$$item.{f["volumen"]}',
                        'peso':       f'$$item.{f["peso"]}',
                        'sello':      f'$$item.{f["sello"]}',
                        'contenedor': f'$$item.{f["contenedor"]}',
                    },
                }},

                # lugar entrega / recepción
                'ubicacion':    f'$answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.mf["ubicacion"]}',
                'direccion':    {'$first': f'$answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.f["address_name"]}'},
                'anden':        f'$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf["nombre_area"]}',
                'fecha_desde':  f'$answers.{f["fecha_pase_transportista_desde"]}',
                'fecha_hasta':  f'$answers.{f["fecha_pase_transportista_hasta"]}',
                'hora_inicial': f'$answers.{f["hora_inicial"]}',
                'hora_final':   f'$answers.{f["hora_final"]}',

                # lugar recolección (tipos 2 y 3)
                'lugar_recoleccion':         f'$answers.{f["lugar_de_recoleccion"]}',
                'direccion_recoleccion':     f'$answers.{f["direccion_lugar_de_recoleccion"]}',
                'fecha_recoleccion':         f'$answers.{f["fecha_de_recoleccion"]}',
                'hora_inicial_recoleccion':  f'$answers.{f["hora_inicial_recoleccion"]}',
                'hora_final_recoleccion':    f'$answers.{f["hora_final_recoleccion"]}',
                'anden_recoleccion':         f'$answers.{f["anden_recoleccion"]}',
                'responsable':               f'$answers.{f["responsable"]}',
                'responsable_email':         f'$answers.{f["responsable_email"]}',
                'responsable_telefono':      f'$answers.{f["responsable_telefono"]}',
                'metodo_de_embarque':        f'$answers.{f["metodo_de_embarque"]}',
                'incoterm':                  f'$answers.{f["incoterm"]}',

                # control
                'estado_transportista': f'$answers.{f["estado_transportista"]}',
                'url_del_pase':         f'$answers.{f["url_del_pase_transportista"]}',
                'qr':                   f'$answers.{f["qr_del_pase_transportista"]}',
                'token':                f'$answers.{f["token_transportista"]}',
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

    def get_users_data(self, locations=None):
        locations = ["Planta Monterrey"]
        match = {
            'form_id': self.CONF_AREA_EMPLEADOS,
            'deleted_at': {'$exists': False},
        }
        if locations:
            if isinstance(locations, str):
                locations = [locations]
            match[f'answers.{self.mf["areas_grupo"]}'] = {
                '$elemMatch': {
                    f'{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f["location"]}': {'$in': locations}
                }
            }
        query = [
            {'$match': match},
            {'$project': {
                'nombre':   f'$answers.{self.EMPLOYEE_OBJ_ID}.{self.mf["nombre_empleado"]}',
                'email':    {'$first': f'$answers.{self.EMPLOYEE_OBJ_ID}.{self.f["new_user_email"]}'},
                'telefono': {'$first': f'$answers.{self.EMPLOYEE_OBJ_ID}.{self.mf["telefono_visita_a"]}'},
            }},
            {'$group': {
                '_id': '$nombre',
                'email':    {'$first': '$email'},
                'telefono': {'$first': '$telefono'},
            }},
            {'$project': {
                '_id': 0,
                'nombre':   '$_id',
                'email':    1,
                'telefono': 1,
            }},
            {'$sort': {'nombre': 1}},
        ]
        return self.format_cr(self.cr.aggregate(query))

    def get_location_data(self, location):
        areas_query = [
            {'$match': {
                'form_id': self.AREAS_DE_LAS_UBICACIONES,
                'deleted_at': {'$exists': False},
                f'answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.f["location"]}': location,
            }},
            {'$project': {
                '_id': 0,
                'area': f'$answers.{self.mf["nombre_area"]}',
            }},
            {'$sort': {'area': 1}},
        ]
        areas = [r['area'] for r in self.format_cr(self.cr.aggregate(areas_query)) if r.get('area')]

        ubicacion_query = [
            {'$match': {
                'form_id': self.UBICACIONES,
                'deleted_at': {'$exists': False},
                f'answers.{self.f["location"]}': location,
            }},
            {'$project': {
                '_id': 0,
                'direccion': {'$first': f'$answers.{self.CONTACTO_CAT_OBJ_ID}.{self.mf["direccion"]}'},
            }},
            {'$limit': 1},
        ]
        ubicacion = self.format_cr(self.cr.aggregate(ubicacion_query), get_one=True)

        return {
            'ubicacion': location,
            'direccion': ubicacion.get('direccion', '') if ubicacion else '',
            'areas': areas,
        }

    def get_proveedores_transportista(self):
        query = [
            {'$match': {
                'form_id': self.PROVEEDORES_FORM,
                'deleted_at': {'$exists': False},
                'answers.6a18e4086423e82150aa527c': 'recoleccion',
            }},
            {'$project': {
                '_id': 0,
                'nombre':    '$answers.667468e3e577b8b98c852aaa',
                'direccion': {'$first': f'$answers.{self.CONTACTO_CAT_OBJ_ID}.663a7e0fe48382c5b1230902'},
            }},
            {'$sort': {'nombre': 1}},
        ]
        return self.format_cr(self.cr.aggregate(query))

    def validate_token(self, record_id=None, token=None):
        f = self.pass_fields_transportista
        match = {
            'form_id': self.PASE_ENTRADA_TRANSPORTISTA,
            'deleted_at': {'$exists': False},
        }
        if record_id and token:
            match['_id'] = ObjectId(record_id)
            match[f'answers.{f["token_transportista"]}'] = token
        else:
            self.LKFException({'title': 'Se requiere record_id y token para validar el pase', 'status_code': 400})
        query = [
            {'$match': match},
            {'$project': {
                '_id': 1,
            }},
        ]
        data = self.format_cr(self.cr.aggregate(query), get_one=True)
        if data:
            return True
        return False

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv)
    script_obj.console_run()
    data = script_obj.data.get('data', {})
    option = data.get("option", '')
    payload = data.get("payload", {})
    record_id = data.get("record_id", None)
    token = data.get("token", None)
    locations = data.get("locations", None)
    location = data.get("location", None)

    dispatcher = {
        "create_pass_transportista": lambda: script_obj.create_pass_transportista(payload),
        "generate_submit_token_transportista": lambda: script_obj.generate_submit_token_transportista(record_id),
        "get_andenes": lambda: script_obj.get_andenes(),
        "get_horarios_data": lambda: script_obj.get_horarios_data(dia=data.get('dia')),
        "get_pass_transportista": lambda: script_obj.get_pass_transportista(record_id, token),
        "get_users_data": lambda: script_obj.get_users_data(locations),
        "get_location_data": lambda: script_obj.get_location_data(location),
        "get_proveedores_transportista": lambda: script_obj.get_proveedores_transportista(),
        "validate_token": lambda: script_obj.validate_token(record_id, token)
    }

    action = dispatcher.get(option)
    if action:
        response = action()
        print(simplejson.dumps(response, indent=4))
    else:
        response = {"error": "Opción no válida"}

    script_obj.HttpResponse({"data": response})