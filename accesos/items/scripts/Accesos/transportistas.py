# coding: utf-8
import dis
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.pass_fields_transportista = {
            "tipo_de_operacion": "6a1ddb53f5a36ba1c7dd029c",
            "proveedor": "6a1ddb53f5a36ba1c7dd029d",
            "material": "6a1ddb53f5a36ba1c7dd029e",
            "cantidad": "6a1ddb53f5a36ba1c7dd029f",
            "orden_de_compra": "6a1ddb53f5a36ba1c7dd02a0",
            "direccion_de_recoleccion": "6a1ddb53f5a36ba1c7dd02a1",
            "material_a_recoger": "6a1ddb53f5a36ba1c7dd02a2",
            "cliente": "6a1e28a397ee5bdd2dba11fa",
            "direccion_de_entrega": "6a1ddb54f5a36ba1c7dd02a4",
            "producto": "6a1e28a397ee5bdd2dba11fb",
            "orden_de_venta": "6a1ddb54f5a36ba1c7dd02a5",
            "responsable_de_entrega": "6a1ddb54f5a36ba1c7dd02a6",
            "responsable_de_despacho": "6a1f1b1c5c0c4cacd2fd04ec",
            "transportista": "6a1ddcba20dadbb04a29b59c",
            "placas_del_vehiculo": "6a1ddcba20dadbb04a29b59d",
            "nombre_del_operador": "6a1ddcba20dadbb04a29b59e",
            "fecha_pase_transportista_desde": "6a1ddcba20dadbb04a29b59f",
            "fecha_pase_transportista_hasta": "6a1f15aec19e655f79987c34",
            "hora_inicial": "6a1f15aec19e655f79987c36",
            "hora_final": "6a1f15aec19e655f79987c37",
            "anden_de_recepcion": "6a1ddcba20dadbb04a29b5a1",
            "grupo_documentos": "6a1ddcba20dadbb04a29b5a2",
            "tipo_de_documento": "6a1dde7b9de82363357088d4",
            "documento_transportista": "6a1dde7b9de82363357088d5"
        }

    def create_pass_transportista(self, data):
        f = self.pass_fields_transportista
        metadata = self.lkf_api.get_metadata(form_id=self.PASE_ENTRADA_TRANSPORTISTA)
        metadata.update({
            'properties': {
                'device_properties': {
                    'System': 'Script',
                    'Module': 'Accesos',
                    'Process': 'Pase Transportista',
                    'Action': 'create_pass_transportista',
                    'File': 'modules/accesos/items/scripts/Accesos/transportistas.py',
                }
            }
        })

        tipo = data.get('tipo_de_operacion', '')
        transportista = data.get('transportista', {})

        # La clave de programacion varía según el tipo de operación
        prog = (
            data.get('programacion') or
            data.get('programacion_regreso') or
            data.get('programacion_salida') or
            {}
        )

        horario = prog.get('horario_disponible', '')
        hora_inicio, hora_fin = '', ''
        if horario and '-' in horario:
            partes = horario.split('-')
            hora_inicio = partes[0].strip()
            hora_fin = partes[1].strip()

        answers = {
            f['tipo_de_operacion']:              tipo,
            f['transportista']:                  transportista.get('nombre', ''),
            f['placas_del_vehiculo']:            transportista.get('placas_vehiculo', ''),
            f['nombre_del_operador']:            transportista.get('nombre_operador', ''),
            f['fecha_pase_transportista_desde']: prog.get('fecha_pase_transportista_desde', ''),
            f['fecha_pase_transportista_hasta']: prog.get('fecha_pase_transportista_hasta', ''),
            f['hora_inicial']:                   hora_inicio + ":00" if hora_inicio else None,
            f['hora_final']:                     hora_fin + ":00" if hora_fin else None,
            self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                f['anden_de_recepcion']: prog.get('anden', '')
            },
            f['grupo_documentos']: [
                {
                    f['tipo_de_documento']:       doc.get('tipo_de_documento', ''),
                    f['documento_transportista']: doc.get('documento_transportista', []),
                }
                for doc in data.get('documentos', [])
            ],
        }

        # Tipo 1: Entrega de materia prima
        if tipo == 'entrega_de_materia_prima':
            pym = data.get('proveedor_y_material', {})
            answers.update({
                f['proveedor']:       pym.get('proveedor', ''),
                f['material']:        pym.get('material', ''),
                f['cantidad']:        pym.get('cantidad', ''),
                f['orden_de_compra']: pym.get('orden_compra', ''),
            })

        # Tipo 2: Recoleccion de materia prima
        elif tipo == 'recoleccion_de_materia_prima':
            origen = data.get('origen_recoleccion', {})
            answers.update({
                f['proveedor']:                origen.get('proveedor', ''),
                f['direccion_de_recoleccion']: origen.get('direccion_recoleccion', ''),
                f['material_a_recoger']:       origen.get('material_a_recoger', ''),
                f['orden_de_compra']:          origen.get('orden_compra', ''),
            })

        # Tipo 3: Entrega de producto terminado
        elif tipo == 'entrega_de_producto_terminado':
            cyp = data.get('cliente_y_producto', {})
            answers.update({
                f['cliente']:                cyp.get('cliente', ''),
                f['direccion_de_entrega']:   cyp.get('direccion_entrega', ''),
                f['producto']:               cyp.get('producto', ''),
                f['orden_de_venta']:         cyp.get('orden_venta_remision', ''),
                f['cantidad']:               cyp.get('cantidad', ''),
                f['responsable_de_entrega']: cyp.get('responsable_entrega', ''),
            })

        # Tipo 4: Recoleccion de producto terminado
        elif tipo == 'recoleccion_de_producto_terminado':
            cyp = data.get('cliente_y_producto', {})
            answers.update({
                f['cliente']:                 cyp.get('cliente', ''),
                f['producto']:                cyp.get('producto', ''),
                f['orden_de_venta']:          cyp.get('orden_venta_remision', ''),
                f['cantidad']:                cyp.get('cantidad', ''),
                f['responsable_de_despacho']: cyp.get('responsable_despacho', ''),
            })

        metadata.update({'answers': answers})
        print(simplejson.dumps(answers, indent=3))
        res = self.lkf_api.post_forms_answers(metadata)
        if res.get('status_code') not in [200, 201, 202]:
            self.LKFException({'title': 'Error al crear pase transportista', 'msg': res})
        return res

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

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv)
    script_obj.console_run()
    data = script_obj.data.get('data', {})
    option = data.get("option", '')
    payload = data.get("payload", {})

    dispatcher = {
        "create_pass_transportista": lambda: script_obj.create_pass_transportista(payload),
        "get_andenes": lambda: script_obj.get_andenes(),
        "get_horarios_data": lambda: script_obj.get_horarios_data(dia=data.get('dia'))
    }

    action = dispatcher.get(option)
    if action:
        response = action()
        print(simplejson.dumps(response, indent=4))
    else:
        response = {"error": "Opción no válida"}

    script_obj.HttpResponse({"data": response})