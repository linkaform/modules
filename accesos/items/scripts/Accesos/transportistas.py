# coding: utf-8
import dis
import sys, simplejson, pytz
from math import ceil
from datetime import datetime
from bson import ObjectId
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.envio_correo_fields.update({
            'clase': '6a77cd140ad8005cec5d9700',
        })
        self.envio_notificacion_transportista_fields = {
            'estatus':               '6a7775df8997b1604c8d34b1',
            'folio':                 '6a7775df8997b1604c8d34b2',
            'tipo_de_operacion':     '6a7775df8997b1604c8d34b3',
            'empresa_transportista': '6a7775df8997b1604c8d34b4',
            'proveedor_cliente':     '6a7775df8997b1604c8d34b5',
            'fecha_hora_ingreso':    '6a7775df8997b1604c8d34b6',
            'fecha_hora_descarga':   '6a7775df8997b1604c8d34b7',
            'fecha_hora_terminado':  '6a7775df8997b1604c8d34b8',
        }

    def get_search_fields_transportistas(self):
        return self.search_fields_config(self.transportistas_search_fields())

    def get_search_counts_transportistas(self, date_from=None, date_to=None, facets=[], candidates=[]):
        base_match = self.transportistas_base_match(date_from, date_to)
        return self.count_facet_candidates(base_match, self.transportistas_search_fields(), facets, candidates)

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

    
    def save_inspecciones(self, record_id, data):
        print(simplejson.dumps(data, indent=3))
        f_bit = self.bitacora_transportista_fields

        # Rama "remolque" muerta desde 2026-07-31 (sin UI que la dispare) — se deja
        # intacta, sigue dependiendo de esta lista fija por no estar en uso real.
        REMOLQUE_CAMPOS = [
            'tanque_de_aire',
            'ejes_de_transmision',
            'quinta_rueda',
            'chasis',
            'puertas_externa',
            'piso_externo_trailer_contenedor_caja',
            'paredes_externa',
            'pared_frontal_externa',
            'techo_externo',
            'unidad_de_refrigeracion',
            'escape_mofles',
        ]

        # field_id "universales" de las 3 medidas de contenedor — mismos que asume
        # el frontend (CONTENEDOR_MEDIDA_FIELD_IDS en useInspeccionPuntosTransportista.ts),
        # estables sin importar qué forma (default o custom) esté resuelta.
        CONTENEDOR_MEDIDA_FIELD_IDS_UNIVERSAL = {
            'altura':    'd412fb9f428dfc231c9bc3f0',
            'ancho':     '6477c73222d9b7e8dd1de3b9',
            'longitud':  'd7c19cbd2cfe6b19f848d697',
        }

        inspecciones_creadas = []

        for inspeccion in data:
            tipo   = inspeccion.get('tipo', '')
            unidad = inspeccion.get('unidad')
            tipo_label = f'{tipo}_{unidad}' if unidad else tipo
            tipo_base = tipo.replace('salida_', '')
            es_default = False

            if tipo_base == 'tractor':
                # Guardado siempre por field_id — funciona igual para la forma
                # default de cuenta 10 o cualquier forma custom resuelta por
                # ubicación, porque el frontend ya detecta cada punto y su
                # comentario/evidencia propios por adyacencia (ver extractPuntos
                # en useInspeccionPuntosTransportista.ts), no por posición fija.
                puntos = inspeccion.get('puntos', [])
                if not any(p.get('resultado') for p in puntos):
                    continue
                form_id = inspeccion.get('form_id') or self.INSPECCION_ENTRADA_CTPAT_TRACTOR
                # El frontend manda form_id como string; self.INSPECCION_ENTRADA_CTPAT_TRACTOR
                # es int — comparar como string para no fallar por tipo.
                es_default = str(form_id) == str(self.INSPECCION_ENTRADA_CTPAT_TRACTOR)
                answers = {}
                for punto in puntos:
                    resultado = (punto.get('resultado') or '').lower()
                    if punto.get('field_id') and resultado:
                        answers[punto['field_id']] = resultado
                    if punto.get('comentario_field_id') and punto.get('comentario'):
                        answers[punto['comentario_field_id']] = punto['comentario']
                    if punto.get('evidencia_field_id') and punto.get('fotos'):
                        answers[punto['evidencia_field_id']] = punto['fotos']

            elif tipo_base == 'remolque':
                puntos = inspeccion.get('puntos', [])
                if not any(p.get('resultado') for p in puntos):
                    continue
                form_id = self.INSPECCION_ENTRADA_CTPAT_REMOLQUE
                f_ins   = self.inspeccion_entrada_ctpat_remolque_fields
                answers = {}
                medidas = inspeccion.get('medidas', {}) or {}
                if medidas.get('longitud'):
                    answers[f_ins['longitud_interior']] = medidas['longitud']
                if medidas.get('ancho'):
                    answers[f_ins['ancho_interior']] = medidas['ancho']
                if medidas.get('altura'):
                    answers[f_ins['altura_interior']] = medidas['altura']
                for punto in puntos:
                    num = punto.get('numero', 0) - 1
                    if 0 <= num < len(REMOLQUE_CAMPOS):
                        campo = REMOLQUE_CAMPOS[num]
                        resultado = (punto.get('resultado') or '').lower()
                        if resultado:
                            answers[f_ins[campo]] = resultado
                        if punto.get('comentario'):
                            answers[f_ins[f'{campo}_comentarios']] = punto['comentario']
                        if punto.get('fotos'):
                            answers[f_ins[f'{campo}_evidencia']] = punto['fotos']

            elif tipo_base == 'contenedor':
                puntos = inspeccion.get('puntos') or []
                if puntos:
                    # Forma custom tipo Patrón A (ej. BASC): mismo tratamiento
                    # que tractor — radio + comentario + evidencia propios por
                    # punto, detectados por adyacencia en el frontend.
                    if not any(p.get('resultado') for p in puntos):
                        continue
                    form_id = inspeccion.get('form_id') or self.INSPECCION_ENTRADA_CTPAT_CONTENEDOR
                    es_default = str(form_id) == str(self.INSPECCION_ENTRADA_CTPAT_CONTENEDOR)
                    answers = {}
                    for punto in puntos:
                        resultado = (punto.get('resultado') or '').lower()
                        if punto.get('field_id') and resultado:
                            answers[punto['field_id']] = resultado
                        if punto.get('comentario_field_id') and punto.get('comentario'):
                            answers[punto['comentario_field_id']] = punto['comentario']
                        if punto.get('evidencia_field_id') and punto.get('fotos'):
                            answers[punto['evidencia_field_id']] = punto['fotos']
                else:
                    # Forma CTPAT default (Patrón B): filas + medidas
                    # universales, sin la lista fija de labels de antes.
                    filas   = inspeccion.get('filas', [])
                    medidas = inspeccion.get('medidas', {}) or {}
                    has_data = (
                        any(fila.get('valores') for fila in filas)
                        or any(medidas.get(k) for k in ['longitud', 'ancho', 'altura'])
                    )
                    if not has_data:
                        continue
                    form_id = inspeccion.get('form_id') or self.INSPECCION_ENTRADA_CTPAT_CONTENEDOR
                    es_default = str(form_id) == str(self.INSPECCION_ENTRADA_CTPAT_CONTENEDOR)
                    answers = {}
                    for medida_key, field_id in CONTENEDOR_MEDIDA_FIELD_IDS_UNIVERSAL.items():
                        if medidas.get(medida_key):
                            answers[field_id] = medidas[medida_key]
                    for fila in filas:
                        field_id = fila.get('field_id')
                        valores  = fila.get('valores') or []
                        if field_id and valores:
                            answers[field_id] = [v.lower() for v in valores]
            else:
                continue

            # Comentario/evidencia generales de toda la inspección — remolque
            # (rama muerta) conserva su propio manejo arriba y no entra aquí.
            if tipo_base in ('tractor', 'contenedor'):
                comentario_general_field_id = inspeccion.get('comentario_general_field_id')
                if comentario_general_field_id and inspeccion.get('comentario_general'):
                    answers[comentario_general_field_id] = inspeccion['comentario_general']

                evidencias = inspeccion.get('evidencias') or []
                if evidencias:
                    if es_default:
                        f_ins = (
                            self.inspeccion_entrada_tractor_fields if tipo_base == 'tractor'
                            else self.inspeccion_entrada_ctpat_contenedor_fields
                        )
                        answers[f_ins['fotos_y_documentos']] = [
                            {
                                f_ins['tipo_de_documento']: ev.get('tipo', ''),
                                f_ins['documento']:         [{'file_name': ev['file_name'], 'file_url': ev['file_url']}],
                            }
                            for ev in evidencias
                        ]
                    else:
                        evidencia_general_field_id = inspeccion.get('evidencia_general_field_id')
                        if evidencia_general_field_id:
                            answers[evidencia_general_field_id] = [
                                {'file_name': ev['file_name'], 'file_url': ev['file_url']}
                                for ev in evidencias
                            ]

            metadata = self.lkf_api.get_metadata(form_id=form_id)
            inspeccion_id = self.object_id()
            metadata.update({
                'id': inspeccion_id,
                'properties': {
                    'device_properties': {
                        'System':  'Script',
                        'Module':  'Accesos',
                        'Process': 'Inspección CTPAT',
                        'Action':  'save_inspecciones',
                        'File':    'modules/accesos/items/scripts/Accesos/transportistas.py',
                    }
                },
                'answers': answers,
            })
            print(simplejson.dumps(answers, indent=3))
            res = self.lkf_api.post_forms_answers(metadata)
            print(f'save_inspecciones [{tipo_label}] res=', res.get('status_code'))
            if res.get('status_code') not in [200, 201, 202]:
                self.LKFException({'title': f'Error al crear inspección {tipo_label}', 'msg': res})
            inspecciones_creadas.append((tipo_label, inspeccion_id))

        if inspecciones_creadas:
            es_salida = any(t.startswith('salida_') for t, _ in inspecciones_creadas)
            nuevo_estatus = self._resolver_estatus_tras_inspeccion(es_salida)
            answers_bitacora = {
                f_bit['estatus']: nuevo_estatus,
                f_bit['grupo_inspecciones']: {
                    -(i + 1): {
                        f_bit['tipo_inspeccion']: tipo_label,
                        f_bit['url_inspeccion']:  f'https://app.linkaform.com/#/records/detail/{inspeccion_id}',
                    }
                    for i, (tipo_label, inspeccion_id) in enumerate(inspecciones_creadas)
                }
            }
            if nuevo_estatus in ('carga_/_descarga', 'terminado'):
                tz_name = self.user.get('timezone', 'America/Mexico_City')
                fecha_hora_actual = datetime.now(pytz.timezone(tz_name)).strftime('%Y-%m-%d %H:%M:%S')
                if nuevo_estatus == 'carga_/_descarga':
                    answers_bitacora[f_bit['fecha_hora_descarga']] = fecha_hora_actual
                elif nuevo_estatus == 'terminado':
                    answers_bitacora[f_bit['fecha_hora_terminado']] = fecha_hora_actual
            res_bit = self.lkf_api.patch_multi_record(
                answers=answers_bitacora,
                form_id=self.BITACORA_TRANSPORTISTAS,
                record_id=[record_id],
            )
            if res_bit.get('status_code') not in [201, 202, 203]:
                self.LKFException({'title': 'Error al actualizar inspecciones en bitácora', 'msg': res_bit})

        return {'status_code': 200, 'msg': 'OK', 'inspecciones_creadas': [t for t, _ in inspecciones_creadas]}

    def save_inspecciones_sello(self, record_id, data):
        print(simplejson.dumps(data, indent=3))
        f_bit = self.bitacora_transportista_fields
        f     = self.inspeccion_de_sello_fields

        SLOT_MAP = {
            'foto_sello':              f['1_foto_del_sello'],
            'sello_puertas':           f['2_sello_colocado_en_las_puertas'],
            'puertas_completas':       f['3_puertas_completas_del_remolque'],
            'placas_economico':        f['4_placas_o_economico'],
            'identificacion_operador': f['5_identificacion_del_operador'],
        }

        ISO_MAP = {
            'I':  'indicative',
            'S':  'security',
            'H':  'high_security',
            'HS': 'high_security',
        }

        inspecciones_creadas = []

        for inspeccion in data:
            tipo_bitacora = inspeccion.get('tipo')
            if tipo_bitacora not in ('sello', 'salida_sello'):
                continue

            answers = {}

            if inspeccion.get('no_sello_revisado'):
                answers[f['numero_de_sello_fisico']] = inspeccion['no_sello_revisado']
            if inspeccion.get('no_sello_sistema'):
                answers[f['numero_de_sello_esperado_revisado']] = inspeccion['no_sello_sistema']
            if inspeccion.get('clasificacion_iso'):
                iso_raw = inspeccion['clasificacion_iso']
                answers[f['tipo_de_sello_clasificacion_iso_17712']] = ISO_MAP.get(iso_raw, iso_raw.lower())
            if inspeccion.get('comentario'):
                answers[f['comentarios']] = inspeccion['comentario']

            vvtt = inspeccion.get('vvtt', []) or []
            acciones_verificadas = [v['punto'].lower() for v in vvtt if v.get('verificado')]
            if acciones_verificadas:
                answers[f['matriz_vttt_marca_cada_accion_verificada']] = acciones_verificadas

            for evidencia in inspeccion.get('evidencias', []) or []:
                field_id = SLOT_MAP.get(evidencia.get('slot', ''))
                if field_id and evidencia.get('file_url'):
                    answers[field_id] = [{'file_name': evidencia['file_name'], 'file_url': evidencia['file_url']}]

            metadata = self.lkf_api.get_metadata(form_id=self.INSPECCION_SELLO)
            inspeccion_id = self.object_id()
            metadata.update({
                'id': inspeccion_id,
                'properties': {
                    'device_properties': {
                        'System':  'Script',
                        'Module':  'Accesos',
                        'Process': 'Inspección de Sello',
                        'Action':  'save_inspecciones_sello',
                        'File':    'modules/accesos/items/scripts/Accesos/transportistas.py',
                    }
                },
                'answers': answers,
            })
            print(simplejson.dumps(answers, indent=3))
            res = self.lkf_api.post_forms_answers(metadata)
            print(f'save_inspecciones_sello [unidad={inspeccion.get("unidad")}] res=', res.get('status_code'))
            if res.get('status_code') not in [200, 201, 202]:
                self.LKFException({'title': f'Error al crear inspección de sello unidad {inspeccion.get("unidad")}', 'msg': res})

            tipo_label = f'{tipo_bitacora}_{inspeccion.get("unidad", "")}'
            inspecciones_creadas.append((tipo_label, inspeccion_id))

        if inspecciones_creadas:
            answers_bitacora = {
                f_bit['grupo_inspecciones']: {
                    -(i + 1): {
                        f_bit['tipo_inspeccion']: tipo_label,
                        f_bit['url_inspeccion']:  f'https://app.linkaform.com/#/records/detail/{inspeccion_id}',
                    }
                    for i, (tipo_label, inspeccion_id) in enumerate(inspecciones_creadas)
                }
            }
            res_bit = self.lkf_api.patch_multi_record(
                answers=answers_bitacora,
                form_id=self.BITACORA_TRANSPORTISTAS,
                record_id=[record_id],
            )
            if res_bit.get('status_code') not in [201, 202, 203]:
                self.LKFException({'title': 'Error al actualizar inspecciones de sello en bitácora', 'msg': res_bit})

        return {'status_code': 200, 'msg': 'OK', 'inspecciones_creadas': [t for t, _ in inspecciones_creadas]}

    def get_inspeccion_record(self, record_id, tipo):
        FORM_MAP = {
            'tractor':           (self.INSPECCION_ENTRADA_CTPAT_TRACTOR,    self.inspeccion_entrada_tractor_fields),
            'remolque':          (self.INSPECCION_ENTRADA_CTPAT_REMOLQUE,   self.inspeccion_entrada_ctpat_remolque_fields),
            'contenedor':        (self.INSPECCION_ENTRADA_CTPAT_CONTENEDOR, self.inspeccion_entrada_ctpat_contenedor_fields),
            'sello':             (self.INSPECCION_SELLO,                    self.inspeccion_de_sello_fields),
        }
        tipo_base = tipo
        if tipo_base.startswith('salida_'):
            tipo_base = tipo_base[len('salida_'):]
        # strip numeric suffix: contenedor_1 → contenedor
        if '_' in tipo_base and tipo_base.rsplit('_', 1)[-1].isdigit():
            tipo_base = tipo_base.rsplit('_', 1)[0]
        entry = FORM_MAP.get(tipo_base)
        if not entry:
            self.LKFException({'title': f'Tipo de inspección no válido: {tipo}', 'status_code': 400})

        form_id_default, fields = entry
        query = [
            {'$match': {
                'deleted_at': {'$exists': False},
                '_id': ObjectId(record_id),
            }},
            {'$project': {
                '_id': 1,
                'folio': 1,
                'created_at': 1,
                'form_id': 1,
                'answers': 1,
            }},
        ]
        # OJO: NO usar self.format_cr/self._labels sobre el documento completo
        # antes de decidir la rama — _labels() APLANA cualquier dict anidado
        # (incluida 'answers') hacia el nivel superior, perdiendo la llave
        # 'answers' por completo (misma clase de bug que en get_formas_inspeccion).
        # Se lee el documento crudo primero y solo se aplica _labels() en la rama
        # default, donde SÍ se quiere ese aplanado (traducido por `fields`).
        resultados = list(self.cr.aggregate(query))
        data = resultados[0] if resultados else None
        if not data:
            return data
        # format_cr normalmente convierte _id/created_at antes de _labels() —
        # como aquí se evita format_cr, se replica manualmente para las 2 ramas.
        if data.get('_id') is not None:
            data['_id'] = str(data['_id'])
        if data.get('created_at'):
            data['created_at'] = self.get_date_str(data['created_at'])
        if data.get('form_id') == form_id_default:
            return self._labels(data, ids_label_dct=fields)
        # Forma custom resuelta por ubicación — no hay diccionario de slugs
        # conocido de antemano, se regresan las respuestas crudas por field_id.
        # Limitación aceptada: el visor de registros ya guardados no las agrupa
        # en secciones para este caso (igual que useGetInspeccionRecord.ts, que
        # ya quedó fuera de la dinamización previa).
        return {
            '_id':        data.get('_id'),
            'folio':      data.get('folio'),
            'created_at': data.get('created_at'),
            'answers':    data.get('answers'),
        }

    def get_form_fields(self, form_ids):
        if isinstance(form_ids, str):
            form_ids = [form_ids]

        def normaliza_field(field):
            return {
                'field_id':   field.get('field_id'),
                'label':      field.get('label'),
                'field_type': field.get('field_type'),
                'options':    field.get('options', []),
                'required':   field.get('required', False),
            }

        resultado = []
        for form_id in form_ids:
            form_data = self.lkf_api.get_form_id_fields(form_id)
            if not form_data:
                self.LKFException({'title': f'No se pudo obtener la forma {form_id}', 'status_code': 404})

            form = form_data[0]
            form_pages = form.get('form_pages') or []
            if form_pages:
                pages = [
                    {
                        'page_name': page.get('page_name', ''),
                        'fields': [normaliza_field(f) for f in page.get('page_fields', [])],
                    }
                    for page in form_pages
                ]
            else:
                pages = [{
                    'page_name': '',
                    'fields': [normaliza_field(f) for f in form.get('fields', [])],
                }]

            resultado.append({'form_id': form_id, 'pages': pages})

        return resultado

    def get_config_flujo_transportistas(self):
        """Etapas activas y columnas visibles del kanban del flujo de transportistas
        para esta cuenta. Registro singleton (un solo record) en la forma
        "Configuración de Flujo de Transportistas". Si no existe el registro
        todavía, regresa los defaults fail-open (mismo comportamiento que antes
        de estos toggles)."""
        f = self.conf_flujo_transportistas_fields
        query = [
            {'$match': {
                'form_id': self.CONFIGURACION_FLUJO_TRANSPORTISTAS,
                'deleted_at': {'$exists': False},
            }},
            {'$sort': {'updated_at': -1}},
            {'$limit': 1},
            {'$project': {
                '_id': 0,
                'etapas_activas': f'$answers.{f["etapas_activas"]}',
                'kanban_view': f'$answers.{f["kanban_view"]}',
            }},
        ]
        data = self.format_cr(self.cr.aggregate(query), get_one=True)
        # Valores tal cual quedaron las opciones del checkbox `etapas_activas` en Linkaform.
        # 'inspeccion_materiales' es un toggle aparte (no una etapa del kanban): si carga/
        # descarga es solo informativa, esto controla si además exige inspeccionar cantidad
        # física de materiales antes de dejar avanzar a inspección de salida.
        etapas_activas = (data or {}).get('etapas_activas') or [
            'inspeccion_de_entrada', 'carga_/_descarga', 'inspeccion_salida', 'inspeccion_materiales',
        ]
        # Kanban View: qué columnas del kanban se muestran para esta cuenta — es
        # puramente visual, no afecta el flujo/estatus real de la bitácora (a
        # diferencia de `etapas_activas`). Valores tal cual las opciones del
        # checkbox `kanban_view` en Linkaform. Fail-open: todas visibles si la
        # cuenta todavía no configuró este campo.
        kanban_view = (data or {}).get('kanban_view') or [
            'programados', 'arribo', 'inspeccion_de_entrada', 'carga_/_descarga', 'inspeccion_salida', 'terminados',
        ]
        return {'etapas_activas': etapas_activas, 'kanban_view': kanban_view}

    def _resolver_estatus_tras_inspeccion(self, es_salida):
        """A qué estatus debe pasar la bitácora tras guardar una inspección de
        entrada o de salida — respeta qué etapas están activas para la cuenta
        (mismo criterio que ORDEN_ESTATUS en el frontend, page.tsx), en vez de
        asumir siempre 'inspeccion_entrada'/'inspeccion_salida' como antes.
        Si esa etapa conceptual no está activa, salta al siguiente estatus activo
        después de donde normalmente caería."""
        etapas_activas = self.get_config_flujo_transportistas()['etapas_activas']
        orden = ['arribo']
        if 'inspeccion_de_entrada' in etapas_activas:
            orden.append('inspeccion_entrada')
        if 'carga_/_descarga' in etapas_activas:
            orden.append('carga_/_descarga')
        if 'inspeccion_salida' in etapas_activas:
            orden.append('inspeccion_salida')
        orden.append('terminado')

        objetivo = 'inspeccion_salida' if es_salida else 'inspeccion_entrada'
        if objetivo in orden:
            return objetivo

        ancla = 'carga_/_descarga' if es_salida else 'arribo'
        if ancla not in orden:
            ancla = 'arribo'
        idx = orden.index(ancla)
        return orden[idx + 1] if idx + 1 < len(orden) else orden[-1]

    def get_formas_inspeccion(self, ubicacion):
        """Resuelve qué form_id de inspección usar para tractor/contenedor/sello en
        una ubicación, leyendo el grupo `configuracion_de_inspecciones` de la forma
        "Configuración de Flujo de Transportistas" (referencia al Catálogo de Formas,
        `self.CATALOGO_FORMAS_CAT_OBJ_ID`, ya heredado de la capa base).
        Fail-open: si no hay fila para esa ubicación+tipo (o no hay ubicación, o no
        hay registro de config), cae al form_id hardcodeado de la cuenta.

        Para `contenedor`, una fila puede además traer `subtipo` (ej. caja_seca,
        refrigerado): esas filas NO pisan el catch-all `resueltas['contenedor']`,
        se acumulan aparte en `contenedor_por_subtipo`. Una fila de contenedor sin
        subtipo especificado es el catch-all — aplica a cualquier subtipo que no
        tenga su propia fila.

        `norma` en el resultado es un indicador derivado, solo para mostrar en el
        front (ej. badge "BASC"/"CTPAT" en el detalle de la visita) — NO se usa
        como criterio de match arriba: si la ubicación tiene alguna fila con
        Norma=BASC se considera BASC en conjunto, si no tiene ninguna es CTPAT
        por default. No valida que todas las filas de la ubicación compartan la
        misma norma (esa validación queda pendiente, fuera de alcance por ahora)."""
        # self.lkm.form_id() regresa int — se castea a str para que coincida en tipo
        # con el form_id guardado en el catálogo (también numérico) y con las
        # constantes del frontend (TRACTOR_FORM_ID = "157729", como string).
        defaults = {
            'tractor':    str(self.INSPECCION_ENTRADA_CTPAT_TRACTOR),
            'contenedor': str(self.INSPECCION_ENTRADA_CTPAT_CONTENEDOR),
            'sello':      str(self.INSPECCION_SELLO),
        }
        if not ubicacion:
            return dict(defaults, contenedor_por_subtipo={}, norma='ctpat')

        f = self.conf_flujo_transportistas_fields
        query = [
            {'$match': {
                'form_id': self.CONFIGURACION_FLUJO_TRANSPORTISTAS,
                'deleted_at': {'$exists': False},
            }},
            {'$sort': {'updated_at': -1}},
            {'$limit': 1},
            {'$project': {
                '_id': 0,
                'filas': {'$ifNull': [f'$answers.{f["configuracion_de_inspecciones"]}', []]},
            }},
        ]
        # OJO: NO usar self.format_cr aquí — sin un ids_label_dct explícito, _labels()
        # cae a self.f (el diccionario GLOBAL de toda la cuenta) y re-etiqueta los
        # field_id anidados del catálogo con el slug que sea que tengan registrado
        # en CUALQUIER otro módulo (ej. "incidente_location" en vez de la ubicación
        # real), rompiendo el match. Aquí necesitamos el documento crudo tal cual.
        resultados = list(self.cr.aggregate(query))
        data = resultados[0] if resultados else {}
        filas = (data or {}).get('filas') or []
        if isinstance(filas, dict):
            filas = list(filas.values())

        # field_id del sub-campo "ID de la forma" dentro del Catálogo de Formas.
        # self.mf no trae 'form_id'/'form_name'/'form_type' en esta cadena de
        # herencia (se pierden en algún punto del MRO) aunque sí trae 'ubicacion' —
        # se usa el field_id fijo directo, mismo patrón que las medidas universales.
        FORMA_ID_DE_LA_FORMA_FIELD = '5d810a982628de5556500d56'

        resueltas = dict(defaults)
        contenedor_por_subtipo = {}
        normas_vistas = set()
        for fila in filas:
            fila_ubicacion = (fila.get(self.UBICACIONES_CAT_OBJ_ID) or {}).get(self.mf['ubicacion'])
            if fila_ubicacion != ubicacion:
                continue
            norma_fila = (fila.get(f['norma']) or '').strip()
            if norma_fila:
                normas_vistas.add(norma_fila)
            tipo = fila.get(f['tipo_de_inspeccion'])
            subtipo = (fila.get(f['subtipo']) or '').strip()
            forma_id = (fila.get(self.CATALOGO_FORMAS_CAT_OBJ_ID) or {}).get(FORMA_ID_DE_LA_FORMA_FIELD)
            if not forma_id or tipo not in resueltas:
                continue
            if tipo == 'contenedor' and subtipo:
                contenedor_por_subtipo[subtipo] = str(forma_id)
            else:
                resueltas[tipo] = str(forma_id)
        resueltas['contenedor_por_subtipo'] = contenedor_por_subtipo
        resueltas['norma'] = 'basc' if 'basc' in normas_vistas else 'ctpat'
        return resueltas

    def send_aviso_correo_transportista(self, record_id, email_to):
        if not email_to:
            self.LKFException({'title': 'Se requiere al menos un correo destinatario', 'status_code': 400})
        emails = email_to if isinstance(email_to, list) else [email_to]

        record = self.get_bitac_transportista_record(record_id)
        if not record:
            self.LKFException({'title': 'Registro no encontrado', 'status_code': 404})

        folio = record.get('folio', '')
        titulo = f'Proceso de transportista terminado — Folio {folio}'
        tf = self.envio_notificacion_transportista_fields

        for email in emails:
            metadata = self.lkf_api.get_metadata(form_id=self.ENVIO_DE_NOTIFICACIONES_FORM)
            metadata.update({
                "properties": {
                    "device_properties": {
                        "System": "Addons",
                        "Process": "Creación de envio de correo",
                        "Action": "send_aviso_correo_transportista",
                        "File": "transportistas.py",
                    }
                },
            })
            answers = {
                self.envio_correo_fields['tipo_de_notificacion']: 'email',
                self.envio_correo_fields['clase']:                'transportista',
                self.envio_correo_fields['titulo']:               titulo,
                self.envio_correo_fields['nombre']:                titulo,
                self.envio_correo_fields['msj']:                   titulo,
                self.envio_correo_fields['email_from']:            'no-reply@linkaform.com',
                self.envio_correo_fields['email_to']:              email,
                self.envio_correo_fields['enviado_desde']:         'Bitácora de Transportistas',
                tf['estatus']:               record.get('estatus', ''),
                tf['folio']:                 folio,
                tf['tipo_de_operacion']:     record.get('tipo_de_operacion', ''),
                tf['empresa_transportista']: record.get('empresa_transportista', ''),
                tf['proveedor_cliente']:     record.get('proveedor_cliente', ''),
                tf['fecha_hora_ingreso']:    record.get('fecha_hora_ingreso', ''),
                tf['fecha_hora_descarga']:   record.get('fecha_hora_descarga', ''),
                tf['fecha_hora_terminado']:  record.get('fecha_hora_terminado', ''),
            }
            metadata.update({'answers': answers})
            res = self.lkf_api.post_forms_answers(metadata)
            if res.get('status_code') not in [200, 201, 202]:
                self.LKFException({'title': f'Error al enviar aviso a {email}', 'msg': res.get('json'), 'status_code': 400})

        return {'status_code': 200, 'msg': 'OK', 'enviado_a': emails}

    def get_fotografias(self, registros):
        FORM_MAP = {
            'bitacora':   (self.BITACORA_TRANSPORTISTAS,            self.bitacora_transportista_fields),
            'tractor':    (self.INSPECCION_ENTRADA_CTPAT_TRACTOR,    self.inspeccion_entrada_tractor_fields),
            'remolque':   (self.INSPECCION_ENTRADA_CTPAT_REMOLQUE,   self.inspeccion_entrada_ctpat_remolque_fields),
            'contenedor': (self.INSPECCION_ENTRADA_CTPAT_CONTENEDOR, self.inspeccion_entrada_ctpat_contenedor_fields),
            'sello':      (self.INSPECCION_SELLO,                   self.inspeccion_de_sello_fields),
        }

        FOTO_KEYS = {
            'bitacora':   ['firma_conductor'],
            'tractor':    [k for k in self.inspeccion_entrada_tractor_fields if k.endswith('_evidencia')],
            'remolque':   [k for k in self.inspeccion_entrada_ctpat_remolque_fields if k.endswith('_evidencia')],
            'contenedor': [],
            'sello': [
                '1_foto_del_sello',
                '2_sello_colocado_en_las_puertas',
                '3_puertas_completas_del_remolque',
                '4_placas_o_economico',
                '5_identificacion_del_operador',
            ],
        }

        GRUPO_FOTOS_KEY = {
            'bitacora':   'grupo_fotos_y_documentos',
            'tractor':    'fotos_y_documentos',
            'remolque':   'fotos_y_documentos',
            'contenedor': 'fotos_y_documentos',
            'sello':      None,
        }

        def normaliza_tipo(tipo):
            tipo = (tipo or '').replace('salida_', '')
            if '_' in tipo and tipo.rsplit('_', 1)[-1].isdigit():
                tipo = tipo.rsplit('_', 1)[0]
            return tipo

        ids_por_tipo = {}
        for registro in registros:
            tipo = normaliza_tipo(registro.get('tipo_de_registro'))
            record_id = registro.get('record_id')
            if tipo in FORM_MAP and record_id:
                ids_por_tipo.setdefault(tipo, []).append(record_id)

        fotos_por_record = {}
        for tipo, ids in ids_por_tipo.items():
            form_id, fields = FORM_MAP[tipo]
            query = [
                {'$match': {
                    'form_id': form_id,
                    'deleted_at': {'$exists': False},
                    '_id': {'$in': [ObjectId(i) for i in ids]},
                }},
                {'$project': {'_id': 1, 'answers': 1}},
            ]
            for reg_db in self.format_cr(self.cr.aggregate(query)):
                labeled = self._labels(reg_db, ids_label_dct=fields)
                fotos = []
                for key in FOTO_KEYS[tipo]:
                    imgs = labeled.get(key)
                    if not imgs:
                        continue
                    if key.endswith('_evidencia'):
                        comentario = labeled.get(key[:-len('_evidencia')] + '_comentarios')
                    elif tipo == 'sello':
                        comentario = labeled.get('comentarios')
                    else:
                        comentario = None
                    for img in imgs:
                        foto = dict(img)
                        if comentario:
                            foto['comentario'] = comentario
                        fotos.append(foto)
                grupo_key = GRUPO_FOTOS_KEY[tipo]
                if grupo_key:
                    grupo = labeled.get(grupo_key) or []
                    if isinstance(grupo, dict):
                        grupo = list(grupo.values())
                    for fila in grupo:
                        documento = (fila or {}).get('documento')
                        if documento:
                            fotos.extend(documento)
                fotos_por_record[str(reg_db['_id'])] = fotos

        return [
            {
                'record_id': registro['record_id'],
                'tipo_de_registro': registro.get('tipo_de_registro'),
                'fotografias': fotos_por_record.get(registro['record_id'], []),
            }
            for registro in registros
        ]

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()
    data = script_obj.data.get('data', {})
    option = data.get("option", '')
    inspecciones = data.get("inspecciones", [])
    registros = data.get("registros", [])
    payload = data.get("payload", {})
    record_id = data.get("record_id", None)
    token = data.get("token", None)
    folio = data.get("folio", None)
    locations = data.get("locations", None)
    location = data.get("location", None)
    date_from = data.get("date_from", None)
    date_to = data.get("date_to", None)
    tipo_de_vehiculo = data.get("tipo_de_vehiculo", None)
    proveedor_cliente = data.get("proveedor_cliente", None)
    anden_asignado = data.get("anden_asignado", None)
    estatus = data.get("estatus", None)
    tipo_de_operacion = data.get("tipo_de_operacion", None)
    conductor = data.get("conductor", None)
    material = data.get("material", None)
    search = data.get("search", None)
    skip = data.get("skip", 0)
    limit = data.get("limit", None)
    pagination = data.get("pagination", False)
    form_ids = data.get("form_ids", [])
    email_to = data.get("email_to")
    ubicacion = data.get("ubicacion")

    dispatcher = {
        "create_pass_transportista": lambda: script_obj.create_pass_transportista(payload),
        "create_visit_transportista": lambda: script_obj.create_visit_transportista(payload),
        "generate_submit_token_transportista": lambda: script_obj.generate_submit_token_transportista(record_id),
        "get_andenes": lambda: script_obj.get_andenes(),
        "get_bitac_transportista_record": lambda: script_obj.get_bitac_transportista_record(record_id),
        "get_bitac_transportista_records": lambda: script_obj.get_bitac_transportista_records(
            date_from=date_from, date_to=date_to,
            tipo_de_vehiculo=tipo_de_vehiculo, proveedor_cliente=proveedor_cliente, anden_asignado=anden_asignado,
            estatus=estatus, tipo_de_operacion=tipo_de_operacion, conductor=conductor, material=material,
            search=search, skip=skip, limit=limit, pagination=pagination, facets=data.get("facets", []),
        ),
        "get_search_fields": lambda: script_obj.get_search_fields_transportistas(),
        "get_search_counts": lambda: script_obj.get_search_counts_transportistas(
            date_from=date_from, date_to=date_to, facets=data.get("facets", []), candidates=data.get("candidates", []),
        ),
        "get_horarios_data": lambda: script_obj.get_horarios_data(dia=data.get('dia')),
        "get_pass_transportista": lambda: script_obj.get_pass_transportista(record_id, token, folio),
        "get_users_data": lambda: script_obj.get_users_data(locations),
        "get_location_data": lambda: script_obj.get_location_data(location),
        "get_proveedores_transportista": lambda: script_obj.get_proveedores_transportista(),
        "validate_token": lambda: script_obj.validate_token(record_id, token),
        "update_information_transportista": lambda: script_obj.update_information_transportista(payload),
        "save_bitac_transportista_record": lambda: script_obj.save_bitac_transportista_record(record_id, payload),
        "delete_bitac_transportista_items": lambda: script_obj.delete_bitac_transportista_items(record_id, payload),
        "save_inspecciones": lambda: script_obj.save_inspecciones(record_id, inspecciones),
        "save_inspecciones_sello": lambda: script_obj.save_inspecciones_sello(record_id, inspecciones),
        "get_inspeccion_record": lambda: script_obj.get_inspeccion_record(record_id, data.get('tipo', '')),
        "get_fotografias": lambda: script_obj.get_fotografias(registros),
        "get_form_fields": lambda: script_obj.get_form_fields(form_ids),
        "get_config_flujo_transportistas": lambda: script_obj.get_config_flujo_transportistas(),
        "get_formas_inspeccion_transportista": lambda: script_obj.get_formas_inspeccion(ubicacion),
        "send_aviso_correo_transportista": lambda: script_obj.send_aviso_correo_transportista(record_id, email_to),
    }

    action = dispatcher.get(option)
    if action:
        response = action()
        print(simplejson.dumps(response, indent=4))
    else:
        response = {"error": "Opción no válida"}

    script_obj.HttpResponse({"data": response})