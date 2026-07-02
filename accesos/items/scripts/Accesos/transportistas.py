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

    def get_bitac_transportista_record(self, record_id):
        f = self.bitacora_transportista_fields
        query = [
            {'$match': {
                'form_id': self.BITACORA_TRANSPORTISTAS,
                'deleted_at': {'$exists': False},
                '_id': ObjectId(record_id),
            }},
            {'$project': {
                '_id': 1,
                'folio': 1,
                'created_at': 1,
                'estatus':               f'$answers.{f["estatus"]}',
                'fecha_hora_ingreso':    f'$answers.{f["fecha_hora_ingreso"]}',
                'fecha_hora_descarga':   f'$answers.{f["fecha_hora_descarga"]}',
                'num_de_pase':           f'$answers.{f["num_de_pase"]}',
                'empresa_transportista': f'$answers.{f["empresa_transportista"]}',
                'tipo_de_operacion':     f'$answers.{f["tipo_de_operacion"]}',
                'procedencia':           f'$answers.{f["procedencia"]}',
                'tipo_de_vehiculo':      f'$answers.{f["tipo_de_vehiculo"]}',
                'placas_de_vehiculo':    f'$answers.{f["placas_de_vehiculo"]}',
                'num_eco_num_rotulo':    f'$answers.{f["num_eco_num_rotulo"]}',
                'marca_vehiculo':        f'$answers.{f["marca_vehiculo"]}',
                'year_vehiculo':         f'$answers.{f["year_vehiculo"]}',
                'color_vehiculo':        f'$answers.{f["color_vehiculo"]}',
                'conductor':             f'$answers.{f["conductor"]}',
                'ayudante':              f'$answers.{f["ayudante"]}',
                'num_licencia':          f'$answers.{f["num_licencia"]}',
                'firma_conductor':       f'$answers.{f["firma_conductor"]}',
                'anden_asignado':        f'$answers.{f["anden_asignado"]}',
                'proveedor_cliente':     f'$answers.{f["proveedor_cliente"]}',
                'orden_de_compra':       f'$answers.{f["orden_de_compra"]}',
                'documentos': {'$map': {
                    'input': {'$ifNull': [f'$answers.{f["grupo_fotos_y_documentos"]}', []]},
                    'as': 'doc',
                    'in': {
                        'tipo':      f'$$doc.{f["tipo_de_documento"]}',
                        'documento': f'$$doc.{f["documento"]}',
                    },
                }},
                'materiales': {'$map': {
                    'input': {'$ifNull': [f'$answers.{f["grupo_materiales"]}', []]},
                    'as': 'm',
                    'in': {
                        'lugar':             f'$$m.{f["lugar_material"]}',
                        'no_referencia':     f'$$m.{f["no_referencia_material"]}',
                        'producto':          f'$$m.{f["producto_material"]}',
                        'lote':              f'$$m.{f["lote_material"]}',
                        'cantidad':          f'$$m.{f["cantidad_material"]}',
                        'cantidad_fisica':   f'$$m.{f["cantidad_fisica_material"]}',
                        'peso':              f'$$m.{f["peso_material"]}',
                        'volumen':           f'$$m.{f["volumen_material"]}',
                    },
                }},
                'remolques': {'$map': {
                    'input': {'$ifNull': [f'$answers.{f["grupo_remolques"]}', []]},
                    'as': 'r',
                    'in': {
                        'tipo_remolque': f'$$r.{f["tipo_remolque"]}',
                        'no_referencia_remolque': f'$$r.{f["no_referencia_remolque"]}',
                        'no_sello':      f'$$r.{f["num_sello"]}',
                        'no_caja':       f'$$r.{f["num_caja_contenedor"]}',
                        'placas_caja':   f'$$r.{f["placas_de_caja"]}',
                        'color':         f'$$r.{f["color_remolque_contenedor"]}',
                        'comentarios':   f'$$r.{f["comentarios"]}',
                    },
                }},
                'inspecciones': {'$map': {
                    'input': {'$ifNull': [f'$answers.{f["grupo_inspecciones"]}', []]},
                    'as': 'i',
                    'in': {
                        'tipo': f'$$i.{f["tipo_inspeccion"]}',
                        'url':  f'$$i.{f["url_inspeccion"]}',
                    },
                }},
            }},
        ]
        return self.format_cr(self.cr.aggregate(query), get_one=True)
    
    def get_bitac_transportista_records(self):
        f = self.bitacora_transportista_fields
        query = [
            {'$match': {
                'form_id': self.BITACORA_TRANSPORTISTAS,
                'deleted_at': {'$exists': False},
            }},
            {'$project': {
                '_id': 1,
                'folio':              1,
                'placas':             f'$answers.{f["placas_de_vehiculo"]}',
                'proveedor_cliente':  f'$answers.{f["proveedor_cliente"]}',
                'conductor':          f'$answers.{f["conductor"]}',
                'tipo_de_operacion':  f'$answers.{f["tipo_de_operacion"]}',
                'estatus':            f'$answers.{f["estatus"]}',
                'num_de_pase':        f'$answers.{f["num_de_pase"]}',
                'fecha_hora_ingreso': f'$answers.{f["fecha_hora_ingreso"]}',
                'material': {
                    '$let': {
                        'vars': {'primer': {'$arrayElemAt': [f'$answers.{f["grupo_materiales"]}', 0]}},
                        'in': f'$$primer.{f["producto_material"]}',
                    }
                },
            }},
            {'$sort': {'_id': -1}},
        ]
        return self.format_cr(self.cr.aggregate(query))

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
                'documentos': {'$map': {
                    'input': f'$answers.{f["grupo_documentos_para_ocr"]}',
                    'as':    'doc',
                    'in': {
                        'tipo':      f'$$doc.{f["tipo_de_documento"]}',
                        'no_doc':    f'$$doc.{f["no_de_documento"]}',
                        'archivo':   f'$$doc.{f["documento_para_ocr"]}',
                    },
                }},
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

                # conductor
                'conductor_nombre':           f'$answers.{f["conductor_nombre"]}',
                'conductor_no_licencia':      f'$answers.{f["conductor_no_licencia"]}',
                'conductor_lugar_expedicion': f'$answers.{f["conductor_lugar_expedicion"]}',
                'conductor_vigencia':         f'$answers.{f["conductor_vigencia"]}',
                'conductor_foto_licencia':    f'$answers.{f["conductor_foto_licencia"]}',

                # ayudante
                'ayudante_nombre':            f'$answers.{f["ayudante_nombre"]}',
                'ayudante_no_licencia':       f'$answers.{f["ayudante_no_licencia"]}',
                'ayudante_lugar_expedicion':  f'$answers.{f["ayudante_lugar_expedicion"]}',
                'ayudante_vigencia':          f'$answers.{f["ayudante_vigencia"]}',
                'ayudante_foto_licencia':     f'$answers.{f["ayudante_foto_licencia"]}',

                # vehículo
                'vehiculo_linea':               f'$answers.{f["vehiculo_linea"]}',
                'vehiculo_tipo_unidad':         f'$answers.{f["vehiculo_tipo_unidad"]}',
                'vehiculo_marca':               f'$answers.{f["vehiculo_marca"]}',
                'vehiculo_modelo':              f'$answers.{f["vehiculo_modelo"]}',
                'vehiculo_year':                f'$answers.{f["vehiculo_year"]}',
                'vehiculo_placas':              f'$answers.{f["vehiculo_placas"]}',
                'vehiculo_no_economico':        f'$answers.{f["vehiculo_no_economico"]}',
                'vehiculo_niv':                 f'$answers.{f["vehiculo_niv"]}',
                'vehiculo_tarjeta_circulacion': f'$answers.{f["vehiculo_tarjeta_circulacion"]}',

                # contenedores
                'foto_contenedores': f'$answers.{f["foto_contenedores"]}',
                'contenedores': {'$map': {
                    'input': {'$ifNull': [f'$answers.{f["grupo_contenedores"]}', []]},
                    'as':    'row',
                    'in': {
                        'numero': f'$$row.{f["contenedor_numero"]}',
                        'sello':  f'$$row.{f["contenedor_sello"]}',
                        'tipo':   f'$$row.{f["contenedor_tipo"]}',
                    },
                }},

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

    def update_information_transportista(self, data):
        f = self.pass_fields_transportista
        record_id  = data.get('record_id')
        conductor  = data.get('conductor')
        ayudante   = data.get('ayudante')
        vehiculo   = data.get('vehiculo')
        foto_cont  = data.get('foto_contenedores')
        contenedores = data.get('contenedores')

        answers = {}

        if conductor:
            foto = conductor.get('foto') or {}
            answers.update({
                f['conductor_nombre']:           conductor.get('nombre', ''),
                f['conductor_no_licencia']:      conductor.get('licencia', ''),
                f['conductor_lugar_expedicion']: conductor.get('lugar_expedicion', ''),
                f['conductor_vigencia']:         conductor.get('vigencia', ''),
                f['conductor_foto_licencia']:    [{'file_name': foto.get('file_name', ''), 'file_url': foto['file_url']}] if foto.get('file_url') else [],
            })

        if ayudante:
            foto = ayudante.get('foto') or {}
            answers.update({
                f['ayudante_nombre']:            ayudante.get('nombre', ''),
                f['ayudante_no_licencia']:       ayudante.get('licencia', ''),
                f['ayudante_lugar_expedicion']:  ayudante.get('lugar_expedicion', ''),
                f['ayudante_vigencia']:          ayudante.get('vigencia', ''),
                f['ayudante_foto_licencia']:     [{'file_name': foto.get('file_name', ''), 'file_url': foto['file_url']}] if foto.get('file_url') else [],
            })

        if vehiculo:
            foto = vehiculo.get('foto') or {}
            answers.update({
                f['vehiculo_linea']:               vehiculo.get('linea', ''),
                f['vehiculo_tipo_unidad']:         vehiculo.get('tipo', ''),
                f['vehiculo_marca']:               vehiculo.get('marca', ''),
                f['vehiculo_modelo']:              vehiculo.get('modelo', ''),
                f['vehiculo_year']:                vehiculo.get('año', ''),
                f['vehiculo_placas']:              vehiculo.get('placas', ''),
                f['vehiculo_no_economico']:        vehiculo.get('economico', ''),
                f['vehiculo_niv']:                 vehiculo.get('niv', ''),
                f['vehiculo_tarjeta_circulacion']: [{'file_name': foto.get('file_name', ''), 'file_url': foto['file_url']}] if foto.get('file_url') else [],
            })

        if foto_cont:
            answers[f['foto_contenedores']] = [{'file_name': foto_cont.get('file_name', ''), 'file_url': foto_cont['file_url']}] if foto_cont.get('file_url') else []

        if contenedores:
            answers[f['grupo_contenedores']] = {
                -(i + 1): {
                    f['contenedor_numero']: c.get('numero', ''),
                    f['contenedor_sello']:  c.get('sello', ''),
                    f['contenedor_tipo']:   c.get('tipo', ''),
                }
                for i, c in enumerate(contenedores)
            }

        print(simplejson.dumps(answers, indent=3))
        res = self.lkf_api.patch_multi_record(
            answers=answers,
            form_id=self.PASE_ENTRADA_TRANSPORTISTA,
            record_id=[record_id],
        )
        if res.get('status_code') not in [201, 202]:
            self.LKFException({'title': 'Error al actualizar información del transportista', 'msg': res})
        return res

    def save_bitac_transportista_record(self, record_id, data):
        print(simplejson.dumps(data, indent=3))
        # breakpoint()
        f = self.bitacora_transportista_fields
        answers = {}

        vehiculo = data.get('vehiculo') or {}
        if vehiculo:
            answers.update({
                f['placas_de_vehiculo']: vehiculo.get('placas_de_vehiculo', ''),
                f['num_eco_num_rotulo']: vehiculo.get('num_eco_num_rotulo', ''),
                f['tipo_de_vehiculo']:   vehiculo.get('tipo_de_vehiculo', ''),
                f['marca_vehiculo']:     vehiculo.get('marca', ''),
                f['year_vehiculo']:      vehiculo.get('modelo', ''),
                f['color_vehiculo']:     vehiculo.get('color', ''),
            })

        embarque = data.get('embarque') or {}
        if embarque:
            answers.update({
                f['empresa_transportista']: embarque.get('procedencia', ''),
                f['proveedor_cliente']:     embarque.get('proveedor_cliente', ''),
                f['orden_de_compra']:       embarque.get('no_orden_compra', ''),
            })

        # contenedores y remolques van al mismo grupo
        remolques    = data.get('remolques', []) or []
        contenedores = data.get('contenedores', []) or []
        grupo = remolques + contenedores
        if grupo:
            answers[f['grupo_remolques']] = {
                (item['index'] if item.get('index') is not None else -(i + 1)): {
                    f['tipo_remolque']:            item.get('tipo', ''),
                    f['num_caja_contenedor']:      item.get('no_caja', ''),
                    f['num_sello']:                item.get('no_sello', ''),
                    f['placas_de_caja']:           item.get('placas', ''),
                    f['color_remolque_contenedor']: item.get('color', ''),
                    f['no_referencia_remolque']:   item.get('ref_remolque', ''),
                    f['comentarios']:              item.get('comentarios', ''),
                }
                for i, item in enumerate(grupo)
            }

        materiales = data.get('materiales', []) or []
        if materiales:
            answers[f['grupo_materiales']] = {
                (m['index'] if m.get('index') is not None else -(i + 1)): {
                    f['producto_material']:        m.get('producto', ''),
                    f['lote_material']:            m.get('lote', ''),
                    f['cantidad_material']:        m.get('cant_esperada', ''),
                    f['cantidad_fisica_material']: m.get('cant_fisica', ''),
                    f['peso_material']:            m.get('peso', ''),
                    f['volumen_material']:         m.get('volumen', ''),
                    f['no_referencia_material']:   m.get('ref', ''),
                    f['lugar_material']:           'contenedor' if str(m.get('ref', '')).startswith('contenedor') else 'remolque' if str(m.get('ref', '')).startswith('remolque') else 'vehiculo',
                }
                for i, m in enumerate(materiales)
            }

        if data.get('delete_remolques') or data.get('delete_contenedores') or data.get('delete_materiales'):
            self.delete_bitac_transportista_items(record_id, data)

        if answers:
            print(simplejson.dumps(answers, indent=3))
            res = self.lkf_api.patch_multi_record(
                answers=answers,
                form_id=self.BITACORA_TRANSPORTISTAS,
                record_id=[record_id],
            )
            print('ressssssssssss', res)
            if res.get('status_code') not in [201, 202, 203]:
                self.LKFException({'title': 'Error al guardar registro de bitácora', 'msg': res})
            return res

        return {'status_code': 200, 'msg': 'OK'}

    def delete_bitac_transportista_items(self, record_id, data):
        print(simplejson.dumps(data, indent=3))
        f = self.bitacora_transportista_fields
        current = None

        delete_remolques    = data.get('delete_remolques', []) or []
        delete_contenedores = data.get('delete_contenedores', []) or []
        delete_materiales   = data.get('delete_materiales', []) or []

        if delete_remolques or delete_contenedores:
            current = self.get_bitac_transportista_record(record_id)
            indexes_borrar = set(delete_remolques + delete_contenedores)
            nuevo_grupo = [
                {
                    f['tipo_remolque']:             r.get('tipo_remolque', ''),
                    f['num_caja_contenedor']:       r.get('no_caja', ''),
                    f['num_sello']:                 r.get('no_sello', ''),
                    f['placas_de_caja']:            r.get('placas_caja', ''),
                    f['color_remolque_contenedor']: r.get('color', ''),
                    f['no_referencia_remolque']:    r.get('no_referencia_remolque', ''),
                    f['comentarios']:               r.get('comentarios', ''),
                }
                for i, r in enumerate(current.get('remolques', []))
                if i not in indexes_borrar
            ]
            print('nuevo grupo_remolques=', simplejson.dumps(nuevo_grupo, indent=3))
            self.cr.update_one(
                {'_id': ObjectId(record_id), 'form_id': self.BITACORA_TRANSPORTISTAS, 'deleted_at': {'$exists': False}},
                {'$set': {f'answers.{f["grupo_remolques"]}': nuevo_grupo}}
            )

        if delete_materiales:
            if current is None:
                current = self.get_bitac_transportista_record(record_id)
            indexes_borrar = set(delete_materiales)
            nuevo_grupo = [
                {
                    f['lugar_material']:           m.get('lugar', ''),
                    f['no_referencia_material']:   m.get('no_referencia', ''),
                    f['producto_material']:        m.get('producto', ''),
                    f['lote_material']:            m.get('lote', ''),
                    f['cantidad_material']:        m.get('cantidad', ''),
                    f['cantidad_fisica_material']: m.get('cantidad_fisica', ''),
                    f['peso_material']:            m.get('peso', ''),
                    f['volumen_material']:         m.get('volumen', ''),
                }
                for i, m in enumerate(current.get('materiales', []))
                if i not in indexes_borrar
            ]
            print('nuevo grupo_materiales=', simplejson.dumps(nuevo_grupo, indent=3))
            self.cr.update_one(
                {'_id': ObjectId(record_id), 'form_id': self.BITACORA_TRANSPORTISTAS, 'deleted_at': {'$exists': False}},
                {'$set': {f'answers.{f["grupo_materiales"]}': nuevo_grupo}}
            )

        return {'status_code': 200, 'msg': 'OK'}
    
    def save_inspecciones(self, record_id, data):
        print(simplejson.dumps(data, indent=3))
        f_bit = self.bitacora_transportista_fields

        TRACTOR_CAMPOS = [
            'defensa',
            'motor_caja_de_la_bateria_caja_y_filtros_de_aire',
            'llantas_y_rines_tractor_y_remolque',
            'piso_tractor',
            'tanque_de_combustible',
            'cabina_dormitorio_puertas_y_compartimientos_de_herramientas_seccion_de_pasajero_y_techo',
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

        CONTENEDOR_PUNTO_MAP = {
            'Exterior / parte inferior del contenedor (bastidor o chasis)': 'exterior_parte_inferior_del_contenedor_bastidor_o_chasis',
            'Puertas interiores / exteriores':  'puertas_interiores_exteriores',
            'Pared interior lado derecho':       'pared_interior_lado_derecho',
            'Pared interior lado izquierdo':     'pared_interior_lado_izquierdo',
            'Pared interior frontal':            'pared_interior_frontal',
            'Techo / cubierta superior':         'techo_cubierta_superior',
            'Piso (interior)':                   'piso_interior',
        }

        inspecciones_creadas = []

        for inspeccion in data:
            tipo   = inspeccion.get('tipo', '')
            unidad = inspeccion.get('unidad')
            tipo_label = f'{tipo}_{unidad}' if unidad else tipo

            if tipo == 'tractor':
                puntos = inspeccion.get('puntos', [])
                if not any(p.get('resultado') for p in puntos):
                    continue
                form_id = self.INSPECCION_ENTRADA_CTPAT_TRACTOR
                f_ins   = self.inspeccion_entrada_tractor_fields
                answers = {}
                for punto in puntos:
                    num = punto.get('numero', 0) - 1
                    if 0 <= num < len(TRACTOR_CAMPOS):
                        campo = TRACTOR_CAMPOS[num]
                        resultado = (punto.get('resultado') or '').lower().replace('í', 'i')
                        if resultado:
                            answers[f_ins[campo]] = resultado
                        if punto.get('comentario'):
                            answers[f_ins[f'{campo}_comentarios']] = punto['comentario']
                        if punto.get('fotos'):
                            answers[f_ins[f'{campo}_evidencia']] = punto['fotos']

            elif tipo == 'remolque':
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
                        resultado = (punto.get('resultado') or '').lower().replace('í', 'i')
                        if resultado:
                            answers[f_ins[campo]] = resultado
                        if punto.get('comentario'):
                            answers[f_ins[f'{campo}_comentarios']] = punto['comentario']
                        if punto.get('fotos'):
                            answers[f_ins[f'{campo}_evidencia']] = punto['fotos']

            elif tipo == 'contenedor':
                filas   = inspeccion.get('filas', [])
                medidas = inspeccion.get('medidas', {}) or {}
                has_data = (
                    any(fila.get('valores') for fila in filas)
                    or any(medidas.get(k) for k in ['longitud', 'ancho', 'altura'])
                )
                if not has_data:
                    continue
                form_id = self.INSPECCION_ENTRADA_CTPAT_CONTENEDOR
                f_ins   = self.inspeccion_entrada_ctpat_contenedor_fields
                answers = {}
                if medidas.get('longitud'):
                    answers[f_ins['longitud_interior']] = medidas['longitud']
                if medidas.get('ancho'):
                    answers[f_ins['ancho_interior']] = medidas['ancho']
                if medidas.get('altura'):
                    answers[f_ins['altura_interior']] = medidas['altura']
                for fila in filas:
                    campo = CONTENEDOR_PUNTO_MAP.get(fila.get('punto', ''))
                    if not campo:
                        continue
                    valores = fila.get('valores') or []
                    if valores:
                        answers[f_ins[campo]] = [v.lower() for v in valores]
            else:
                continue

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
                self.LKFException({'title': 'Error al actualizar inspecciones en bitácora', 'msg': res_bit})

        return {'status_code': 200, 'msg': 'OK', 'inspecciones_creadas': [t for t, _ in inspecciones_creadas]}

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()
    data = script_obj.data.get('data', {})
    option = data.get("option", '')
    inspecciones = data.get("inspecciones", [])
    payload = data.get("payload", {})
    record_id = data.get("record_id", None)
    token = data.get("token", None)
    locations = data.get("locations", None)
    location = data.get("location", None)

    dispatcher = {
        "create_pass_transportista": lambda: script_obj.create_pass_transportista(payload),
        "create_visit_transportista": lambda: script_obj.create_visit_transportista(payload),
        "generate_submit_token_transportista": lambda: script_obj.generate_submit_token_transportista(record_id),
        "get_andenes": lambda: script_obj.get_andenes(),
        "get_bitac_transportista_record": lambda: script_obj.get_bitac_transportista_record(record_id),
        "get_bitac_transportista_records": lambda: script_obj.get_bitac_transportista_records(),
        "get_horarios_data": lambda: script_obj.get_horarios_data(dia=data.get('dia')),
        "get_pass_transportista": lambda: script_obj.get_pass_transportista(record_id, token),
        "get_users_data": lambda: script_obj.get_users_data(locations),
        "get_location_data": lambda: script_obj.get_location_data(location),
        "get_proveedores_transportista": lambda: script_obj.get_proveedores_transportista(),
        "validate_token": lambda: script_obj.validate_token(record_id, token),
        "update_information_transportista": lambda: script_obj.update_information_transportista(payload),
        "save_bitac_transportista_record": lambda: script_obj.save_bitac_transportista_record(record_id, payload),
        "delete_bitac_transportista_items": lambda: script_obj.delete_bitac_transportista_items(record_id, payload),
        "save_inspecciones": lambda: script_obj.save_inspecciones(record_id, inspecciones),
    }

    action = dispatcher.get(option)
    if action:
        response = action()
        print(simplejson.dumps(response, indent=4))
    else:
        response = {"error": "Opción no válida"}

    script_obj.HttpResponse({"data": response})