# coding: utf-8
import dis
import sys, simplejson, pytz
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
                'fecha_hora_terminado':  f'$answers.{f["fecha_hora_terminado"]}',
                'num_de_pase':           f'$answers.{f["num_de_pase"]}',
                'empresa_transportista': f'$answers.{f["empresa_transportista"]}',
                'tipo_de_operacion':     f'$answers.{f["tipo_de_operacion"]}',
                'procedencia':           f'$answers.{f["procedencia"]}',
                'tipo_de_vehiculo':      f'$answers.{f["tipo_de_vehiculo"]}',
                'placas_de_vehiculo':                    f'$answers.{f["placas_de_vehiculo"]}',
                'placas_de_vehiculo_tarjeta_circulacion': f'$answers.{f["placas_de_vehiculo_tarjeta_circulacion"]}',
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
                'ubicacion':             f'$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf["ubicacion"]}',
                'area':                  f'$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf["nombre_area"]}',
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
                        'cantidad_buena':    f'$$m.{f["cantidad_buena_material"]}',
                        'cantidad_danada':   f'$$m.{f["cantidad_danada_material"]}',
                        'cantidad_faltante': f'$$m.{f["cantidad_faltante_material"]}',
                        'peso':              f'$$m.{f["peso_material"]}',
                        'volumen':           f'$$m.{f["volumen_material"]}',
                    },
                }},
                'desglose_empaque': {'$map': {
                    'input': {'$ifNull': [f'$answers.{f["grupo_desglose_empaque"]}', []]},
                    'as': 'd',
                    'in': {
                        'no_referencia_material': f'$$d.{f["no_referencia_material_desglose"]}',
                        'nivel':                  f'$$d.{f["nivel_desglose"]}',
                        'tipo_unidad_empaque':    f'$$d.{f["tipo_unidad_empaque_desglose"]}',
                        'cantidad':               f'$$d.{f["cantidad_desglose"]}',
                        'cantidad_acumulada':     f'$$d.{f["cantidad_acumulada_desglose"]}',
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
    
    def get_bitac_transportista_records(self, date_from=None, date_to=None,
                                         tipo_de_vehiculo=None, proveedor_cliente=None, anden_asignado=None):
        f = self.bitacora_transportista_fields
        match_filters = {
            'form_id': self.BITACORA_TRANSPORTISTAS,
            'deleted_at': {'$exists': False},
        }
        fecha_field = f'answers.{f["fecha_hora_ingreso"]}'
        if date_from and date_to:
            match_filters[fecha_field] = {'$gte': date_from, '$lte': date_to}
        elif date_from:
            match_filters[fecha_field] = {'$gte': date_from}
        elif date_to:
            match_filters[fecha_field] = {'$lte': date_to}

        if tipo_de_vehiculo:
            values = tipo_de_vehiculo if isinstance(tipo_de_vehiculo, list) else [tipo_de_vehiculo]
            match_filters[f'answers.{f["tipo_de_vehiculo"]}'] = {'$in': values}
        if proveedor_cliente:
            values = proveedor_cliente if isinstance(proveedor_cliente, list) else [proveedor_cliente]
            match_filters[f'answers.{f["proveedor_cliente"]}'] = {'$in': values}
        if anden_asignado:
            values = anden_asignado if isinstance(anden_asignado, list) else [anden_asignado]
            match_filters[f'answers.{f["anden_asignado"]}'] = {'$in': values}

        query = [
            {'$match': match_filters},
            {'$project': {
                '_id': 1,
                'folio':              1,
                'anden_asignado':     f'$answers.{f["anden_asignado"]}',
                'placas':             f'$answers.{f["placas_de_vehiculo"]}',
                'proveedor_cliente':  f'$answers.{f["proveedor_cliente"]}',
                'conductor':          f'$answers.{f["conductor"]}',
                'tipo_de_operacion':  f'$answers.{f["tipo_de_operacion"]}',
                'tipo_de_vehiculo':   f'$answers.{f["tipo_de_vehiculo"]}',
                'estatus':            f'$answers.{f["estatus"]}',
                'num_de_pase':        f'$answers.{f["num_de_pase"]}',
                'fecha_hora_ingreso': f'$answers.{f["fecha_hora_ingreso"]}',
                'material': {
                    '$let': {
                        'vars': {'primer': {'$arrayElemAt': [f'$answers.{f["grupo_materiales"]}', 0]}},
                        'in': f'$$primer.{f["producto_material"]}',
                    }
                },
                'documentos': {
                    '$let': {
                        'vars': {
                            'grupo': {'$ifNull': [f'$answers.{f["grupo_fotos_y_documentos"]}', []]},
                        },
                        'in': {
                            '$map': {
                                'input': {
                                    '$cond': {
                                        'if': {'$isArray': '$$grupo'},
                                        'then': '$$grupo',
                                        'else': {
                                            '$map': {
                                                'input': {'$objectToArray': '$$grupo'},
                                                'as': 'kv',
                                                'in': '$$kv.v',
                                            }
                                        },
                                    }
                                },
                                'as': 'row',
                                'in': {
                                    'tipo':      f'$$row.{f["tipo_de_documento"]}',
                                    'file_url':  {'$arrayElemAt': [f'$$row.{f["documento"]}.file_url', 0]},
                                    'file_name': {'$arrayElemAt': [f'$$row.{f["documento"]}.file_name', 0]},
                                },
                            }
                        },
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

    def get_pass_transportista(self, record_id=None, token=None, folio=None):
        f = self.pass_fields_transportista
        match = {
            'form_id': self.PASE_ENTRADA_TRANSPORTISTA,
            'deleted_at': {'$exists': False},
        }
        if record_id and not ObjectId.is_valid(record_id):
            folio = folio or record_id
            record_id = None
        if record_id:
            match['_id'] = ObjectId(record_id)
        elif token:
            match[f'answers.{f["token_transportista"]}'] = token
        elif folio:
            match['folio'] = folio
        else:
            self.LKFException({'title': 'Se requiere record_id, token o folio', 'status_code': 400})
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
                'empresa_transportista': f'$answers.{f["empresa_transportista"]}',

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
                        'producto':      f'$$item.{f["producto"]}',
                        'lote':          f'$$item.{f["lote"]}',
                        'no_referencia': f'$$item.{f["no_referencia"]}',
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
                'conductor_rfc':              f'$answers.{f["conductor_rfc"]}',
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
                'vehiculo_color':               f'$answers.{f["vehiculo_color"]}',
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
        result = self.format_cr(self.cr.aggregate(query), get_one=True)
        if not result and (record_id or folio):
            # El folio/_id buscado puede ser el de la bitácora (visible en el
            # kanban) y no el del pase — se liga por num_de_pase al _id del pase.
            bf = self.bitacora_transportista_fields
            bitac_match = {
                'form_id': self.BITACORA_TRANSPORTISTAS,
                'deleted_at': {'$exists': False},
            }
            if record_id:
                bitac_match['_id'] = ObjectId(record_id)
            else:
                bitac_match['folio'] = folio
            bitac = self.cr.find_one(bitac_match, {f'answers.{bf["num_de_pase"]}': 1})
            pase_record_id = bitac and bitac.get('answers', {}).get(bf['num_de_pase'])
            if pase_record_id:
                return self.get_pass_transportista(record_id=pase_record_id)
        return result

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
                f['conductor_rfc']:              conductor.get('rfc', ''),
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
                f['vehiculo_color']:               vehiculo.get('color', ''),
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

        if data.get('estatus'):
            answers[f['estatus']] = data['estatus']
            tz_name = self.user.get('timezone', 'America/Mexico_City')
            fecha_hora_actual = datetime.now(pytz.timezone(tz_name)).strftime('%Y-%m-%d %H:%M:%S')
            if data['estatus'] == 'carga_/_descarga':
                answers[f['fecha_hora_descarga']] = fecha_hora_actual
            elif data['estatus'] == 'terminado':
                answers[f['fecha_hora_terminado']] = fecha_hora_actual

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

        if data.get('anden'):
            answers[f['anden_asignado']] = data['anden']

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
                    # Los remolques solo traen no_caja; los contenedores traen
                    # además no_contenedor (su propio ID/ISO), que se prefiere
                    # cuando está presente — si no, ambos comparten esta columna.
                    f['num_caja_contenedor']:      item.get('no_contenedor') or item.get('no_caja', ''),
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
                    f['cantidad_buena_material']:   m.get('cant_buena', ''),
                    f['cantidad_danada_material']:  m.get('cant_danada', ''),
                    f['cantidad_faltante_material']: m.get('cant_faltante', ''),
                    f['peso_material']:            m.get('peso', ''),
                    f['volumen_material']:         m.get('volumen', ''),
                    f['no_referencia_material']:   m.get('ref', ''),
                    f['lugar_material']:           'contenedor' if str(m.get('ref', '')).startswith('contenedor') else 'remolque' if str(m.get('ref', '')).startswith('remolque') else 'vehiculo',
                }
                for i, m in enumerate(materiales)
            }

        # El front siempre reenvía el set completo del desglose (no ediciones
        # incrementales a filas existentes), así que se reemplaza el grupo
        # entero en vez de usar patch_multi_record con índices negativos
        # (eso solo inserta filas nuevas y duplicaría el desglose en cada guardado).
        if 'desglose_empaque' in data:
            nuevo_grupo_desglose = [
                {
                    f['no_referencia_material_desglose']: d.get('no_referencia_material', ''),
                    f['nivel_desglose']:                  d.get('nivel', ''),
                    f['tipo_unidad_empaque_desglose']:    d.get('tipo_unidad_empaque', ''),
                    f['cantidad_desglose']:               d.get('cantidad', ''),
                    f['cantidad_acumulada_desglose']:     d.get('cantidad_acumulada', ''),
                }
                for d in (data.get('desglose_empaque') or [])
            ]
            self.cr.update_one(
                {'_id': ObjectId(record_id), 'form_id': self.BITACORA_TRANSPORTISTAS, 'deleted_at': {'$exists': False}},
                {'$set': {f'answers.{f["grupo_desglose_empaque"]}': nuevo_grupo_desglose}}
            )

        documentos_raw = data.get('documentos_adicionales') or []
        if isinstance(documentos_raw, dict):
            documentos_raw = [documentos_raw]
        if documentos_raw:
            new_counter = -1
            docs_patch = {}
            for doc in documentos_raw:
                idx = doc.get('index')
                if idx is not None:
                    key = idx
                else:
                    key = new_counter
                    new_counter -= 1
                docs_patch[key] = {
                    f['tipo_de_documento']: doc.get('tipo', '').lower().replace(' ', '_'),
                    f['documento']: [{'file_name': doc['file_name'], 'file_url': doc['file_url']}],
                }
            answers[f['grupo_fotos_y_documentos']] = docs_patch

        if data.get('delete_remolques') or data.get('delete_contenedores') or data.get('delete_materiales') or data.get('delete_documentos'):
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
        delete_documentos   = data.get('delete_documentos', []) or []

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
                    f['cantidad_buena_material']:   m.get('cantidad_buena', ''),
                    f['cantidad_danada_material']:  m.get('cantidad_danada', ''),
                    f['cantidad_faltante_material']: m.get('cantidad_faltante', ''),
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

        if delete_documentos:
            if current is None:
                current = self.get_bitac_transportista_record(record_id)
            indexes_borrar = set(delete_documentos)
            nuevo_grupo = [
                {
                    f['tipo_de_documento']: d.get('tipo', ''),
                    f['documento']:         d.get('documento', []),
                }
                for i, d in enumerate(current.get('documentos', []))
                if i not in indexes_borrar
            ]
            print('nuevo grupo_fotos_y_documentos=', simplejson.dumps(nuevo_grupo, indent=3))
            self.cr.update_one(
                {'_id': ObjectId(record_id), 'form_id': self.BITACORA_TRANSPORTISTAS, 'deleted_at': {'$exists': False}},
                {'$set': {f'answers.{f["grupo_fotos_y_documentos"]}': nuevo_grupo}}
            )

        return {'status_code': 200, 'msg': 'OK'}
    
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
        """Etapas activas del flujo de transportistas para esta cuenta.
        Registro singleton (un solo record) en la forma "Configuración de Flujo de
        Transportistas". Si no existe el registro todavía, regresa las 3 etapas
        opcionales activas (fail-open, mismo comportamiento que antes de este toggle)."""
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
        return {'etapas_activas': etapas_activas}

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
                    if labeled.get(key):
                        fotos.extend(labeled[key])
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