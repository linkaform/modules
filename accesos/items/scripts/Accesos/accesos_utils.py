# -*- coding: utf-8 -*-
from datetime import datetime
from linkaform_api import base
from lkf_addons.addons.accesos.app import Accesos



class Accesos(Accesos):
    print('Entra a acceos utils')

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def get_message_content(self):
        pass

    def get_cantidades_de_pases(self):
        print('entra a get_cantidades_de_pases')
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PASE_ENTRADA,
        }

        proyect_fields = {
            '_id':1,
            'folio': f"$folio",
            'estatus':f"$answers.{self.pase_entrada_fields['status_pase']}",
            'empresa': f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}",
            'nombre': f"$answers.{self.mf['nombre_pase']}",
            'nombre_perfil': f"$answers.{self.pase_entrada_fields['nombre_perfil']}",
            'fecha_hasta_pase': f"$answers.{self.pase_entrada_fields['fecha_hasta_pase']}",
            'created_at': 1
        }
        
        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            {'$match': {'estatus': {'$exists': True}}},
        ]

        records = self.format_cr(self.cr.aggregate(query))

        return  records
    
    def format_cantidades_de_pases(self, records):
        activos = 0
        en_proceso = 0
        vencidos = 0
        total = 0

        for pase in records:
            if pase['estatus'] == 'activo':
                activos += 1
            elif pase['estatus'] == 'proceso':
                en_proceso += 1
            elif pase['estatus'] == 'vencido':
                vencidos += 1

        total = activos + en_proceso + vencidos

        return {'activos': activos, 'en_proceso': en_proceso, 'vencidos': vencidos, 'total': total}
    
    def get_pases_x_contratista(self, empresa, records):
        activos = 0
        en_proceso = 0
        vencidos = 0
        total = 0
        ultima_fecha = None

        for pase in records:
            if empresa in pase.get('empresa', []):
                if pase['estatus'] == 'activo':
                    activos += 1
                elif pase['estatus'] == 'proceso':
                    en_proceso += 1
                elif pase['estatus'] == 'vencido':
                    vencidos += 1

                created_at = pase.get('created_at')
                if created_at:
                    if ultima_fecha is None or created_at > ultima_fecha:
                        ultima_fecha = created_at

        total = activos + en_proceso + vencidos

        return {
            'empresa': empresa,
            'activos': activos,
            'en_proceso': en_proceso,
            'vencidos': vencidos,
            'total': total,
            'fecha_ultimo_pase_emitido': ultima_fecha
        }
    
    def get_pases_x_persona(self, records):
        pases_persona = []

        for pase in records:
            aux = {}
            
            if pase.get('nombre'):
                aux['nombre'] = pase['nombre']
            else:
                aux['nombre'] = 'Información faltante'
            
            if pase.get('empresa'):
                aux['contratista'] = pase['empresa']
            else:
                aux['contratista'] = 'Información faltante'

            if pase.get('fecha_hasta_pase'):
                aux['fecha_vigencia_ultimo_pase'] = pase['fecha_hasta_pase']
            else:
                aux['fecha_vigencia_ultimo_pase'] = 'Información faltante'
            
            aux['folio'] = pase['folio']
            aux['tipo_perfil'] = pase['nombre_perfil']
            
            pases_persona.append(aux)

        return pases_persona 
    
    def get_pases_x_perfil(self, records):
        pases_por_perfil = {}

        for pase in records:
            nombre_perfil = pase.get('nombre_perfil', 'Otros')
            
            if nombre_perfil in pases_por_perfil:
                pases_por_perfil[nombre_perfil] += 1
            else:
                pases_por_perfil[nombre_perfil] = 1

        return pases_por_perfil
    
