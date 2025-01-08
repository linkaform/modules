# -*- coding: utf-8 -*-
from datetime import datetime
from linkaform_api import base
from lkf_addons.addons.accesos.app import Accesos



class Accesos(Accesos):
    print('Entra a acceos utils')

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.bitacora_fields.update({
            "catalogo_pase_entrada": "66a83ad652d2643c97489d31",
            "gafete_catalog": "66a83ace56d1e741159ce114"
        })

        self.consecionados_fields.update({
            "catalogo_ubicacion_concesion": "66a83a74de752e12018fbc3c",
        })


    def get_cantidades_de_pases(self, x_empresa=False):
        print('entra a get_cantidades_de_pases')
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PASE_ENTRADA,
        }

        proyect_fields = {
            '_id':1,
            'folio': f"$folio",
            'estatus':f"$answers.{self.pase_entrada_fields['status_pase']}",
            'empresa': { "$first" : f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}"},
            'nombre': f"$answers.{self.mf['nombre_pase']}",
            'nombre_perfil': f"$answers.{self.pase_entrada_fields['nombre_perfil']}",
            'fecha_hasta_pase': f"$answers.{self.pase_entrada_fields['fecha_hasta_pase']}",
            'created_at': 1
        }

        match_query.update({f"answers.{self.pase_entrada_fields['status_pase']}":{'$exists': True}})

        post_project_match = {}

        group_by = {
                '_id':{
                    'estatus': '$estatus',
                    },
                'cantidad': {'$sum': 1},
                }
        
        if x_empresa:
            post_project_match = {
                "$and": [
                    {'empresa': {"$ne": None}},
                    {'empresa': {"$ne": ""}}
                ]
            }

            group_by = {
                '_id':{
                    'empresa':'$empresa',
                    'estatus': '$estatus',
                    },
                'cantidad': {'$sum': 1},
            }

        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            {'$match': post_project_match},
            {'$group': group_by}
        ]

        records = self.format_cr(self.cr.aggregate(query))
        print('/////////records', records)
        return  records
    
    def get_cantidades_de_pases_x_persona(self, contratista=None):
        print('entra a get_cantidades_de_pases_x_persona')
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PASE_ENTRADA,
        }

        if contratista:
            match_query.update({f"answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}":contratista})

        proyect_fields = {
            '_id':1,
            'folio': f"$folio",
            'estatus':f"$answers.{self.pase_entrada_fields['status_pase']}",
            'empresa': { "$first" : f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}"},
            'nombre': f"$answers.{self.mf['nombre_pase']}",
            'nombre_perfil': f"$answers.{self.pase_entrada_fields['nombre_perfil']}",
            'fecha_hasta_pase': f"$answers.{self.pase_entrada_fields['fecha_hasta_pase']}",
            'created_at': 1
        }

        match_query.update({f"answers.{self.pase_entrada_fields['status_pase']}":{'$exists': True}})

        group_by = {
                '_id':{
                    'folio':'$folio',
                    'nombre': '$nombre',
                    'empresa': '$empresa',
                    'nombre_perfil': '$nombre_perfil',
                    'fecha_hasta_pase': '$fecha_hasta_pase',
                    }
                }
        

        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            {'$group': group_by}
        ]

        records = self.format_cr(self.cr.aggregate(query))
        print('/////////records', records)
        return  records
    

    def get_page_stats(self, booth_area, location, page=''):
        print('entra a get_booth_stats')
        print('booth_area', booth_area)
        print('location', location)
        today = datetime.today().strftime("%Y-%m-%d")
        res={}

        if page == 'Turnos':
            #Visitas dentro, Gafetes pendientes y Vehiculos estacionados
            match_query_visitas = {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_ACCESOS,
                f"answers.{self.bitacora_fields['status_visita']}": "entrada",
                f"answers.{self.bitacora_fields['catalogo_pase_entrada']}.{self.pase_entrada_fields['status_pase']}": {"$in": ["Activo"]},
                f"answers.{self.bitacora_fields['caseta_entrada']}": booth_area,
                f"answers.{self.bitacora_fields['ubicacion']}": location,
            }

            proyect_fields_visitas = {
                '_id': 1,
                'vehiculos': {"$ifNull": [f"$answers.{self.mf['grupo_vehiculos']}", []]},
                'id_gafete': f"$answers.{self.bitacora_fields['gafete_catalog']}.{self.gafetes_fields['gafete_id']}",
                'status_gafete': f"$answers.{self.mf['status_gafete']}"
            }

            group_by_visitas = {
                '_id': None,
                'total_visitas_dentro': {'$sum': 1},
                'total_vehiculos_dentro': {'$sum': {'$size': '$vehiculos'}},
                'gafetes_info': {
                    '$push': {
                        'id_gafete':'$id_gafete',
                        'status_gafete':'$status_gafete'
                    }
                }
            }

            query_visitas = [
                {'$match': match_query_visitas},
                {'$project': proyect_fields_visitas},
                {'$group': group_by_visitas}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_visitas))
            total_vehiculos_dentro = resultado[0]['total_vehiculos_dentro'] if resultado else 0
            total_visitas_dentro = resultado[0]['total_visitas_dentro'] if resultado else 0
            gafetes_info = resultado[0]['gafetes_info'] if resultado else []
            gafetes_pendientes = sum(1
                for gafete in gafetes_info
                    if gafete.get('id_gafete') and gafete.get('status_gafete', '').lower() != 'entregado'
            )
            
            res['total_vehiculos_dentro'] = total_vehiculos_dentro
            res['in_invitees'] = total_visitas_dentro
            res['gafetes_pendientes'] = gafetes_pendientes

            #Articulos concesionados
            match_query_concesionados = {
                "deleted_at": {"$exists": False},
                "form_id": self.CONCESSIONED_ARTICULOS,
                f"answers.{self.consecionados_fields['catalogo_ubicacion_concesion']}.{self.mf['ubicacion']}": location,
            }

            proyect_fields_concesionados = {
                '_id': 1,
            }

            group_by_concesionados = {
                '_id': None,
                'articulos_concesionados': {'$sum': 1}
            }

            query_concesionados = [
                {'$match': match_query_concesionados},
                {'$project': proyect_fields_concesionados},
                {'$group': group_by_concesionados}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_concesionados))
            articulos_concesionados = resultado[0]['articulos_concesionados'] if resultado else 0
            
            res['articulos_concesionados'] = articulos_concesionados

            #Incidentes pendientes
            match_query_incidentes = {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_INCIDENCIAS,
                f"answers.{self.incidence_fields['area_incidencia_catalog']}.{self.incidence_fields['area_incidencia']}": booth_area,
                f"answers.{self.incidence_fields['ubicacion_incidencia_catalog']}.{self.incidence_fields['ubicacion_incidencia']}": location
            }

            proyect_fields_incidentes = {
                '_id': 1,
                'acciones_tomadas_incidencia': f"$answers.{self.incidence_fields['acciones_tomadas_incidencia']}",
            }

            group_by_incidentes = {
                '_id': None,
                'incidentes_pendientes': {'$sum': {'$cond': [{'$or': [{'$eq': [{'$size': {'$ifNull': ['$acciones_tomadas_incidencia', []]}}, 0]},{'$eq': ['$acciones_tomadas_incidencia', None]}]}, 1, 0]}}
            }

            query_incidentes = [
                {'$match': match_query_incidentes},
                {'$project': proyect_fields_incidentes},
                {'$group': group_by_incidentes}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_incidentes))
            incidentes_pendientes = resultado[0]['incidentes_pendientes'] if resultado else 0
            
            res['incidentes_pendites'] = incidentes_pendientes
        elif page == 'Accesos' or page == 'Bitacoras':
            #Visitas en el dia, personal dentro, vehiculos dentro y salidas registradas
            match_query_visitas = {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_ACCESOS,
                # f"answers.{self.bitacora_fields['status_visita']}": "entrada",
                f"answers.{self.bitacora_fields['catalogo_pase_entrada']}.{self.pase_entrada_fields['status_pase']}": {"$in": ["Activo"]},
                f"answers.{self.bitacora_fields['caseta_entrada']}": booth_area,
                f"answers.{self.bitacora_fields['ubicacion']}": location,
                f"answers.{self.mf['fecha_entrada']}": {"$gte": today,"$lt": f"{today}T23:59:59"}
            }

            proyect_fields_visitas = {
                '_id': 1,
                'vehiculos': {"$ifNull": [f"$answers.{self.mf['grupo_vehiculos']}", []]},
                'perfil': f"$answers.{self.bitacora_fields['catalogo_pase_entrada']}.{self.mf['nombre_perfil']}",
                'status_visita': f"$answers.{self.bitacora_fields['status_visita']}"
            }

            group_by_visitas = {
                '_id': None,
                'visitas_en_dia': {'$sum': 1},
                'total_vehiculos_dentro': {'$sum': {'$size': '$vehiculos'}},
                'detalle_visitas': {
                    '$push': {
                        'perfil': '$perfil',
                        'status_visita': '$status_visita'
                    }
                }
            }

            query_visitas = [
                {'$match': match_query_visitas},
                {'$project': proyect_fields_visitas},
                {'$group': group_by_visitas}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_visitas))
            # print('resultadooooooooooooooooo',resultado)
            total_vehiculos_dentro = resultado[0]['total_vehiculos_dentro'] if resultado else 0
            visitas_en_dia = resultado[0]['visitas_en_dia'] if resultado else 0
            detalle_visitas = resultado[0]['detalle_visitas'] if resultado else []
            personal_dentro = sum(1 for visita in detalle_visitas if visita['perfil'][0].lower() != "visita general")
            salidas = sum(1 for visita in detalle_visitas if visita['status_visita'].lower() == "salida")

            res['total_vehiculos_dentro'] = total_vehiculos_dentro
            res['visitas_en_dia'] = visitas_en_dia
            res['personal_dentro'] = personal_dentro
            res['salidas_registradas'] = salidas
        elif page == 'Incidencias':
            #Incidentes por dia
            match_query_incidentes = {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_INCIDENCIAS,
                f"answers.{self.incidence_fields['area_incidencia_catalog']}.{self.incidence_fields['area_incidencia']}": booth_area,
                f"answers.{self.incidence_fields['area_incidencia_catalog']}.{self.incidence_fields['area_incidencia']}": booth_area,
                f"answers.{self.incidence_fields['ubicacion_incidencia_catalog']}.{self.incidence_fields['ubicacion_incidencia']}": location,
                f"answers.{self.incidence_fields['fecha_hora_incidencia']}": {"$gte": today,"$lt": f"{today}T23:59:59"}
            }

            proyect_fields_incidentes = {
                '_id': 1,
            }

            group_by_incidentes = {
                '_id': None,
                'incidentes_x_dia': {'$sum': 1}
            }

            query_incidentes = [
                {'$match': match_query_incidentes},
                {'$project': proyect_fields_incidentes},
                {'$group': group_by_incidentes}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_incidentes))
            incidentes_x_dia = resultado[0]['incidentes_x_dia'] if resultado else 0

            res['incidentes_x_dia'] = incidentes_x_dia

            #Fallas pendientes
            match_query_fallas = {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_FALLAS,
                f"answers.{self.fallas_fields['falla_ubicacion_catalog']}.{self.fallas_fields['falla_caseta']}": booth_area,
                f"answers.{self.fallas_fields['falla_ubicacion_catalog']}.{self.fallas_fields['falla_ubicacion']}": location,
                f"answers.{self.fallas_fields['falla_estatus']}": 'abierto',
                # f"answers.{self.incidence_fields['fecha_hora_incidencia']}": {"$gte": today,"$lt": f"{today}T23:59:59"}
            }

            proyect_fields_fallas = {
                '_id': 1,
            }

            group_by_fallas = {
                '_id': None,
                'fallas_pendientes': {'$sum': 1}
            }

            query_fallas = [
                {'$match': match_query_fallas},
                {'$project': proyect_fields_fallas},
                {'$group': group_by_fallas}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_fallas))
            fallas_pendientes = resultado[0]['fallas_pendientes'] if resultado else 0

            res['fallas_pendientes'] = fallas_pendientes
        elif page == 'Articulos':
            #Articulos concesionados pendientes
            match_query_concesionados = {
                "deleted_at": {"$exists": False},
                "form_id": self.CONCESSIONED_ARTICULOS,
                f"answers.{self.consecionados_fields['catalogo_ubicacion_concesion']}.{self.mf['ubicacion']}": location,
                f"answers.{self.consecionados_fields['status_concesion']}": "abierto",
            }

            proyect_fields_concesionados = {
                '_id': 1,
            }

            group_by_concesionados = {
                '_id': None,
                'articulos_concesionados_pendientes': {'$sum': 1}
            }

            query_concesionados = [
                {'$match': match_query_concesionados},
                {'$project': proyect_fields_concesionados},
                {'$group': group_by_concesionados}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_concesionados))
            articulos_concesionados_pendientes = resultado[0]['articulos_concesionados_pendientes'] if resultado else 0
            
            res['articulos_concesionados_pendientes'] = articulos_concesionados_pendientes

            #Articulos perdidos
            match_query_perdidos = {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_OBJETOS_PERDIDOS,
                f"answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.perdidos_fields['ubicacion_perdido']}": location,
                f"answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.perdidos_fields['area_perdido']}": booth_area,
            }

            proyect_fields_perdidos = {
                '_id': 1,
                'status_perdido': f"$answers.{self.perdidos_fields['estatus_perdido']}",
            }

            group_by_perdidos = {
                '_id': None,
                'perdidos_info': {
                    '$push': {
                        'status_perdido':'$status_perdido'
                    }
                }
            }

            query_perdidos = [
                {'$match': match_query_perdidos},
                {'$project': proyect_fields_perdidos},
                {'$group': group_by_perdidos}
            ]

            resultado = self.format_cr(self.cr.aggregate(query_perdidos))
            perdidos_info = resultado[0]['perdidos_info'] if resultado else []

            articulos_perdidos = 0
            for perdido in perdidos_info:
                status_perdido = perdido.get('status_perdido', '').lower()
                if status_perdido not in ['entregado', 'donado']:
                    articulos_perdidos += 1

            res['articulos_perdidos'] = articulos_perdidos

        # res ={
        #         "in_invitees":0,
        #         "articulos_concesionados":0,
        #         "incidentes_pendites": incidentes_pendientes,
        #         "vehiculos_estacionados": total_vehiculos,
        #         "gefetes_pendientes": 0,
        #     }
        return res