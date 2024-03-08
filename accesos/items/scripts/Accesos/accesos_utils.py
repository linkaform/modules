# -*- coding: utf-8 -*-
from linkaform_api import base
from lkf_addons.addons.accesos.accesos_utils import Accesos

class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        #use self.lkm.catalog_id() to get catalog id
        self.name =  __class__.__name__
        self.settings = settings

        #--Variable Catalogs
        self.ACCESOS = self.lkm.catalog_id('accesos')
        self.ACCESOS_ID = self.ACCESOS.get('id')
        self.ACCESOS_OBJ_ID = self.ACCESOS.get('obj_id')

        self.COLABORADORES_AUTORIZADOS = self.lkm.catalog_id('colaboradores_autorizados_v2')
        self.COLABORADORES_AUTORIZADOS_ID = self.COLABORADORES_AUTORIZADOS.get('id')
        self.COLABORADORES_AUTORIZADOS_OBJ_ID = self.COLABORADORES_AUTORIZADOS.get('obj_id')

        self.COLORES = self.lkm.catalog_id('colores')
        self.COLORES_ID = self.COLORES.get('id')
        self.COLORES_OBJ_ID = self.COLORES.get('obj_id')


        self.CONTRATISTAS = self.lkm.catalog_id('contratistas_v2')
        self.CONTRATISTAS_ID = self.CONTRATISTAS.get('id')
        self.CONTRATISTAS_OBJ_ID = self.CONTRATISTAS.get('obj_id')

        self.EQUIPOS = self.lkm.catalog_id('equipos')
        self.EQUIPOS_ID = self.EQUIPOS.get('id')
        self.EQUIPOS_OBJ_ID = self.EQUIPOS.get('obj_id')

        self.PERMISOS = self.lkm.catalog_id('permisos_certificaciones')
        self.PERMISOS_ID = self.PERMISOS.get('id')
        self.PERMISOS_OBJ_ID = self.PERMISOS.get('obj_id')

        self.UBICACIONES = self.lkm.catalog_id('ubicaciones')
        self.UBICACIONES_ID = self.UBICACIONES.get('id')
        self.UBICACIONES_OBJ_ID = self.UBICACIONES.get('obj_id')

        self.VEHICULOS = self.lkm.catalog_id('vehiculos')
        self.VEHICULOS_ID = self.VEHICULOS.get('id')
        self.VEHICULOS_OBJ_ID = self.VEHICULOS.get('obj_id')

        self.VISITAS = self.lkm.catalog_id('visitas')
        self.VISITAS_ID = self.VISITAS.get('id')
        self.VISITAS_OBJ_ID = self.VISITAS.get('obj_id')

        #--Variable Forms
        self.FORM_ALTA_COLABORADORES = self.lkm.form_id('alta_de_colaboradores_visitantes','id')
        self.FORM_ALTA_EQUIPOS = self.lkm.form_id('alta_de_equipos','id')
        self.FORM_ALTA_VEHICULOS = self.lkm.form_id('alta_de_vehiculos','id')
        self.FORM_BITACORA = self.lkm.form_id('bitacora','id')
        self.FORM_LOCKER = self.lkm.form_id('locker','id')
        self.FORM_PASE_DE_ENTRADA = self.lkm.form_id('pase_de_entrada','id')
        self.FORM_REGISTRO_PERMISOS = self.lkm.form_id('registro_de_permisos','id')



        self.f = {
            'colaborador_nombre_pase_entrada':'65e8aae0c56d2775fd3cb978.5ea0693a0c12d5a8e43d37df',
            'colaborador_foto_pase_entrada':'65e8aae0c56d2775fd3cb978.5ea35de83ab7dad56c66e045',
            'colaborador_curp_pase_entrada':'65e8aae0c56d2775fd3cb978.5ea0897550b8dfe1f4d83a9f',
            'colaborador_status_pase_entrada':'65e8aae0c56d2775fd3cb978.5ea1bd280ae8bad095055e61',
            'colaborador_rfc_pase_entrada':'65e8aae0c56d2775fd3cb978.64ecc95271803179d68ee081',
            'vigencia_pase_entrada':'64ef9215b2f00d5312ca2790',
            'num_access_pase_entrada':'65e1086538c02be94fb61255',
            'status_pase_entrada':'64f20b7b018d04f897432961',
            'motivo_pase_entrada':'65e0a68a06799422eded24a6',
            'visita_area_pase_entrada':'65e8aae513dc1de0cf3cb922.65e2167be938f7075f58c66c',
            'visita_ubicacion_pase_entrada':'65e8aae513dc1de0cf3cb922.65e2167be938f7075f58c66d',
            'visita_nombre_pase_entrada':'65e8aae513dc1de0cf3cb922.65e2167be938f7075f58c66e',
            'autorizo_pase_entrada':'65e0a68a06799422eded24a8',
            'autorizo_telefono_pase_entrada':'65e0a68a06799422eded24a9',
            'comentarios_comentario_pase_entrada':'65e0a68a06799422eded24aa.65e0a69a322b61fbf9ed23af',
            'permisos__pase_entrada':'65ce32228c2c5fa298cc3533.65e8aae4c56d2775fd3cb97e.65ce4128e66b6e867cf71931 ',
            'permisos_status_pase_entrada':'65ce32228c2c5fa298cc3533.65d6401cbc9e63afad61389d',
            'accesos_nombre_pase_entrada':'65e0a9be35642d47a48cdbb3.65e8aae140e295376a3cb93d',
            'vehiculos_pase_entrada':'65e0a9be35642d47a48cdbb2',
            'items_pase_entrada':'65c67e21d7add8306488f064',

            'planta_bitacora':'64ee8349998263ad9bec20cd',
            'curp_bitacora':'64ee8349998263ad9bec20cc',
            'nombre_bitacora':'5ea0693a0c12d5a8e43d37df',
            'rfc_bitacora':'64ecc95271803179d68ee081',
            'visita_bitacora':'65e0b649bc72443361f99782',
            'acceso_bitacora':'65e0b649bc72443361f99783',
            'type_user_bitacora':'65e0b649bc72443361f99784',
            'status_bitacora':'65e0b649bc72443361f99785',
            'entrada_bitacora':'65e0b649bc72443361f99786',
            'gafete_bitacora':'65e0b649bc72443361f99787',
            'date_in_bitacora':'65cbe03c6c78b071a59f481d',
            'date_out_bitacora':'65cbe03c6c78b071a59f481c',
            'duration_bitacora':'65cbe03c6c78b071a59f481e',

            'location_locker':'65e8aadfc56d2775fd3cb976.65ce4088f9f1cb6b9fcbe45f',
            'locker_locker':'65e0b6f7a07a72e587124dc3',
            'ocupado_locker':'65e0b6f7a07a72e587124dc4',
            'visitante_locker':'65e0b6f7a07a72e587124dc5',
            'documento_locker':'65e0b6f7a07a72e587124dc6',
            'numero_gafete_locker':'65e0b6f7a07a72e587124dc8',
        }

        self.fecha = self.date_from_str('2024-01-15')


    #---Format Functions 
    def get_time_format(self, date_start, date_end):
        date_start = datetime.strptime(date_start, "%Y-%m-%d %H:%M:%S")
        date_end = datetime.strptime(date_end, "%Y-%m-%d %H:%M:%S")
        secondsTotal = date_end - date_start;
        secondsTotal = int(secondsTotal.seconds)
        hours = secondsTotal // 3600
        minutes = (secondsTotal % 3600) // 60
        seconds_res = secondsTotal % 60
        return "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds_res)


    #---Query Functions
    def get_query_alerts(self, location):
        #----Filters
        self.now = self.get_today_format()
        dic_alert = {
            'count_in': 0,
            'count_out': 0,
            'count_cars_in': 0,
            'count_out_register': 0,
        }
       
        #----Query Users In
        match_query = {
            "form_id":self.FORM_BITACORA,
            #f"answers.{self.f['planta_bitacora']}":location,
            f"answers.{self.f['date_out_bitacora']}":{"$eq": ''},
            "created_at": {'$gte': self.now}
        }
        result = self.cr.find(match_query).count()
        dic_alert['count_in'] = result
        #----Query Users Out
        match_query = {
            "form_id":self.FORM_BITACORA,
            #f"answers.{self.f['planta_bitacora']}":location,
            f"answers.{self.f['date_out_bitacora']}":{"$ne": ''},
            "created_at": {'$gte': self.now}
        }
        result = self.cr.find(match_query).count()
        dic_alert['count_out'] = result
        #----Query Car In
        match_query = {
            "form_id":self.FORM_BITACORA,
            #f"answers.{self.f['planta_bitacora']}":location,
            f"answers.{self.f['date_out_bitacora']}":{"$eq": ''},
            "created_at": {'$gte': self.now}
        }
        result = self.cr.find(match_query).count()
        dic_alert['count_cars_in'] = result
        #----Query Out register
        match_query = {
            "form_id":self.FORM_BITACORA,
            #f"answers.{self.f['planta_bitacora']}":location,
            f"answers.{self.f['date_out_bitacora']}":{"$eq": ''},
            "created_at": {'$gte': self.now}
        }
        result = self.cr.find(match_query).count()
        dic_alert['count_out_register'] = result
        return dic_alert

    def get_user_information(self, curp):
        match_query = {
            "form_id":self.FORM_PASE_DE_ENTRADA,
            f"answers.{self.f['colaborador_curp_pase_entrada']}":curp,
        }

        query= [{'$match': match_query },
            {"$project":
                {
                    "_id":"$id",
                    "folio":"$folio",
                    "colaborador_nombre_pase_entrada": f"$answers.{self.f['colaborador_nombre_pase_entrada']}",
                    "colaborador_foto_pase_entrada": f"$answers.{self.f['colaborador_foto_pase_entrada']}",
                    "colaborador_curp_pase_entrada": f"$answers.{self.f['colaborador_curp_pase_entrada']}",
                    "colaborador_status_pase_entrada": f"$answers.{self.f['colaborador_status_pase_entrada']}",
                    "colaborador_rfc_pase_entrada": f"$answers.{self.f['colaborador_rfc_pase_entrada']}",
                    "vigencia_pase_entrada": f"$answers.{self.f['vigencia_pase_entrada']}",
                    "num_access_pase_entrada": f"$answers.{self.f['num_access_pase_entrada']}",
                    "status_pase_entrada": f"$answers.{self.f['status_pase_entrada']}",
                    "motivo_pase_entrada": f"$answers.{self.f['motivo_pase_entrada']}",
                    "visita_area_pase_entrada": f"$answers.{self.f['visita_area_pase_entrada']}",
                    "visita_ubicacion_pase_entrada": f"$answers.{self.f['visita_ubicacion_pase_entrada']}",
                    "visita_nombre_pase_entrada": f"$answers.{self.f['visita_nombre_pase_entrada']}",
                    "autorizo_pase_entrada": f"$answers.{self.f['autorizo_pase_entrada']}",
                    "autorizo_telefono_pase_entrada": f"$answers.{self.f['autorizo_telefono_pase_entrada']}",
                    "comentarios_comentario_pase_entrada": f"$answers.{self.f['comentarios_comentario_pase_entrada']}",
                    "permisos__pase_entrada": f"$answers.{self.f['permisos__pase_entrada']}",
                    "permisos_status_pase_entrada": f"$answers.{self.f['permisos_status_pase_entrada']}",
                    "accesos_nombre_pase_entrada": f"$answers.{self.f['accesos_nombre_pase_entrada']}",
                }
            },
        ]
        res = self.cr.aggregate(query)
        list_response = [x for x in res]
        return list_response

    def get_list_items(self, curp):
        match_query = {
            "form_id":self.FORM_PASE_DE_ENTRADA,
            f"answers.{self.f['colaborador_curp_pase_entrada']}":curp,
        }

        query= [{'$match': match_query },
            {"$project":
                {
                    "_id":"$id",
                    "folio":"$folio",
                    "items_pase_entrada": f"$answers.{self.f['items_pase_entrada']}",
                    "vehiculos_pase_entrada": f"$answers.{self.f['vehiculos_pase_entrada']}",
                }
            },
        ]
        res = self.cr.aggregate(query)
        list_response = [x for x in res]
        return list_response

    def get_list_items(self, curp):
        match_query = {
            "form_id":self.FORM_PASE_DE_ENTRADA,
            f"answers.{self.f['colaborador_curp_pase_entrada']}":curp,
        }

        query= [{'$match': match_query },
            {"$project":
                {
                    "_id":"$id",
                    "folio":"$folio",
                    "items_pase_entrada": f"$answers.{self.f['items_pase_entrada']}",
                    "vehiculos_pase_entrada": f"$answers.{self.f['vehiculos_pase_entrada']}",
                }
            },
        ]
        res = self.cr.aggregate(query)
        list_response = [x for x in res]
        return list_response

    def get_bitacora_users(self, location):
        match_query = {
            "form_id":self.FORM_LOCKER,
            f"answers.{self.f['date_out_bitacora']}":{"$ne": ''},
        }

        query= [{'$match': match_query },
            {"$project":
                {
                    "_id":"$id",
                    "folio":"$folio",
                    "planta_bitacora": f"$answers.{self.f['planta_bitacora']}",
                    "curp_bitacora": f"$answers.{self.f['curp_bitacora']}",
                    "nombre_bitacora": f"$answers.{self.f['nombre_bitacora']}",
                    "rfc_bitacora": f"$answers.{self.f['rfc_bitacora']}",
                    "visita_bitacora": f"$answers.{self.f['visita_bitacora']}",
                    "acceso_bitacora": f"$answers.{self.f['acceso_bitacora']}",
                    "type_user_bitacora": f"$answers.{self.f['type_user_bitacora']}",
                    "status_bitacora": f"$answers.{self.f['status_bitacora']}",
                    "entrada_bitacora": f"$answers.{self.f['entrada_bitacora']}",
                    "gafete_bitacora": f"$answers.{self.f['gafete_bitacora']}",
                    "date_in_bitacora": f"$answers.{self.f['date_in_bitacora']}",
                    "date_out_bitacora": f"$answers.{self.f['date_out_bitacora']}",
                    "duration_bitacora": f"$answers.{self.f['duration_bitacora']}",
                }
            },
        ]
        res = self.cr.aggregate(query)
        list_response = [x for x in res]
        return list_response

    def get_locker_users(self, location):
        match_query = {
            "form_id":self.FORM_BITACORA,
            #f"answers.{self.f['location_locker']}":{"$eq": location},
        }
        query= [{'$match': match_query },
            {"$project":
                {
                    "_id":"$id",
                    "folio":"$folio",
                    "location_locker": f"$answers.{self.f['location_locker']}",
                    "locker_locker": f"$answers.{self.f['locker_locker']}",
                    "ocupado_locker": f"$answers.{self.f['ocupado_locker']}",
                    "visitante_locker": f"$answers.{self.f['visitante_locker']}",
                    "documento_locker": f"$answers.{self.f['documento_locker']}",
                    "numero_gafete_locker": f"$answers.{self.f['numero_gafete_locker']}",
                }
            },
        ]
        res = self.cr.aggregate(query)
        list_response = [x for x in res]
        return list_response
