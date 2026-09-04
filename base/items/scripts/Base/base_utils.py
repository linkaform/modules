# -*- coding: utf-8 -*-
from datetime import datetime
from linkaform_api import base
from lkf_addons.addons.base.app import Base


class Base(Base):
    print('Entra a base utils')

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.load(module='Accesos', **self.kwargs)
        self.load(module='Employee', **self.kwargs)

        self.Accesos.checkin_fields.update({
            'nombre_suplente':'6927a1176c60848998a157a2'
        })

        self.Accesos.pase_entrada_fields.update({
            'grupo_vehiculos':'663e446cadf967542759ebba',
        })

        self.Accesos.cons_f.update({
            'quien_recibe_otro': '69c47a1ce96590f9dbf494b0',
        })

        self.module_permits = {
            'always':{
                'forms':[],
                'catalogs':[
                    self.Accesos.ACTIVOS_FIJOS_CAT_ID,
                    self.Accesos.AREAS_DE_LAS_UBICACIONES_CAT_ID,
                    self.Accesos.CATEGORIAS_INCIDENCIAS_ID,
                    self.Accesos.CONFIGURACION_RECORRIDOS_ID,
                    self.Accesos.CONF_AREA_EMPLEADOS_AP_CAT_ID,
                    self.Accesos.CONF_AREA_EMPLEADOS_CAT_ID,
                    self.Accesos.ESTADO_ID,
                    self.Accesos.LISTA_FALLAS_CAT_ID,
                    self.Accesos.LISTA_INCIDENCIAS_CAT_ID,
                    self.Accesos.LOCKERS_CAT_ID,
                    self.Accesos.PASE_ENTRADA_ID,
                    self.Accesos.PROVEEDORES_CAT_ID,
                    self.Accesos.SUB_CATEGORIAS_INCIDENCIAS_ID,
                    self.Accesos.TIPO_ARTICULOS_PERDIDOS_CAT_ID,
                    self.Accesos.TIPO_DE_EQUIPO_ID,
                    self.Accesos.UBICACIONES_CAT_ID,
                    self.Accesos.USUARIOS_ID,
                    self.Accesos.VISITA_AUTORIZADA_CAT_ID,
                    self.Accesos.MENUS_CATALOG_ID,
                    self.OCR_DOCS,
                    self.SCRIPT_PASE_ACCESO,
                    self.SCRIPT_PASE_ACCESO_API,
                ],
                'scripts':[self.OFFLINE_SERVICES, self.SCRIPT_MENUS, self.FILTERS, self.SCRIPT_TRANSPORTISTAS]
            },
            'accesos':{
                'forms':[self.Accesos.CHECKIN_CASETAS, self.Accesos.REGISTRO_ASISTENCIA, self.Accesos.BITACORA_GAFETES_LOCKERS, self.Accesos.CHECK_UBICACIONES, self.Accesos.BITACORA_ACCESOS],
                'catalogs':[],
                'scripts':[]
            },
            'seguridad':{
                'forms':[self.Accesos.CONFIGURACION_RECORRIDOS_FORM, self.Accesos.BITACORA_RONDINES, self.Accesos.BITACORA_FALLAS, self.Accesos.BITACORA_INCIDENCIAS],
                'catalogs':[],
                'scripts':[self.SCRIPT_RONDINES, self.FALLAS, self.SCRIPT_INCIDENCIAS]
            },
            'activos':{
                'forms':[self.Accesos.CONCESSIONED_ARTICULOS, self.Accesos.BITACORA_OBJETOS_PERDIDOS],
                'catalogs':[self.Accesos.ACTIVOS_FIJOS_CAT_ID, ],
                'scripts':[self.Accesos.PAQUETERIA, self.GET_STATS, self.GAFETES_LOCKERS, self.FALLAS, self.ARTICULOS_PERDIDOS, self.ARTICULOS_CONSECIONADOS]
            },
            'notas':{
                'forms':[self.ACCESOS_NOTAS],
                'catalogs':[],
                'scripts':[self.NOTAS]
            },
            'pases_de_entrada':{
                'forms':[self.Accesos.PASE_ENTRADA],
                'catalogs':[],
                'scripts':[self.SCRIPT_PASE_ACCESO, self.GET_STATS, self.SCRIPT_PASE_ACCESO_API]
            },
            'caseta':{
                'forms':[self.Accesos.CHECKIN_CASETAS, self.Accesos.REGISTRO_ASISTENCIA, self.Accesos.FORMATO_VACACIONES],
                'catalogs':[],
                'scripts':[self.SCRIPT_TURNOS]
            },
        }

        # En mobile los mismos módulos de permisos viven bajo otros labels
        # (Turnos, Bitácoras, Artículos, Rondines) — alias al mismo config de Web.
        self.module_permits['turnos'] = self.module_permits['caseta']
        self.module_permits['rondines'] = self.module_permits['seguridad']
        self.module_permits['artículos'] = self.module_permits['activos']
        self.module_permits['bitácoras'] = {
            'forms': self.module_permits['accesos']['forms'] + self.module_permits['seguridad']['forms'],
            'catalogs': self.module_permits['accesos']['catalogs'] + self.module_permits['seguridad']['catalogs'],
            'scripts': self.module_permits['accesos']['scripts'] + self.module_permits['seguridad']['scripts'],
        }
