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
                'catalogs':[self.Accesos.MENUS_CATALOG_ID],
                'scripts':[
                    self.OFFLINE_SERVICES,
                    self.SCRIPT_MENUS,
                    self.FILTERS,
                    self.SCRIPT_TRANSPORTISTAS,
                    self.SCRIPT_PASE_ACCESO,
                    self.GET_STATS,
                    self.SCRIPT_PASE_ACCESO_API,
                    self.OCR_DOCS,
                ]
            },
            'accesos':{
                'forms':[self.Accesos.CHECKIN_CASETAS, self.Accesos.REGISTRO_ASISTENCIA, self.Accesos.BITACORA_GAFETES_LOCKERS, self.Accesos.CHECK_UBICACIONES, self.Accesos.BITACORA_ACCESOS],
                'catalogs':[],
                'scripts':[self.OCR_DOCS]
            },
            'seguridad':{
                'forms':[self.Accesos.CONFIGURACION_RECORRIDOS_FORM, self.Accesos.BITACORA_RONDINES, self.Accesos.BITACORA_FALLAS, self.Accesos.BITACORA_INCIDENCIAS],
                'catalogs':[],
                'scripts':[self.SCRIPT_RONDINES, self.FALLAS, self.SCRIPT_INCIDENCIAS]
            },
            'activos':{
                'forms':[self.Accesos.CONCESSIONED_ARTICULOS, self.Accesos.BITACORA_OBJETOS_PERDIDOS, self.Accesos.PAQUETERIA],
                'catalogs':[],
                'scripts':[self.Accesos.SCRIPT_PAQUETERIA, self.GET_STATS, self.GAFETES_LOCKERS, self.FALLAS, self.ARTICULOS_PERDIDOS, self.ARTICULOS_CONSECIONADOS]
            },
            'notas':{
                'forms':[self.Accesos.ACCESOS_NOTAS],
                'catalogs':[],
                'scripts':[self.SCRIPT_NOTAS]
            },
            'pases_de_entrada':{
                'forms':[self.Accesos.PASE_ENTRADA],
                'catalogs':[],
                'scripts':[self.SCRIPT_PASE_ACCESO, self.GET_STATS, self.SCRIPT_PASE_ACCESO_API]
            },
            'caseta':{
                'forms':[self.Accesos.CHECKIN_CASETAS, self.Accesos.REGISTRO_ASISTENCIA, self.Accesos.FORMATO_VACACIONES, self.Accesos.ACCESOS_NOTAS],
                'catalogs':[],
                'scripts':[self.SCRIPT_TURNOS, self.SCRIPT_NOTAS]
            },
        }

        # Cada script se comparte junto con su gemelo *_sdk (Sanic). getattr: la imagen
        # de prod puede traer un lkf_addons anterior sin SCRIPTS_SDK.
        # Solo entran los ids validos que si tienen gemelo: {} (script que la cuenta no
        # tiene) no sirve de llave y .get() metia None.
        scripts_sdk = getattr(self, 'SCRIPTS_SDK', {})
        for config in self.module_permits.values():
            config['scripts'] += [scripts_sdk[s] for s in config['scripts'] if isinstance(s, int) and s in scripts_sdk]

        # En mobile los mismos módulos viven bajo otros labels (Turnos, Bitácoras,
        # Artículos, Rondines, Incidencias / Fallas). Son copias, no alias, para que los
        # catálogos offline de la app no les lleguen a los usuarios que solo usan web.
        catalogos_app = self.Accesos.catalogos_offline_por_menu

        def movil(*webs, menu):
            return {
                'forms': [x for web in webs for x in web['forms']],
                'catalogs': [x for web in webs for x in web['catalogs']] + list(catalogos_app.get(menu, [])),
                'scripts': [x for web in webs for x in web['scripts']],
            }

        mp = self.module_permits
        mp['turnos'] = movil(mp['caseta'], menu='turnos')
        mp['rondines'] = movil(mp['seguridad'], menu='rondines')
        mp['artículos'] = movil(mp['activos'], menu='artículos')
        mp['bitácoras'] = movil(mp['accesos'], mp['seguridad'], menu='bitácoras')
        mp['incidencias_/_fallas'] = movil(menu='incidencias_/_fallas')

        # Catálogos que antes iban en 'always': a los usuarios que ya los tienen no se
        # les quitan en esta fase (set_item_permits los protege), solo ya no se dan.
        A = self.Accesos
        self.catalogos_sin_descompartir = {x for x in (
            A.ACTIVOS_FIJOS_CAT_ID, A.AREAS_DE_LAS_UBICACIONES_CAT_ID, A.CATEGORIAS_INCIDENCIAS_ID,
            A.CONFIGURACION_RECORRIDOS_ID, A.CONF_AREA_EMPLEADOS_AP_CAT_ID, A.CONF_AREA_EMPLEADOS_CAT_ID,
            A.ESTADO_ID, A.LISTA_FALLAS_CAT_ID, A.LISTA_INCIDENCIAS_CAT_ID, A.LOCKERS_CAT_ID,
            A.PASE_ENTRADA_ID, A.PROVEEDORES_CAT_ID, A.SUB_CATEGORIAS_INCIDENCIAS_ID,
            A.TIPO_ARTICULOS_PERDIDOS_CAT_ID, A.TIPO_DE_EQUIPO_ID, A.UBICACIONES_CAT_ID,
            A.USUARIOS_ID, A.VISITA_AUTORIZADA_CAT_ID, A.MENUS_CATALOG_ID,
        ) if isinstance(x, int)}
