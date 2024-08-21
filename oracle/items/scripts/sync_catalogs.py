# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from oracle import Oracle

class Oracle(Oracle):
    pass

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        #use self.lkm.catalog_id() to get catalog id
        self.name =  __class__.__name__
        self.settings = settings
        self.etl_values = {
            'A':'Activo',
            'I':'Activo',
            'H':'Hombre',
            'M':'Mujer',
            'D':'Disponible',
            'ND':'NoDisponible',
        }
        self.views = {
            # 'LINK_PAIS':{
            #     'catalog_id': self.COUNTRY_ID,
            #     'schema':{
            #         'PAISID': '_id',
            #         'DESCRIPCION': self.f['country'],
            #         }
            # },
            # 'LINK_PROVINCIAS':{
            #     'catalog_id': self.ESTADO_ID,
            #     'schema':{
            #         'PROVINCIAID': '_id',
            #         'DESCRIPCION': self.f['state'],
            #         'PAIS': self.f['country'],
            #         }
            #     },
            # 'LINK_DEPARTAMENTO':{
            #     'catalog_id': self.DEPARTAMENTOS_ID,
            #     'schema':{
            #         'DEPARTAMENTOID': '_id',
            #         'DESCRIPCION': self.f['worker_department'],
            #         }
            #     },
            # 'LINK_PUESTO':{
            #     'catalog_id': self.PUESTOS_ID,
            #     'schema':{
            #         'CARGOID': '_id',
            #         'DESCRIPCION': self.f['worker_position'],
            #         }
            # },
            'LINK_EMPLEADOS':{
                'catalog_id': self.EMPLOYEE_ID,
                'schema':{
                    'CONTACTOID': '_id',
                    'DEPARTAMENTO': self.f['worker_department'],
                    'EMAIL': self.f['email'],
                    'ESTADO_EMPLEADO': self.f['estatus_dentro_empresa'],
                    'GENERO': self.f['genero'],
                    'PUESTO': self.f['worker_position'],
                    'RAZON_SOCIAL': self.f['worker_name'],
                    'TELEFONO1': self.f['telefono1'],
                    # 'PAIS': self.f['country'],
                    # 'PROVINCIA': self.f['state'],
                    # 'CODIGOPOSTAL': self.f['state'],
                    # 'DIRECCION': self.f['state'],
                    }
                },
            # 'LINK_FABRICANTE':{
            #     'catalog_id': self.MARCA_ID,
            #     'schema':{
            #         'MARCAID': '_id',
            #         'DESCRIPCION': self.f['marca'],
            #         }
            #     },
            # 'LINK_MODELO_EQUIPOS':{
            #     'catalog_id': self.MODELO_ID,
            #     'schema':{
            #         'MODELOID': '_id',
            #         'MARCA': self.f['marca'],
            #         'MODELO': self.f['modelo'],
            #         'TIPO_MAQ': self.f['categoria_marca'],
            #         }
            #     },
            # 'LINK_CLIENTES':{
            #     'catalog_id': self.CLIENTE_CAT_ID,
            #     'schema':{
            #         'CONTACTOID': '_id',
            #         'NOMBRE_COMERCIAL': self.f['nombre_comercial'],
            #         'RAZON_SOCIAL': self.f['razon_social'],
            #         'RUC': self.f['rfc_razon_social'],
            #         }
            # },
            # 'LINK_EQUIPOS':{
            #     'catalog_id': self.ACTIVOS_FIJOS_CAT_ID,
            #     'schema':{
            #         'VEHICULO_TALID': '_id',
            #         'CATEGORIA': self.f['categoria_marca'],
            #         'EQUIPO': self.f['tipo_equipo'],
            #         'FABRICANTE': self.f['marca'],
            #         'MODELO': self.f['modelo'],
            #         'NOMBRE': self.f['nombre_equipo'],
            #         'CHASIS': self.f['numero_de_serie_chasis'],
            #         'CLIENTE': self.f['nombre_comercial'],
            #         'ULTIMO_HOROMETRO': self.f['ultimo_horometro'],
            #         'FEC_ULT_HOROMETRO': self.f['fecha_horometro'],
            #         'ESTATUS': self.f['estatus'],
            #         'ESTADO': self.f['estado'],
            #         },
            #     }
            }
        self.aux_view = {
            'EQUIPOS':{
                'catalog_id': self.TIPO_DE_EQUIPO_ID,
                'schema':{
                    'VEHICULO_TALID': '_id',
                    'EQUIPO': self.f['tipo_equipo'],
                    }
            }
            }


    def api_etl(self, data, schema):
        translated_dict = {}
        for key, value in data.items():
            if schema.get(key) and value:
                if self.etl_values.get(value):
                    translated_dict[schema[key]] = self.etl_values[value]
                else:
                    translated_dict[schema[key]] = value
        # translated_dict = {schema[key]: value for key, value in data.items() if schema.get(key) and if value}
        return translated_dict

if __name__ == "__main__":
    module_obj = Oracle(settings, sys_argv=sys.argv)
    module_obj.console_run()
    # gg = module_obj.search_views()
    # print('gg',gg)
    data = module_obj.data.get('data',{})
    option = data.get("option",'read')

    print('data',  module_obj.ESTADO_ID)
    db_name = data.get('db_name',"LINK_EMPLEADOS")

    #-FUNCTIONS
    print('option', option)
    if option == 'read':

        # print('module_obj.views.keys()', module_obj.views)
        views = list(module_obj.views.keys())
        equipos = []
        equipos_row = []
        equipos_schema:{
                    'VEHICULO_TALID': '_id',
                    'DESCRIPCION': module_obj.f['worker_position'],
                    }
        for v in views:
            if True:
                print('dbname=', v)
                header, response = module_obj.sync_db_catalog(db_name=v)
                # schema = getattr(module_obj, v, "Attribute not found")
                schema = module_obj.views[v]['schema']
                print('CATALOG_ID=', module_obj.views[v]['catalog_id'])
                metadata_catalog = module_obj.lkf_api.get_catalog_metadata( module_obj.views[v]['catalog_id'] )
                # schema = module_obj.view_a
                # print('module_obj',module_obj)
                data = []
                udpdate_data = []
                print('len=', len(response))
                for row in response:
                    if v == 'LINK_EQUIPOS':
                        row['CATEGORIA'] = [row['EQUIPO'],]
                        row['NOMBRE'] = str(row['VEHICULO_TALID'])
                        row['ESTATUS'] = 'Disponible'
                        row['ESTADO'] = 'Activo'
                        this_eq = row.get('EQUIPO')
                        if this_eq not in equipos:
                            equipos.append(this_eq)
                            equipos_row.append(row)
                    usr = {}
                    usr.update(metadata_catalog)
                    usr['answers'] = module_obj.api_etl(row, schema)
                    usr['_id'] = usr['answers'].get('_id')
                    # print('row', row.get('GENERO'))
                    new = True
                    update = False
                    if new:
                        data.append(usr)
                    elif update:
                        udpdate_data.append(usr)
                    print('row=', row)
                    #print('usr=', usr)
                if equipos:
                    eq = module_obj.aux_view['EQUIPOS']
                    metadata_equipos = module_obj.lkf_api.get_catalog_metadata( eq['catalog_id'] )
                    data_equipos = []
                    for t_eq in equipos_row:
                        eq_ans = {}
                        eq_ans.update(metadata_equipos)
                        eq_ans['answers'] = module_obj.api_etl(t_eq, eq['schema'])
                        eq_ans['_id'] = eq_ans['answers'].get('_id')
                        data_equipos.append(eq_ans)
                    #module_obj.lkf_api.post_catalog_answers_list(data_equipos)
                if data:
                    print('a')
                    # print('data=',data)
                    module_obj.lkf_api.post_catalog_answers_list(data)