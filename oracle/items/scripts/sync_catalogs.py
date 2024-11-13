# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from oracle import Oracle

class Oracle(Oracle):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        print('kwtsss111111', kwargs)
        # super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        print('99999', self.kwargs)

        self.class_cr = self.get_db_cr('Oracle')
        #use self.lkm.catalog_id() to get catalog id
        self.name =  __class__.__name__
        self.load('Employee', **self.kwargs)
        self.Employee.f.update({
            'worker_code':'670f585bf844ff7bc357b1dc',
            'worker_code':'670f585bf844ff7bc357b1dc',
            
            })
        self.f.update(self.Employee.f)
        self.settings = settings
        self.etl_values = {
            'A':'Activo',
            'I':'Activo',
            'H':'Masculino',
            'M':'Femenino',
            'D':'Disponible',
            'ND':'NoDisponible',
        }

        self.schema_dict = {
            'LINK_EMPLEADOS':{
                'catalog_id': self.CONTACTO_CAT_ID,
                'schema':{
                    'CONTACTOID': self.f['address_code'],
                    'CIUDAD': self.f['city'],
                    'DIRECCION': self.f['address'],
                    'CODIGOPOSTAL': self.f['zip_code'],
                    'DEPARTAMENTO': self.f['worker_department'],
                    'EMAIL': self.f['email'],
                    'ESTADO_EMPLEADO': self.f['address_status'],
                    'GENERO': self.f['genero'],
                    'PUESTO': self.f['worker_position'],
                    'PAIS': self.f['country'],
                    'PROVINCIA': self.f['state'],
                    'RAZON_SOCIAL': self.f['address_name'],
                    'TELEFONO1': self.f['phone'],
                    'TELEFONO2': self.f['phone2'],
                    'TIPO_CONTACTO': self.f['address_type'],
                }
            },
            'LINK_EMPLEADOS_2':{
                'catalog_id': self.Employee.EMPLEADOS_JEFES_DIRECTOS_ID,
                'schema':{
                    'DEPARTAMENTO': self.f['worker_department'],
                    'CONTACTOID': self.f['worker_code_jefes'],
                    'EMAIL': self.f['email'],
                    'ESTADO_EMPLEADO': self.f['address_status'],
                    'GENERO': self.f['genero_jefes'],
                    'PUESTO': self.f['worker_position'],
                    'RAZON_SOCIAL': self.f['worker_name_jefes'],
                    'TELEFONO1': self.f['telefono1'],
                    'DIRECCION_CAT': self.f['address_name'],
                    'PICUTRE': self.f['picture_jefes'],
                }
            },
            'LINK_CLIENTES':{
                'catalog_id': self.CONTACTO_CAT_ID,
                'schema':{
                    'CONTACTOID': self.f['address_code'],
                    'CIUDAD': self.f['city'],
                    'DIRECCION': self.f['address'],
                    'CODIGOPOSTAL': self.f['zip_code'],
                    'DEPARTAMENTO': self.f['worker_department'],
                    'EMAIL': self.f['email'],
                    'ESTADO': self.f['address_status'],
                    'GENERO': self.f['genero'],
                    'PUESTO': self.f['worker_position'],
                    'PAIS': self.f['country'],
                    'PROVINCIA': self.f['state'],
                    'RAZON_SOCIAL': self.f['address_name'],
                    'TELEFONO1': self.f['phone'],
                    'TELEFONO2': self.f['phone2'],
                    'TIPO_CONTACTO': self.f['address_type'],
                }
            }
        }

        self.views = {
            # 'LINK_PAIS':{
            #     'catalog_id': self.COUNTRY_ID,
            #     'schema':{
            #         'PAISID': '_id',
            #         'DESCRIPCION': self.f['country'],
            #         'PAISID': self.f['country_code'],
            #         }
            # },
            # 'LINK_PROVINCIAS':{
            #     'catalog_id': self.ESTADO_ID,
            #     'schema':{
            #         'PROVINCIAID':  self.f['state_code'],
            #         'DESCRIPCION': self.f['state'],
            #         'PAIS': self.f['country'],
            #         }
            #     },
            # 'LINK_DEPARTAMENTO':{
            #     'catalog_id': self.DEPARTAMENTOS_ID,
            #     'schema':{
            #         'DEPARTAMENTOID': self.f['department_code'],
            #         'DESCRIPCION': self.f['worker_department'],
            #         }
            #     },
            # 'LINK_PUESTO':{
            #     'catalog_id': self.PUESTOS_ID,
            #     'schema':{
            #         'CARGOID': self.f['worker_position_code'],
            #         'DESCRIPCION': self.f['worker_position'],
            #         }
            # },
            'LINK_EMPLEADOS':{
                'catalog_id': self.EMPLOYEE_ID,
                'schema':{
                    'DEPARTAMENTO': self.f['worker_department'],
                    'CONTACTOID': self.f['worker_code'],
                    'EMAIL': self.f['email'],
                    'ESTADO_EMPLEADO': self.f['estatus_dentro_empresa'],
                    'GENERO': self.f['genero'],
                    'PUESTO': self.f['worker_position'],
                    'RAZON_SOCIAL': self.f['worker_name'],
                    'TELEFONO1': self.f['telefono1'],
                    'DIRECCION_CAT': self.f['address_name'],
                    'PICUTRE': self.f['picture'],
                    },
            },
            'LINK_FABRICANTE':{
                'catalog_id': self.MARCA_ID,
                'schema':{
                    'MARCAID': self.f['marca_codigo'],
                    'DESCRIPCION': self.f['marca'],
                    }
                },
            'LINK_MODELO_EQUIPOS':{
                'catalog_id': self.MODELO_ID,
                'schema':{
                    'MODELOID':  self.f['modelo_codigo'],
                    'MARCA': self.f['marca'],
                    'MODELO': self.f['modelo'],
                    'TIPO_MAQ': self.f['categoria_marca'],
                    }
                },
            'LINK_CLIENTES':{
                'catalog_id': self.CLIENTE_CAT_ID,
                'schema':{
                    'CONTACTOID': self.f['client_code'],
                    'NOMBRE_COMERCIAL': self.f['nombre_comercial'],
                    'RAZON_SOCIAL': self.f['razon_social'],
                    'DIRECCION_CAT': self.f['address_name'],
                    'RUC': self.f['rfc_razon_social'],
                    }
            },
            'LINK_EQUIPOS':{
                'catalog_id': self.ACTIVOS_FIJOS_CAT_ID,
                'schema':{
                    'VEHICULO_TALID': '_id',
                    'CATEGORIA': self.f['categoria_marca'],
                    'EQUIPO': self.f['tipo_equipo'],
                    'FABRICANTE': self.f['marca'],
                    'MODELO': self.f['modelo'],
                    'NOMBRE': self.f['nombre_equipo'],
                    'CHASIS': self.f['numero_de_serie_chasis'],
                    'CLIENTE': self.f['nombre_comercial'],
                    'ULTIMO_HOROMETRO': self.f['ultimo_horometro'],
                    'FEC_ULT_HOROMETRO': self.f['fecha_horometro'],
                    'ESTATUS': self.f['estatus'],
                    'ESTADO': self.f['estado'],
                    },
                }
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
                if type(value) in (str, int, float) and self.etl_values.get(value):
                    translated_dict[schema[key]] = self.etl_values[value]
                else:
                    translated_dict[schema[key]] = value
        # translated_dict = {schema[key]: value for key, value in data.items() if schema.get(key) and if value}
        return translated_dict


    def load_data(self, v, view, response, schema):
        metadata_catalog = self.lkf_api.get_catalog_metadata( view['catalog_id'] )
        # schema = module_obj.view_a
        data = []
        udpdate_data = []
        print('len=', len(response))
        print('v=',v)
        for row in response:
            if v == 'LINK_EQUIPOS':
                row['CATEGORIA'] = [row['EQUIPO'],]
                row['NOMBRE'] = str(row['VEHICULO_TALID'])
                row['ESTATUS'] = 'Disponible'
                row['ESTADO'] = 'Activo'
                this_eq = row.get('EQUIPO')
                # if this_eq not in equipos:
                #     self.equipos.append(this_eq)
                #     self.equipos_row.append(row)
            elif v == 'LINK_EMPLEADOS':
                row['TIPO_CONTACTO'] = 'Persona'
                row['DIRECCION_CAT'] = row['RAZON_SOCIAL']
                row['PICUTRE'] = {
                    'file_url':'https://f001.backblazeb2.com/file/app-linkaform/public-client-126/71202/60b81349bde5588acca320e1/64efc11e5e294b3fefca279e.png', 
                    'file_name':'default_avatar.jpg'}
            elif v == 'LINK_CLIENTES':
                row['ESTADO'] = 'Activo'
                row['DIRECCION_CAT'] = row['RAZON_SOCIAL']
                row['TIPO_CONTACTO'] = 'Empresa'
            usr = {}
            usr.update(metadata_catalog)
            usr['answers'] = self.api_etl(row, schema)
            usr['_id'] = usr['answers'].get('_id')
            # print('row', row.get('GENERO'))
            new = True
            update = False
            if new:
                data.append(usr)
            elif update:
                udpdate_data.append(usr)
            #print('usr=', usr)
        print('row=', row)
        if self.equipos:
            eq = self.aux_view['EQUIPOS']
            metadata_equipos = self.lkf_api.get_catalog_metadata( eq['catalog_id'] )
            data_equipos = []
            for t_eq in self.equipos_row:
                eq_ans = {}
                eq_ans.update(metadata_equipos)
                eq_ans['answers'] = self.api_etl(t_eq, eq['schema'])
                eq_ans['_id'] = eq_ans['answers'].get('_id')
                data_equipos.append(eq_ans)
            #self.lkf_api.post_catalog_answers_list(data_equipos)
        if data:
            print('a')
            print('data=', data[-1])
            res = self.lkf_api.post_catalog_answers_list(data)
            # print('res=',res)



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
        module_obj.equipos = []
        module_obj.equipos_row = []
        equipos_schema ={
                    'VEHICULO_TALID': '_id',
                    'DESCRIPCION': module_obj.f['worker_position'],
                    }
        for v in views:
            if True:
                print('dbname=', v)
                header, response = module_obj.sync_db_catalog(db_name=v)
                # schema = getattr(module_obj, v, "Attribute not found")
                print('CATALOG_ID=', module_obj.views[v]['catalog_id'])
                if v == 'LINK_EMPLEADOS':
                    view = module_obj.schema_dict[v]
                    schema = view['schema']
                    module_obj.load_data(v, view, response, schema)
                elif  v == 'LINK_CLIENTES':
                    view = module_obj.schema_dict[v]
                    schema = view['schema']
                    module_obj.load_data(v, view, response, schema)
                view = module_obj.views[v]
                schema = view['schema']
                module_obj.load_data(v, view, response, schema)
