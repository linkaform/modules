# coding: utf-8
import sys, simplejson, time, datetime
from linkaform_api import settings
from account_settings import *

from oracle import Oracle

class Oracle(Oracle):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        #self.class_cr = self.get_db_cr('Oracle')
        #use self.lkm.catalog_id() to get catalog id
        self.name =  __class__.__name__
        self.settings = settings
        self.etl_values = {
            'A':'Activo',
            'I':'Activo',
            'H':'Masculino',
            'M':'Femenino',
            'D':'Disponible',
            'ND':'NoDisponible',
        }

        self.VARIABLES_CRITICAS_PRODUCCION = 134148
        self.VARIABLES_CRITICAS_PRODUCCION_FORM = 139471

        self.db_id_fields = {
            'CONTACTOID':'FEC_MODIF',
            'PAISID':'FEC_MODIF',
            'PROVINCIAID':'FEC_MODIF',
            'DEPARTAMENTOID':'FEC_MODIF',
            'CARGOID':'FEC_MODIF',
            'MARCAID':'FEC_MODIF',
            'MODELOID':'FEC_MODIF_MODELO',
            'VEHICULO_TALID':'FEC_MODIF',
            }

        # self.schema_dict = {
        #     'LINK_EMPLEADOS':{
        #         'catalog_id': self.CONTACTO_CAT_ID,
        #         'schema':{
        #             'CONTACTOID': self.f['address_code'],
        #             'CIUDAD': self.f['city'],
        #             'DIRECCION': self.f['address'],
        #             'CODIGOPOSTAL': self.f['zip_code'],
        #             'DEPARTAMENTO': self.f['worker_department'],
        #             'EMAIL': self.f['email'],
        #             'ESTADO_EMPLEADO': self.f['address_status'],
        #             'GENERO': self.f['genero'],
        #             'PUESTO': self.f['worker_position'],
        #             'PAIS': self.f['country'],
        #             'PROVINCIA': self.f['state'],
        #             'RAZON_SOCIAL': self.f['address_name'],
        #             'TELEFONO1': self.f['phone'],
        #             'TELEFONO2': self.f['phone2'],
        #             'TIPO_CONTACTO': self.f['address_type'],
        #         }
        #     },
        #     'LINK_EMPLEADOS_2':{
        #         'catalog_id': self.Employee.EMPLEADOS_JEFES_DIRECTOS_ID,
        #         'schema':{
        #             'DEPARTAMENTO': self.f['worker_department'],
        #             'CONTACTOID': self.f['worker_code_jefes'],
        #             'EMAIL': self.f['email'],
        #             'ESTADO_EMPLEADO': self.f['address_status'],
        #             'GENERO': self.f['genero_jefes'],
        #             'PUESTO': self.f['worker_position'],
        #             'RAZON_SOCIAL': self.f['worker_name_jefes'],
        #             'TELEFONO1': self.f['telefono1'],
        #             'DIRECCION_CAT': self.f['address_name'],
        #             'PICUTRE': self.f['picture_jefes'],
        #         }
        #     },
        #     'LINK_CLIENTES':{
        #         'catalog_id': self.CONTACTO_CAT_ID,
        #         'schema':{
        #             'CONTACTOID': self.f['address_code'],
        #             'CIUDAD': self.f['city'],
        #             'DIRECCION': self.f['address'],
        #             'CODIGOPOSTAL': self.f['zip_code'],
        #             'DEPARTAMENTO': self.f['worker_department'],
        #             'EMAIL': self.f['email'],
        #             'ESTADO': self.f['address_status'],
        #             'GENERO': self.f['genero'],
        #             'PUESTO': self.f['worker_position'],
        #             'PAIS': self.f['country'],
        #             'PROVINCIA': self.f['state'],
        #             'RAZON_SOCIAL': self.f['address_name'],
        #             'TELEFONO1': self.f['phone'],
        #             'TELEFONO2': self.f['phone2'],
        #             'TIPO_CONTACTO': self.f['address_type'],
        #         }
        #     }
        # }

        self.views = {
            'PRODUCCION.VW_LinkAForm_Hora':{
                'catalog_id': self.VARIABLES_CRITICAS_PRODUCCION,
                'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM,
                'schema':{
                    'FECHA':'683753204328adb3fa0bfd2b',
                    'LECBRIXSOLUT2': 'ccaaa5b15ad84734fae92bae',
                    '%BRIXJARABE': 'ccaaa548cf22411a910aabf2',
                    'SODAAPLICADCALD': 'ccaaa864c62694640f0c69c7',
                    'FOSFATOSKITCAL5': 'ccaaa864c62694640f0c69ca',
                    'FOSFATOSKITCAL6': 'ccaaa864c62694640f0c69cd',
                    'CONDAGPURGCALD7': 'ccaaa864c62694640f0c69ce',
                    'FOSFATOSKITCAL7': 'ccaaa864c62694640f0c69d0',
                    'CONDTANQEB3': 'ccaaa864c62694640f0c69d1',
                    }
            },
            'PRODUCCION.VW_LinkAForm_Dia':{
                'catalog_id': self.VARIABLES_CRITICAS_PRODUCCION,
                'schema':{
                    # 'PROVINCIAID':  self.f['state_code'],
                    # 'DESCRIPCION': self.f['state'],
                    # 'PAIS': self.f['country'],
                    }
                },
            'vw_linkaform_fab':{
                'catalog_id': self.VARIABLES_CRITICAS_PRODUCCION,
                'schema':{
                    # 'DEPARTAMENTOID': self.f['department_code'],
                    # 'DESCRIPCION': self.f['worker_department'],
                    }
                },
            }


    def find_db_id(self, data):
        res = {}
        common_key = set(data.keys()).intersection(self.db_id_fields.keys())
        if common_key:
            res = {'sync_data':{}}
            common_key = ', '.join(map(str, common_key))
            res['sync_data']['db_id'] = data[common_key]
            res['sync_data']['db_id'] = data[common_key]
            res['sync_data']['updated_at'] = self.db_updated_at
            res['sync_data']['db_updated_at'] = data[self.db_id_fields[common_key]]
        return res

    def api_etl(self, data, schema, catalog_id, form_id):
        #data: 
        translated_dict = {}
        for key, value in data.items():
            if schema.get(key) and value:
                if type(value) in (str, int, float) and self.etl_values.get(value):
                    translated_dict[schema[key]] = self.etl_values[value]
                elif type(value) == datetime.datetime:
                    translated_dict[schema[key]] = value.strftime('%Y-%m-%d %H:%M:%S')    
                else:
                    translated_dict[schema[key]] = value
        translated_dict.update(self.find_db_id(data))
        translated_dict.update({'catalog_id':catalog_id})
        translated_dict.update({'form_id':form_id})
        return translated_dict

    def get_last_db_update_data(self, db_name):
        query = {"db_name":db_name}
        db_ids = self.class_cr.distinct("db_id", query)
        db_updated_at = self.class_cr.distinct("updated_at", query)
        db_updated_at.sort()
        if db_updated_at:
            db_updated_at = db_updated_at[0]
        return db_ids, db_updated_at

    def get_record_id_to_sync(self, query):
        db_ids = [r['lkf_id'] for r in self.class_cr.find(query)]
        return db_ids

    def get_oracle_id(self, data):
        common_keys = set(self.db_id_fields.keys()).intersection(data.keys())
        oracle_key = ', '.join(common_keys)
        return  data.get(oracle_key)


    def get_variables_model(self, view, **kwargs):
        metadata = self.lkf_api.get_metadata(view['form_id'])
        properties = {
                "device_properties":{
                    "system": "Script",
                    "process": "Sync Catalogs", 
                    "accion": 'sync_catalogs', 
                    "archive": "oracle/sync_catalogs.py",
                },
            }
        metadata.update({
            'properties': properties,
            'kwargs': kwargs,
            'answers': {}
            },
        )
        return metadata

    def load_data(self, v, view, response, schema, catalog_id, form_id, update=False):
        # metadata_catalog = self.lkf_api.get_catalog_metadata( view['catalog_id'] )
        metadata_catalog = self.get_variables_model(view)
        # schema = module_obj.view_a
        data = []
        for row in response:
            usr = {}
            ans = {}
            print('204 row=', row)
            usr.update(metadata_catalog)
            if row.get('answers') or row.get('ANSWERS'):
                ans = simplejson.loads(row.get('answers',row.get('ANSWERS')))
                ans['FECHA'] = row.get('FECHA_HORA')
            usr['answers'] = self.api_etl(ans, schema, catalog_id, form_id)
            usr['_id'] = usr['answers'].get('_id')
            usr['sync_data'] = usr['answers'].pop('sync_data') if usr['answers'].get('sync_data') else {}
            usr['sync_data']['oracle_id'] = self.get_oracle_id(row)
            usr['sync_data']['db_id'] = row.get('fecha','none')
            # print('row', row.get('GENERO'))

            data.append(usr)

        if data:
            # res = self.lkf_api.post_catalog_answers_list(data)
            print('v=', v)
            self.update_and_sync_db(v, catalog_id, form_id, data, update=update)

    def update_and_sync_db(self, db_name, catalog_id, form_id, data, update=False):
        if form_id:
            # print('data=', data)
            print('record=', data[0])
            res = self.lkf_api.post_forms_answers_list(data)
            print('res=', res)
        print(stop)
        for rec in data:
            print('rec=', rec)
            if rec.get('sync_data'):
                sync_data = rec.pop('sync_data')
                #creates
                print('sync_data', sync_data)
                query = {'db_id': sync_data['db_id'], 'item_id': rec['catalog_id']}
                record_id = None #self.get_record_id_to_sync(query)
                if record_id:
                    rec['record_id'] = record_id[0]
                    rec.pop('_id')
                    res = self.lkf_api.update_catalog_answers(rec, record_id=record_id[0])
                    res_data = res.get('json',{})
                    status_code = res['status_code']
                    if status_code in (200,201,202,204):
                        sync_data['"updated_at"'] = time.time()
                        self.update(query, sync_data, upsert=True)
                        # self.create(sync_data)
                else:
                    self.post_catalog(db_name, catalog_id, rec, sync_data)

    def post_catalog(self, db_name, item_id, rec, sync_data={}, db_sync=False):
            res = self.lkf_api.post_catalog_answers(rec)
            res_data = res.get('json',{})
            status_code = res['status_code']
            if status_code in (200,201,202,204):
                sync_data['"updated_at"'] = time.time()
                sync_data['item_id'] = res_data['catalog_id'] if res_data.get('catalog_id') else item_id
                sync_data['item_type'] = 'catalog'
                sync_data['db_name'] = db_name
                if db_sync:
                    query = {'db_name':db_name}
                    res = self.update(query, sync_data, upsert=True)
                else:
                    sync_data['lkf_id'] = res_data['id']
                    # print('Creating', sync_data)
                    res = self.create(sync_data)
            return res



if __name__ == "__main__":
    print('account settings \\\\\\\\\\\\\\', settings.config)
    oo  = settings.config['ORACLE_HOST']
    print('oo',oo)
    module_obj = Oracle(settings, sys_argv=sys.argv)
    # module_obj.console_run()
    module_obj.db_updated_at = time.time()
    #gg = module_obj.search_views()
    #print('gg',gg)
    # print('account settings', module_obj.settings.config)
    data = module_obj.data.get('data',{})
    option = data.get("option",'read')

    db_name = data.get('db_id',"LINK_EMPLEADOS")

    #-FUNCTIONS
    if option == 'read':

        # print('module_obj.views.keys()', module_obj.views)
        views = list(module_obj.views.keys())
        module_obj.equipos = []
        module_obj.equipos_row = []
        print('views',views)
        for v in views:
            print('-----------------------------------------------------------',v)
            if True:
                # record_ids, last_update,  = module_obj.get_last_db_update_data(v)
                last_update = False
                # last_update_date
                update = False
                query = None
                if last_update:
                    update = True
                    #se restan 6 hrs para aplicar GMT-6:00
                    last_update = last_update - 6*60*60
                    date_time = datetime.datetime.fromtimestamp(last_update)
                    last_update_date = date_time.strftime('%Y-%m-%d %H:%M:%S')
                    print('last_update_date', last_update_date)
                    a = f"TO_TIMESTAMP('{last_update_date}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
                    query = f'SELECT * FROM {v} WHERE FEC_MODIF  > {a}'
                else:
                    query = """
                        WITH base AS (
                        SELECT
                            TRUNC(DATA) + (TO_DATE(HORA, 'HH24:MI') - TRUNC(TO_DATE(HORA, 'HH24:MI'))) AS fecha_hora,
                            VARIA,
                            VALOR_N
                        FROM PRODUCCION.VW_LinkAForm_Hora
                        ),
                        ranked AS (
                        SELECT
                            fecha_hora, VARIA, VALOR_N,
                            ROW_NUMBER() OVER (
                            PARTITION BY fecha_hora, VARIA
                            ORDER BY fecha_hora DESC
                            ) AS rn
                        FROM base
                        )
                        SELECT
                        fecha_hora,
                        JSON_OBJECTAGG(VARIA VALUE VALOR_N) AS answers
                        FROM ranked
                        WHERE rn = 1
                        GROUP BY fecha_hora
                        ORDER BY fecha_hora
                    """
                    # # query = "select *  FROM PRODUCCION.VW_LinkAForm_Hora"
                    # query = """SELECT
                    #         TRUNC(DATA) + (TO_DATE(HORA, 'HH24:MI') - TRUNC(TO_DATE(HORA, 'HH24:MI'))) AS fecha_hora,
                    #         VARIA,
                    #         VALOR_N
                    #     FROM PRODUCCION.VW_LinkAForm_Hora"""
                header, response = module_obj.sync_db_catalog(db_name=v, query=query)
                # schema = getattr(module_obj, v, "Attribute not found")
                print('341 query=', query)
                # if v == 'LINK_EMPLEADOS':
                #     #Carga primero los Contactos
                #     view = module_obj.schema_dict[v]
                #     schema = view['schema']
                #     catalog_id = view['catalog_id']
                #     #module_obj.load_data(v, view, response, schema, catalog_id)
                #     # Carga Jefes Directos
                #     view = module_obj.schema_dict[f'{v}_2']
                #     schema = view['schema']
                #     catalog_id = view['catalog_id']
                #     module_obj.load_data(v, view, response, schema, catalog_id, update=update)
                # elif  v == 'LINK_CLIENTES':
                #     view = module_obj.schema_dict[v]
                #     schema = view['schema']
                #     catalog_id = view['catalog_id']
                #     module_obj.load_data(v, view, response, schema, catalog_id)
                view = module_obj.views[v]
                schema = view['schema']
                catalog_id = view['catalog_id']
                form_id = view['form_id']
                module_obj.load_data(v, view, response, schema, catalog_id, form_id)
