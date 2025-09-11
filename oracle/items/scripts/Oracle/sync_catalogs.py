# coding: utf-8
import sys, simplejson, time, datetime
from linkaform_api import settings
from account_settings import *

from oracle import Oracle

class Oracle(Oracle):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.class_cr = self.get_db_cr('Oracle')
        #use self.lkm.catalog_id() to get catalog id
        self.name =  __class__.__name__
        self.settings = settings

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

    def create_last_variable_value_record(self):
        """
        Busca el ultimo valor de las variables criticas y crea un registro en el catalogo de LinkaForm
        """
        metadata_form = self.get_variables_model(view)
        answers = self.get_last_variable_value()
        metadata_form['answers'] = answers
        res = self.lkf_api.post_forms_answers(metadata_form)
        self.upsert_catalog_record(metadata_form, 'Ultimo Valor')
        return res

    def create_average_variable_value_record(self, hours=24):
        metadata_form = self.get_variables_model(view)
        answers = self.get_average_values(hours)
        metadata_form['answers'] = answers
        # print('metadata_form=', simplejson.dumps(metadata_form, indent=4))
        res = self.lkf_api.post_forms_answers(metadata_form)
        # print('res form answers', res)
        self.upsert_catalog_record(metadata_form, 'Promedio')
        return res        

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

    def get_last_db_update_data(self, db_name):
        """
        Busca en LinkaForm los registros que ha sincronziado y busca el mas reciente de dicha base de datos o vista en Oracle
        Obtiene la fecha de la ultima actualizacion de la base de datos o vista en Oracle
        db_name: nombre de la base de datos o vista en Oracle
        
        returns:
        last_db_update_data: str con un datetime en formato %Y-%m-%d %H:%M:%S
        """
        last_db_update_data = None
        db_name = db_name.lower().replace(' ', '_')
        tipo = 'registro_oracle'
        query = [
                {
                    '$match': {
                    'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM,
                    '$or': [{ 'deleted_at': None }, { 'deleted_at': { '$exists': False } }],
                    "answers."+self.f['fecha']: { '$exists': True },
                    "answers."+self.f['base_de_datos_oracle']: db_name,
                    "answers."+self.f['tipo_registro']: tipo
                    }
                },
                { '$addFields': { '_sortDate': { '$toDate': "$answers."+self.f['fecha'] } } },
                { '$sort': { '_sortDate': -1 } },
                { '$limit': 1 },
                {
                    '$project': {
                    '_id': 1,
                    "date": "$answers."+self.f['fecha'],
                    }
                }
                ]
        with self.cr.aggregate(query) as cursor:
            doc = next(cursor, None)
            if doc:
                last_db_update_data = doc.get('date')
        return last_db_update_data

    def get_average_values(self, hours=24):
        query = [
        { '$match': { 'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM } },
        {'$addFields': {
            'fecha_parsed': {
                '$dateFromString': {
                    'dateString': f"$answers.{self.f['fecha']}",
                    'format': "%Y-%m-%d %H:%M:%S",
                    'onError': None,
                    'onNull': None
                }
            },
            'created_at_dt': {
                '$switch': {
                    'branches': [
                        # Numérico (epoch segundos) -> toDate(ms)
                        {
                            'case': { '$in': [{ '$type': '$created_at' }, ["double", "int", "long", "decimal"]] },
                            'then': {
                                '$toDate': { '$multiply': [{ '$toLong': '$created_at' }, 1000] }
                            }
                        },
                        {
                            'case': { '$eq': [{ '$type': '$created_at' }, "date"] },
                            'then': "$created_at"
                        },
                        {
                            'case': { '$eq': [{ '$type': '$created_at' }, "string"] },
                            'then': { '$toDate': "$created_at" }
                        }
                    ],
                    'default': None
                }
            }
        }},
        {'$addFields': {
            'fecha_dt': { '$ifNull': ["$fecha_parsed", "$created_at_dt"] },
            'window_start': { '$dateSubtract': { 'startDate': '$$NOW', 'unit': "hour", 'amount': hours } }
            }
        },
        { '$match': { '$expr': { '$gte': ["$fecha_dt", "$window_start"] } } },
        { '$project': { 'answers': { '$objectToArray': "$answers" }, "fecha_dt":1 }},
        { '$unwind': "$answers" },
        {'$match': {
            "answers.k": {
                '$nin': [
                    self.f['fecha'], # FECHA
                    self.f['base_de_datos_oracle'], # BASE_DE_DATOS_ORACLE
                    self.f['tipo_registro']  # TIPO_REGISTRO
                ]
            }
        }},
        {'$addFields': {
            'val_num': {
                '$convert': { 'input': '$answers.v', 'to': "double", 'onError': None, 'onNull': None }
            }
        }},
        { '$match': { 'val_num': { '$ne': None } } },
        {'$group': {
            '_id': "$answers.k",
            'promedio_24h': { '$avg': '$val_num' },
            'ultima_fecha': { '$max': '$fecha_dt' },
            'n': { '$sum': 1 }
        }},
        { '$sort': { '_id': 1 } }
        ]
        # print('query', simplejson.dumps(query, indent=4))
        answers_average_values = {}
        with self.cr.aggregate(query) as cursor:
            for doc in cursor:
                answers_average_values[doc['_id']] = round(doc['promedio_24h'], 2)
                answers_average_values[self.f['fecha']] = doc['ultima_fecha'].strftime("%Y-%m-%d %H:%M:%S")

        answers_average_values[self.f['tipo_registro']] = 'promedio'
        if answers_average_values.get(self.f['base_de_datos_oracle']):
            answers_average_values.pop(self.f['base_de_datos_oracle'])
            
        return answers_average_values

    def get_last_variable_value(self):
        """
        Busca el ultimo valor de las variables criticas y crea un registro en el catalogo de LinkaForm
        """
        print(' update values....')
        query=  [
            { '$match': { 'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM, 'deleted_at': {'$exists': False} } },
            { '$sort': { f"answers.{self.f['fecha']}": -1 } },
            { '$project': { 'answers': { '$objectToArray': "$answers" } } },
            { '$unwind': "$answers" },
            {
                '$group': {
                '_id': "$answers.k",
                'ultimo_valor': { '$first': "$answers.v" }
                }
            }
            ]
        # print('query', query)
        answers_last_values = {}
        with self.cr.aggregate(query) as cursor:
            for doc in cursor:
                answers_last_values[doc['_id']] = doc['ultimo_valor']
        
        answers_last_values[self.f['tipo_registro']] = 'ultimo_valor'
        if answers_last_values.get(self.f['base_de_datos_oracle']):
            answers_last_values.pop(self.f['base_de_datos_oracle'])
            
        return answers_last_values

    def get_record_id_to_sync(self, query):
        db_ids = [r['lkf_id'] for r in self.class_cr.find(query)]
        return db_ids

    def get_oracle_id(self, data):
        common_keys = set(self.db_id_fields.keys()).intersection(data.keys())
        oracle_key = ', '.join(common_keys)
        return  data.get(oracle_key)

    def get_variables_model(self, view, **kwargs):
        """
        Obtiene los metadatos de la forma figada a la vista de Oracle
        view: nombre de la base de datos o vista en Oracle
        kwargs: argumentos adicionales
        returns:
        metadata: dict modelo de la forma / libre de agregar los parametros necesarios en params
        """
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
        """
        Carga los datos de la base de datos o vista en Oracle
        v: nombre de la base de datos o vista en Oracle
        view: configuracion de vista para conxtarse a oracle, esqueda de ETL
        response: respuesta de la base de datos o vista en Oracle
        schema: schema de la base de datos o vista en Oracle:LKF
        catalog_id: id del catalogo
        form_id: id del formulario
        update: si es True actualiza, si es False inserta
        """
        # metadata_catalog = self.lkf_api.get_catalog_metadata( view['catalog_id'] )
        metadata_catalog = self.get_variables_model(view)
        # schema = module_obj.view_a
        data = []
        for row in response:
            print('row=', row)
            usr = {}
            ans = {}
            usr.update(metadata_catalog)
            if row.get('answers') or row.get('ANSWERS'):
                ans = simplejson.loads(row.get('answers',row.get('ANSWERS')))
                ans['BASE_DE_DATOS_ORACLE'] = v.lower().replace(' ', '_')
                ans['TIPO_REGISTRO'] = 'registro_oracle'
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
            self.update_and_sync_db(v, catalog_id, form_id, data, update=update)

    def post_catalog(self, catalogo_metadata, rec):
        """
        Crea un catalogo en Linkaform
        catalogo_metadata: metadata del catalogo
        rec: datos del catalogo
        """
        catalogo_metadata.update({'answers': rec})
        res = self.lkf_api.post_catalog_answers(catalogo_metadata)
        res_data = res.get('json',{})
        status_code = res['status_code']
        # print('res_data', res_data)
        # print('status_code', status_code)
        # if status_code in (200,201,202,204):
        #     sync_data['"updated_at"'] = time.time()
        #     sync_data['item_id'] = res_data['catalog_id'] if res_data.get('catalog_id') else item_id
        #     sync_data['item_type'] = 'catalog'
        #     sync_data['db_name'] = db_name
        #     if db_sync:
        #         query = {'db_name':db_name}
        #         res = self.update(query, sync_data, upsert=True)
        #     else:
        #         sync_data['lkf_id'] = res_data['id']
        #         # print('Creating', sync_data)
        #         res = self.create(sync_data)
        return res

    def update_and_sync_db(self, db_name, catalog_id, form_id, data, update=False):
        """
        Decide si es un catalogo o un formulario y realiza el post o update
        en el caso de la forma de momento solo hay posts
        db_name: nombre de la base de datos o vista en Oracle
        catalog_id: id del catalogo
        form_id: id del formulario
        data: datos a sincronizar
        update: si es True actualiza, si es False inserta
        """
        print('========================================')
        print('db_name=', db_name)
        print('catalog_id=', catalog_id)
        print('form_id=', form_id)
        if form_id:
            # print('data=', data)
            # print(' SOLO EL PRIMERO DATA[0] record=', data[0])
            res = self.lkf_api.post_forms_answers_list(data)
            self.verify_complete_sync(data, res)
            # print('res=', res)
        elif catalog_id:
            for rec in data:
                print('rec=', rec)
                if rec.get('sync_data'):
                    sync_data = rec.pop('sync_data')
                    #creates
                    # print('sync_data', sync_data)
                    query = {'db_id': sync_data['db_id'], 'item_id': rec.get('catalog_id', rec.get('form_id'))}
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
                            self.create(sync_data)
                    else:
                        self.post_catalog(db_name, catalog_id, rec, sync_data)
        return True

    def update_values(self, db_name, view, form_id, hours=96, N=20):
        """
        Busca en los datos caputrados de las ultimas x Hras o N numero de registros con el dato, 
        con el tipo de registro 'registro_oracle' de la forma y crea los siguentes tipos de registro:
        - Deisvian estandar 
        - Promedio de datos
        - Moda
        - Mediana
        - Varianza
        - Desviacion tipica
        - Maximo
        - Minimo

        N: Tamaño de muestra
        hours: Numero de horas
        db_name: Nombre de la base de datos o vista en Oracle
        view: Nombre de la base de datos o vista en Oracle
        form_id: Id del formulario
        """
        
        last_value_res = self.create_last_variable_value_record()
        # print('last_value_res', last_value_res)
        average_value_res = self.create_average_variable_value_record(hours)
        # print('average_value_res', average_value_res)
        return average_value_res

    def upsert_catalog_record(self, data, record_type):
        """
        Searches for a catalog record, if not found creat it, if found update it!!!
        """
        catalogo_metadata = self.lkf_api.get_catalog_metadata(catalog_id=self.VARIABLES_CRITICAS_PRODUCCION)
        if data.get('answers'):
            catalogo_metadata['answers'] = {}
            for k,v in data.get('answers').items():
                if k not in (self.f['fecha']):
                    catalogo_metadata['answers'][k] = f"{v} | {record_type}"
                else:
                    if record_type == 'Promedio':
                        pass
                    else:
                        catalogo_metadata['answers'][k] = v
        else:
            catalogo_metadata['answers'] = data
        catalogo_metadata['answers'][self.f['tipo_registro']] = record_type
        catalogo_metadata['answers'][self.f['variable_criticas']] = 'Variables críticas'
        # print('catalogo_metadata=', simplejson.dumps(catalogo_metadata, indent=4))
        mango_query = { "selector": {f"answers.{self.f['tipo_registro']}": record_type} }
        record = self.lkf_api.search_catalog(self.VARIABLES_CRITICAS_PRODUCCION, mango_query)
        if record:
            rec = record[0]
            catalogo_metadata['record_id'] = rec['_id']
            res = self.lkf_api.update_catalog_answers(catalogo_metadata, record_id=rec['_id'])
            # print('res catalogo update', res)
        else:
            res = self.lkf_api.post_catalog_answers(catalogo_metadata)
            # print('res catalogo post', res)
        return res

    def verify_complete_sync(self, data, res):
        """
        Verifica que la sincronizacion sea correcta
        data: datos a sincronizar
        res: respuesta de la sincronizacion
        lo que hace es verificar que la variable res tenga dentrdo de los elementos de su lista
        en el diccionario {'status_code': 201}
        """
        print('verify_complete_sync...')
        created_records = [r for r in res if r.get('status_code') == 201]
        updated_records = [r for r in res if r.get('status_code') == 200]
        if (len(created_records) + len(updated_records) )!= len(data):
            raise self.LKFException('No se sincronizaron todos los registros')
        return created_records, updated_records

if __name__ == "__main__":
    """
    Formato last_update_date = '2025-08-25 15:00'
    """
    oo  = settings.config['ORACLE_HOST']
    module_obj = Oracle(settings, sys_argv=sys.argv)
    module_obj.console_run()
    module_obj.db_updated_at = time.time()
    #gg = module_obj.search_views()
    #print('gg',gg)
    # print('account settings', module_obj.settings.config)
    data = module_obj.data.get('data',{})
    option = data.get("option",'read')


    #-FUNCTIONS
    if option == 'read':
        # print('module_obj.views.keys()', module_obj.views)
        views = list(module_obj.views.keys())
        for v in views:
            print('-----------------------------------------------------------',v)
            if True:
                last_update = module_obj.get_last_db_update_data(v)
                # last_update_date
                print('last_update', last_update)
                update = False
                query = None
                # last_update = None
                if last_update:
                    if len(last_update) == 19:
                        last_update = last_update[:-3]
                    if v == 'PRODUCCION.VW_LinkAForm_Hora':
                        query = f"""
                            WITH base AS (
                            SELECT
                                TRUNC(DATA) + (TO_DATE(HORA, 'HH24:MI') - TRUNC(TO_DATE(HORA, 'HH24:MI'))) AS fecha_hora,
                                VARIA,
                                VALOR_N
                            FROM {v}
                            WHERE TRUNC(DATA) + (TO_DATE(HORA, 'HH24:MI') - TRUNC(TO_DATE(HORA, 'HH24:MI')))
                            > TO_DATE('{last_update}', 'YYYY-MM-DD HH24:MI')
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
                    elif v == 'PRODUCCION.VW_LinkAForm_Dia':
                        query = f"""SELECT DATA AS fecha_hora, JSON_OBJECTAGG(VARIA VALUE VALOR_DIA) AS answers
                            FROM {v}
                            WHERE DATA > TO_DATE('{last_update}', 'YYYY-MM-DD')
                            ORDER BY fecha_hora ASC"""
                else:
                    if v == 'PRODUCCION.VW_LinkAForm_Hora':
                        query = f"""
                            WITH base AS (
                            SELECT
                                TRUNC(DATA) + (TO_DATE(HORA, 'HH24:MI') - TRUNC(TO_DATE(HORA, 'HH24:MI'))) AS fecha_hora,
                                VARIA,
                                VALOR_N
                            FROM {v}
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
                    elif v == 'PRODUCCION.VW_LinkAForm_Dia':
                        query  =f"""SELECT DATA AS fecha_hora, VARIA, VALOR_DIA from {v} ORDER BY fecha_hora ASC"""
                        query = """WITH base AS (
                                SELECT DATA AS fecha_hora, VARIA, VALOR_DIA
                                FROM PRODUCCION.VW_LinkAForm_Dia
                                ),
                                dedup AS (
                                SELECT
                                    fecha_hora,
                                    VARIA,
                                    MAX(VALOR_DIA) AS valor_dia_txt   -- elige un valor no nulo si existe
                                FROM base
                                GROUP BY fecha_hora, VARIA
                                )
                                SELECT
                                    fecha_hora,
                                    JSON_OBJECTAGG(VARIA VALUE TO_CHAR(valor_dia_txt) RETURNING VARCHAR2(32767)) AS answers
                                FROM dedup
                                GROUP BY fecha_hora
                                ORDER BY fecha_hora"""

                print('query=', query)
                header, response = module_obj.sync_db_catalog(db_name=v, query=query)
                print('response=', response)
                print('header=', header)
                view = module_obj.views[v]
                schema = view['schema']
                catalog_id = view.get('catalog_id')
                form_id = view.get('form_id')
                module_obj.load_data(v, view, response, schema, catalog_id, form_id)
        module_obj.update_values(v, view, form_id)
