# coding: utf-8
from copy import deepcopy
import sys, simplejson, time, datetime
import pytz
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
            if schema.get(key) and value != None:
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
        data = self.get_variable_values()
        record_answers = self.get_answers_by_kind(data)
        # print('record_answers', record_answers)
        for kind, answers in record_answers.items():
            metadata = deepcopy(metadata_form)
            metadata['answers'] = answers
            if 'ultimos_5' in kind:
                continue
            # else:
            #if kind == 'ultimo_valor':
            #    print('aqui va a hacer el post......')
                # res = self.lkf_api.post_forms_answers(metadata)
            self.upsert_catalog_record(metadata, kind)
        return True

    # def create_average_variable_value_record(self, hours=24):
    #     metadata_form = self.get_variables_model(view)
    #     answers = self.get_average_values(hours)
    #     metadata_form['answers'] = answers
    #     # print('metadata_form=', simplejson.dumps(metadata_form, indent=4))
    #     res = self.lkf_api.post_forms_answers(metadata_form)
    #     # print('res form answers', res)
    #     self.upsert_catalog_record(metadata_form, 'Promedio')
    #     return res        

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

    def get_last_db_update_data(self, db_name, tipo='registro_oracle'):
        """
        Busca en LinkaForm los registros que ha sincronziado y busca el mas reciente de dicha base de datos o vista en Oracle
        Obtiene la fecha de la ultima actualizacion de la base de datos o vista en Oracle
        db_name: nombre de la base de datos o vista en Oracle
        tipo: tipo de registro en LinkaForm
        
        returns:
        last_db_update_data: str con un datetime en formato %Y-%m-%d %H:%M:%S
        """
        last_db_update_data = None
        db_name = db_name.lower().replace(' ', '_')
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

    def get_answers_by_kind(self, data):
        #todo buscar fecha maxima
        result = {
        'ultimo_valor': {},
        'promedio': {},
        'maximo': {},
        'minimo': {},
        'desviacion_estandar': {},
        'ultimos_0': {},
        'ultimos_1': {},
        'ultimos_2': {},
        'ultimos_3': {},
        'ultimos_4': {},
        'ultimos_5': {},
        }
        for variable_data in data:
            if variable_data.get('fecha') and not result['ultimo_valor'].get(self.f['fecha']):
                result['ultimo_valor'][self.f['fecha'][:16]] = variable_data.get('fecha')
            if variable_data.get('fecha') and result['ultimo_valor'].get(self.f['fecha']) \
                and variable_data.get('fecha') > result['ultimo_valor'].get(self.f['fecha']):
                result['ultimo_valor'][self.f['fecha'][:16]] = variable_data.get('fecha')
            variable_id = variable_data['_id']
            # Agregar cada estadística al diccionario correspondiente
            result['ultimo_valor'][variable_id] = variable_data.get('ultimo_valor')
            result['ultimo_valor'][self.f['fecha'][:16]] = variable_data.get('ultimo_valor')
            result['promedio'][variable_id] = variable_data.get('promedio')
            result['promedio'][self.f['fecha'][:16]] = variable_data.get('fecha')
            result['maximo'][variable_id] = variable_data.get('maximo')
            result['maximo'][self.f['fecha'][:16]] = variable_data.get('fecha')
            result['minimo'][variable_id] = variable_data.get('minimo')
            result['minimo'][self.f['fecha'][:16]] = variable_data.get('fecha')
            result['desviacion_estandar'][variable_id] = variable_data.get('desviacion_estandar')
            result['desviacion_estandar'][self.f['fecha'][:16]] = variable_data.get('fecha')
            for idx, value in enumerate(variable_data.get('ultimos_5',[])):
                #if idx == 0:
                    #me salto el ultimo valor ya que es igual que el ultimo
                result[f'ultimos_{idx}'][variable_id] = value
                result[f'ultimos_{idx}'][self.f['fecha']] = variable_data.get('fecha')[:16]
        return result

    # def get_average_values(self, hours=24):
    #     query = [
    #     { '$match': { 'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM } },
    #     {'$addFields': {
    #         'fecha_parsed': {
    #             '$dateFromString': {
    #                 'dateString': f"$answers.{self.f['fecha']}",
    #                 'format': "%Y-%m-%d %H:%M:%S",
    #                 'onError': None,
    #                 'onNull': None
    #             }
    #         },
    #         'created_at_dt': {
    #             '$switch': {
    #                 'branches': [
    #                     # Numérico (epoch segundos) -> toDate(ms)
    #                     {
    #                         'case': { '$in': [{ '$type': '$created_at' }, ["double", "int", "long", "decimal"]] },
    #                         'then': {
    #                             '$toDate': { '$multiply': [{ '$toLong': '$created_at' }, 1000] }
    #                         }
    #                     },
    #                     {
    #                         'case': { '$eq': [{ '$type': '$created_at' }, "date"] },
    #                         'then': "$created_at"
    #                     },
    #                     {
    #                         'case': { '$eq': [{ '$type': '$created_at' }, "string"] },
    #                         'then': { '$toDate': "$created_at" }
    #                     }
    #                 ],
    #                 'default': None
    #             }
    #         }
    #     }},
    #     {'$addFields': {
    #         'fecha_dt': { '$ifNull': ["$fecha_parsed", "$created_at_dt"] },
    #         'window_start': { '$dateSubtract': { 'startDate': '$$NOW', 'unit': "hour", 'amount': hours } }
    #         }
    #     },
    #     { '$match': { '$expr': { '$gte': ["$fecha_dt", "$window_start"] } } },
    #     { '$project': { 'answers': { '$objectToArray': "$answers" }, "fecha_dt":1 }},
    #     { '$unwind': "$answers" },
    #     {'$match': {
    #         "answers.k": {
    #             '$nin': [
    #                 self.f['fecha'], # FECHA
    #                 self.f['base_de_datos_oracle'], # BASE_DE_DATOS_ORACLE
    #                 self.f['tipo_registro']  # TIPO_REGISTRO
    #             ]
    #         }
    #     }},
    #     {'$addFields': {
    #         'val_num': {
    #             '$convert': { 'input': '$answers.v', 'to': "double", 'onError': None, 'onNull': None }
    #         }
    #     }},
    #     { '$match': { 'val_num': { '$ne': None } } },
    #     {'$group': {
    #         '_id': "$answers.k",
    #         'promedio_24h': { '$avg': '$val_num' },
    #         'ultima_fecha': { '$max': '$fecha_dt' },
    #         'n': { '$sum': 1 }
    #     }},
    #     { '$sort': { '_id': 1 } }
    #     ]
    #     # print('query', simplejson.dumps(query, indent=4))
    #     answers_average_values = {}
    #     with self.cr.aggregate(query) as cursor:
    #         for doc in cursor:
    #             answers_average_values[doc['_id']] = round(doc['promedio_24h'], 2)
    #             answers_average_values[self.f['fecha']] = doc['ultima_fecha'].strftime("%Y-%m-%d %H:%M:%S")

    #     answers_average_values[self.f['tipo_registro']] = 'promedio'
    #     if answers_average_values.get(self.f['base_de_datos_oracle']):
    #         answers_average_values.pop(self.f['base_de_datos_oracle'])
            
    #     return answers_average_values

    def get_variable_values(self):
        query = [
                { '$match': { 
                    'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM , 
                    'deleted_at': { '$exists': False },
                    'answers.'+self.f['tipo_registro'] : 'registro_oracle'
                } },
                { '$sort': { 'answers.'+self.f['fecha'] : -1 } },
                {'$limit':100},
                { '$project': { 
                    'fecha': '$answers.'+self.f['fecha'],
                    'answers': { '$objectToArray': '$answers' }
                } },
                { '$unwind': '$answers' },
                { '$match': { 
                    'answers.k': { 
                        '$nin': [
                            '683753204328adb3fa0bfd2b', 
                            '68ae9831a113d169e05af40d', 
                            '68ae9831a113d169e05af40e'
                        ] 
                    }
                } },
                { '$group': {
                    '_id': '$answers.k',
                    'todos_datos': { 
                        '$push': { 
                            'fecha': '$fecha', 
                            'valor': '$answers.v'
                        }
                    }
                } },
                { '$addFields': {
                    'datos_ordenados': {
                        '$sortArray': {
                            'input': '$todos_datos',
                            'sortBy': { 'fecha': -1 }
                        }
                    }
                } },
                { '$addFields': {
                    'ultimos_5': { '$slice': ['$datos_ordenados', 0, 6] },
                    'todos_valores_numericos': {
                        '$map': {
                            'input': '$datos_ordenados',
                            'as': 'dato',
                            'in': { '$toDouble': '$$dato.valor' }
                        }
                    }
                } },
                { '$addFields': {
                    'ultimo_valor': { '$toDouble': { '$arrayElemAt': ['$ultimos_5.valor', 0] } },
                    'promedio': { '$avg': '$todos_valores_numericos' },
                    'desviacion_estandar': { '$stdDevPop': '$todos_valores_numericos' },
                    'minimo': { '$min': '$todos_valores_numericos' },
                    'maximo': { '$max': '$todos_valores_numericos' },
                    'count': { '$size': '$todos_valores_numericos' },
                    'fecha': { '$max' : '$datos_ordenados.fecha' }
                } },

                {'$project': {
                    '_id': 1,
                    'variable': 1,
                    'ultimo_valor': 1,
                    'promedio': 1,
                    'desviacion_estandar': 1,
                    'minimo': 1,
                    'maximo': 1,
                    'count': 1,
                    'ultimos_5': 1,
                    'fecha': 1
                }}
            ]
        resultados = list(self.cr.aggregate(query))
        return resultados

    def get_last_variable_value_back(self):
        """
        Busca el ultimo valor de las variables criticas y crea un registro en el catalogo de LinkaForm
        """
        query=  [
            { '$match': { 'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM, 'deleted_at': {'$exists': False} } },
            { '$sort': { f"answers.{self.f['fecha']}": -1 } },
            { '$limit': 5 },  # Limitar a los 5 documentos más recientes
            { '$project': { 'answers':  "$answers" } },
            { '$project': { 'answers': { '$objectToArray': "$answers" } } },
            { '$unwind': "$answers" },
            {
                '$group': {
                '_id': "$answers.k",
                'ultimos_valores': { '$push': "$answers.v" }
                }
            }
            ]
        answers_last_values = {}
        with self.cr.aggregate(query) as cursor:
            for doc in cursor:
                answers_last_values[doc['_id']] = doc['ultimos_valores']
        
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
        Obtiene los metadatos de la forma ligada a la vista de Oracle
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
            usr = {}
            ans = {}
            usr.update(metadata_catalog)
            if row.get('answers') or row.get('ANSWERS'):
                ans = simplejson.loads(row.get('answers',row.get('ANSWERS')))
                ans['BASE_DE_DATOS_ORACLE'] = v.lower().replace(' ', '_')
                ans['TIPO_REGISTRO'] = 'registro_oracle'
                ans['FECHA'] = row.get('FECHA_HORA', row.get('FECHA'))
                # if last_update == str(ans['FECHA'])[:16]:
                #     print('last_update', last_update)
                #     print('continue....')
                #     continue
            usr['answers'] = self.api_etl(ans, schema, catalog_id, form_id)
            fecha_str = str(ans['FECHA']).replace('-','').replace(':','').replace(' ','')
            fecha_24 = fecha_str.zfill(24)
            # usr['_id'] = fecha_24
            usr['sync_data'] = usr['answers'].pop('sync_data') if usr['answers'].get('sync_data') else {}
            usr['sync_data']['oracle_id'] = self.get_oracle_id(row)
            usr['sync_data']['db_id'] = row.get('fecha','none')
            data.append(usr)
        if data:
            self.update_and_sync_db(v, catalog_id, form_id, data, update=update)
        return data

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

    def set_oracle_server(self, view_name):
        """
        Set the oracle server
        """
        if view_name == 'REPORTES.vw_linkaform_fab':
            print('serivce 2')
            self.ORACLE_HOST = self.settings.config.get('ORACLE_HOST_2')
            self.ORACLE_SERVICE_NAME = self.settings.config.get('ORACLE_SERVICE_NAME_2')
            self.ORACLE_SID = self.settings.config.get('ORACLE_SID_2')
            self.ORACLE_USERNAME = self.settings.config.get('ORACLE_USERNAME_2')
            self.ORACLE_PASSWORD = self.settings.config.get('ORACLE_PASSWORD_2')
            self.oracle = self.connect_to_oracle()
        else:
            self.ORACLE_HOST = self.settings.config.get('ORACLE_HOST')
            self.ORACLE_SERVICE_NAME = self.settings.config.get('ORACLE_SERVICE_NAME_1')
            self.ORACLE_SID = self.settings.config.get('ORACLE_SID_1')
            self.ORACLE_USERNAME = self.settings.config.get('ORACLE_USERNAME_1')
            self.ORACLE_PASSWORD = self.settings.config.get('ORACLE_PASSWORD_1')
        return True

    def update_records(self, data):
        """
        Obtienen la fecha de los registros y busca dicha fecha en la base de datos de linkaform.
        Si existe el resistro lo actualiza, lo quita de la lista y regresa los registros que no se actualizaron para su creacion 
        data: lista de datos
        """
        print('asi entra... data=', len(data))
        fechas  = [rec['answers'][self.f['fecha']] for rec in data] 
        query =  { 'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM, 'deleted_at': {'$exists': False} }
        query.update({f'answers.{self.f["fecha"]}': {'$in': fechas}})
        records = self.cr.find(query,{'_id':1,'answers.'+self.f['fecha']:1})
        records = {rec.get('answers',{}).get(self.f['fecha']):rec['_id'] for rec in records}
        print('records=', records)
        # existing_date = [rec.get('answers',{}).get(self.f['fecha']) for rec in records]
        # for rec in data:
        update_records = []
        create_records = []
        for rec in data:
            if rec['answers'][self.f['fecha']] in list(records.keys()):
                rec['_id'] = records[rec['answers'][self.f['fecha']]]
                update_records.append(rec)
            else:
                create_records.append(rec)
        # update_records = [rec for rec in data if rec['answers'][self.f['fecha']] in existing_date]
        # create_records = [rec for rec in data if rec['answers'][self.f['fecha']] not in existing_date]
        res = self.lkf_api.patch_record_list(update_records)
        return create_records

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
            data = self.update_records(data)
            res = self.lkf_api.post_forms_answers_list(data)
            self.verify_complete_sync(data, res)
        elif catalog_id:
            for rec in data:
                if rec.get('sync_data'):
                    sync_data = rec.pop('sync_data')
                    #creates
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
        return last_value_res

    def ultima_fecha(self, ultima_fecha, record_type):
        mango_query = { "selector": {f"answers.{self.f['tipo_registro']}": record_type} }
        record = self.lkf_api.search_catalog(self.VARIABLES_CRITICAS_PRODUCCION, mango_query)
        if record:
            rec = record[0]
            if rec.get(self.f['fecha']) > ultima_fecha:
                return rec.get(self.f['fecha'])
        return ultima_fecha

    def upsert_catalog_record(self, data, record_type):
        """
        Searches for a catalog record, if not found creat it, if found update it!!!
        """
        if record_type == 'ultimo_valor':
            return {}
        catalogo_metadata = self.lkf_api.get_catalog_metadata(catalog_id=self.VARIABLES_CRITICAS_PRODUCCION)
        if data.get('answers'):
            catalogo_metadata['answers'] = {}
            for k,v in data.get('answers').items():
                #recorreo cada tipo de registro
                record_type = record_type.replace('_', ' ').title()
                if k not in (self.f['fecha']):
                    r_type = record_type
                    if isinstance(v, dict):
                        r_type = v['fecha']
                        v = v['valor']
                    valor_formateado = f"{v:,.2f}" if isinstance(v, float) else f"{v:,}.00"
                    if record_type == 'Ultimos 0':
                        # Se le da formato a los datos para que queden en el catalogo
                        # Se pone una estrella ⭐ delante de la fehca al que sea el ultimo valor reportado.
                        # ese valor debe de ser igual al de ultimo valor
                        catalogo_metadata['answers'][k] = f"{str(r_type[:16] + '   ⭐').ljust(20)} : {valor_formateado:>12}"
                    else:
                        catalogo_metadata['answers'][k] = f"{str(r_type[:16] ).ljust(20)} : {valor_formateado:>12}"
                else:
                    #si el tipo de registro es el ultimo valor, asiganmos valor a la fecha
                    # solo un registro debe de tener la fecha
                    if record_type == 'Ultimos 0':
                        ultima_fecha = self.ultima_fecha(v, record_type)
                        catalogo_metadata['answers'][k] = ultima_fecha
        else:
            catalogo_metadata['answers'] = data
        
        catalogo_metadata['answers'][self.f['tipo_registro']] = record_type
        catalogo_metadata['answers'][self.f['variable_criticas']] = 'Variables críticas'
        mango_query = { "selector": {f"answers.{self.f['tipo_registro']}": record_type} }
        record = self.lkf_api.search_catalog(self.VARIABLES_CRITICAS_PRODUCCION, mango_query)
        if record:
            rec = record[0]
            catalogo_metadata['record_id'] = rec['_id']
            res = self.lkf_api.update_catalog_answers(catalogo_metadata, record_id=rec['_id'])
        else:
            res = self.lkf_api.post_catalog_answers(catalogo_metadata)
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
            raise self.LKFException('No se sincronizaron todos los registros, creados: actualizados: {} data: {} '.format(len(created_records) + len(updated_records), len(data)))
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
    # print('account settings', module_obj.settings.config)
    data = module_obj.data.get('data',{})
    option = data.get("option",'read')

    timezone = module_obj.user.get('timezone','America/Bogota')
    tz = pytz.timezone(timezone)
    now = datetime.datetime.now(tz)
    if now.hour < 6:
        last_update = (now - timedelta(days=1)).date()
    else:
        last_update = now.date()
    last_update = last_update.strftime('%Y-%m-%d 00:00:00')
    #-FUNCTIONS
    if option == 'read':
        # print('module_obj.views.keys()', module_obj.views)
        views = list(module_obj.views.keys())
        for v in views:
            print('-----------------------------------------------------------',v)
            # last_update = module_obj.get_last_db_update_data(v)
            # last_update = '2025-11-10 23:00:00'
            print('last_update', last_update)
            module_obj.set_oracle_server(v)
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
                    #last_update = '2025-09-09 23:59'
                    query = f"""SELECT DATA AS fecha_hora, JSON_OBJECTAGG(VARIA VALUE VALOR_DIA) AS answers
                        FROM {v}
                        WHERE DATA > TO_DATE('{last_update[:10]}', 'YYYY-MM-DD')
                        GROUP BY DATA
                        ORDER BY DATA ASC"""
                elif v == 'REPORTES.vw_linkaform_fab':
                    query = f"""
                        SELECT 
                        FECHA,
                        JSON_OBJECTAGG(VARIABLENOMBRE VALUE VALOR RETURNING VARCHAR2(32767)) AS ANSWERS
                        FROM {v}
                        WHERE FECHA >= {last_update}
                        GROUP BY FECHA
                        ORDER BY FECHA ASC
                    """
                    query =f"""
                            WITH cand AS (
                            SELECT
                                FECHA,
                                VARIABLENOMBRE,
                                VALOR,
                                TO_CHAR(FECHA, 'MI') AS min_mark
                            FROM {v}
                            WHERE FECHA > TO_DATE('{last_update}', 'YYYY-MM-DD HH24:MI')
                            AND TO_CHAR(FECHA, 'MI') IN ('30','59')
                            ),
                            ultimos AS (
                            SELECT
                                min_mark,
                                MAX(FECHA) AS max_fecha
                            FROM cand
                            GROUP BY min_mark
                            )
                            SELECT
                            c.FECHA,
                            JSON_OBJECTAGG(c.VARIABLENOMBRE VALUE c.VALOR RETURNING VARCHAR2(32767)) AS ANSWERS
                            FROM cand c
                            JOIN ultimos u
                            ON c.min_mark = u.min_mark
                            AND c.FECHA     = u.max_fecha
                            GROUP BY c.FECHA
                            ORDER BY c.FECHA ASC"""

            print('query=',query)
            header, response = module_obj.sync_db_catalog(db_name=v, query=query)
            print('header=',header)
            print('response=',response)
            view = module_obj.views[v]
            schema = view['schema']
            catalog_id = view.get('catalog_id')
            form_id = view.get('form_id')
            data = module_obj.load_data(v, view, response, schema, catalog_id, form_id)
        
        if data:
            print('if data update values')
            module_obj.update_values(v, view, form_id)
