# -*- coding: utf-8 -*-

from linkaform_api import base


from lkf_addons.addons.accesos.accesos_utils import Accesos

class Accesos(base.LKF_Base):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        #use self.lkm.catalog_id() to get catalog id
        self.name =  __class__.__name__
        self.settings = settings

        self.ACCESOS = self.lkm.catalog_id('accesos')
        self.ACCESOS_ID = self.ACCESOS.get('id')
        self.ACCESOS_OBJ_ID = self.ACCESOS.get('obj_id')

        # self.STOCK_INVENTORY = self.lkm.catalog_id('stock_inventory')
        # self.STOCK_INVENTORY_ID = self.STOCK_INVENTORY.get('id')
        # self.STOCK_INVENTORY_OBJ_ID = self.STOCK_INVENTORY.get('obj_id')

        self.ALTA_COLABORADORES_VISITANTES_ID = self.lkm.form_id('alta_de_colaboradores_visitantes','id')
        self.ALTA_EQUIPOS = self.lkm.form_id('alta_de_equipos','id')
        self.BITACORA = self.lkm.form_id('bitacora','id')


        self.f = {
            'curp':'64ee8349998263ad9bec20cc'
            'fecha_salida':'65cbe03c6c78b071a59f481c'
            'planta':'64ee8349998263ad9bec20cd'
        }



    #---Format Calc
    def get_time_format(self, date_start, date_end):
        date_start = datetime.strptime(date_start, "%Y-%m-%d %H:%M:%S")
        date_end = datetime.strptime(date_end, "%Y-%m-%d %H:%M:%S")
        secondsTotal = date_end - date_start;
        secondsTotal = int(secondsTotal.seconds)
        hours = secondsTotal // 3600
        minutes = (secondsTotal % 3600) // 60
        seconds_res = secondsTotal % 60
        return "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds_res)

    #---OPtions
    def set_data_update(self, curp_record, list_Data):
        #---Format Item
        dic_items = []
        if list_Data:
            for x in list_Data:
                data_index = x.split("|")
                name_item = ''
                desc_item = ''
                if data_index:
                    name_item = data_index[0]
                    desc_item = data_index[1]
                dic_items.append({
                    '65c67f4ab8db64319658fed1': name_item,
                    '65c67f4ab8db64319658fed2': desc_item
                })
        #---Query
        match_query = {
            "form_id":114233,
            "answers.5ea0897550b8dfe1f4d83a9f":curp_record,
        }

        query= [{'$match': match_query },
            {"$project":
                {
                    "folio":"$folio",
                    "form_id":"$form_id",
                    "answers": "$answers",
                }
            },
            {'$limit':1}
        ]
        res = cr.aggregate(query)
        list_response = [x for x in res]
        #---Update
        if len(list_response)  > 0:
            data_query = list_response[0];
            data_query['answers'].update({'65c67e21d7add8306488f064':dic_items});
            resp_update = lkf_api.patch_record(data_query ,jwt_settings_key = "USER_JWT_KEY")

    def get_data_query_items(self, curp_record):
        #---Query
        match_query = {
            "form_id":114233,
            "answers.5ea0897550b8dfe1f4d83a9f":curp_record,
        }

        query= [{'$match': match_query },
            {"$project":
                {
                    "folio": "$folio",
                    "items": "$answers.65c67e21d7add8306488f064",
                }
            },
        ]
        res = cr.aggregate(query)
        list_response = [x for x in res]
        if len(list_response) >0:
            list_data = []
            for x in list_response:
                list_items = x.get('items',[])
                for j in list_items:
                    name_item = j.get('65c67f4ab8db64319658fed1','');
                    desc_item = j.get('65c67f4ab8db64319658fed2','');
                    list_data.append({'name':name_item,'desc_item':desc_item});
            return list_data
        else:
            return []

    def set_out_user(self, curp_record, location):
        #-----Query Information
        dic_information_user = lkf_api.get_metadata(107815)
        time_elapsed = ''
        #-----Add Bitacora 
        match_query = {
                "form_id":107815,
                "answers.64ee8349998263ad9bec20cc":curp_record,
                "answers.64ee8349998263ad9bec20cd":location
            }
        query= [{'$match': match_query },
            {"$project":
                {
                    "_id":0,
                    "folio":"$folio",
                    "last_in": "$answers.65cbe03c6c78b071a59f481d",
                    "name_colaborador": "$answers.5ea0693a0c12d5a8e43d37df",
                    "company_colaborador": "$answers.64ecc95271803179d68ee081",
                }
            },
            {'$sort':{'folio':-1}},
            {'$limit':1}
        ]
        res = cr.aggregate(query)
        list_response = [x for x in res]
        if len(list_response)  > 0:
            date_now = datetime.now();
            date_in = list_response[0].get('last_in','')
            name_colaborador = list_response[0].get('name_colaborador','')
            company_colaborador = list_response[0].get('company_colaborador','')
            date_now_format = date_now.strftime("%Y-%m-%d %H:%M:%S")
            dic_information_user['answers'] = {
                '64ee8349998263ad9bec20cc': curp_record,
                '5ea0693a0c12d5a8e43d37df': name_colaborador,
                '64ecc95271803179d68ee081': company_colaborador,
                '64ee8349998263ad9bec20cd': location,
                '64ee8349998263ad9bec20ce': 'salida',
                '65cbe03c6c78b071a59f481d': date_in,
                '65cbe03c6c78b071a59f481c': date_now_format,
            }
            if date_in != '' and date_now_format!='':
                time_elapsed = get_time_format(date_in, date_now_format)
                dic_information_user['answers']['65cbe03c6c78b071a59f481e'] = time_elapsed;
                resp_create = lkf_api.post_forms_answers(dic_information_user, jwt_settings_key = "USER_JWT_KEY")


        #-----Update Status Colaborador
        match_query = {
            "form_id":114233,
            "answers.5ea0897550b8dfe1f4d83a9f":curp_record,
        }
        query= [{'$match': match_query },
            {"$project":
                {
                    "_id":0,
                    "folio":"$folio",
                    "5ea1bd280ae8bad095055e62": "$answers.5ea1bd280ae8bad095055e62",
                }
            },
            {'$limit':1}
        ]
        res = cr.aggregate(query)
        list_response = [x for x in res]
        if len(list_response)  > 0:
            dante_now = datetime.now();
            folio = list_response[0].get('folio','0');
            date_now_format = dante_now.strftime("%Y-%m-%d %H:%M:%S")
            dic_update = {'5ea1bd280ae8bad095055e62':'inactivo','65cbdcc7fa78c1a32e72b445':date_now_format,'65cbdd09b920d05515d86952':time_elapsed}
            resp_update = lkf_api.patch_multi_record(dic_update, 114233, folios = [folio] ,jwt_settings_key = "USER_JWT_KEY")



    #---New Functions
    def get_data_query(self, location):
        #---Query
        match_query = {
            "form_id":self.BITACORA,
            f"answers.{self.f['planta']}":location,
            f"answers.{self.f['fecha_salida']}":{"$exists": False } ,
        }

        query= [{'$match': match_query },
            {"$project":
                {
                    "folio":"$folio",
                    "id":"$id",
                    "curp": f"$answers.{self.f['crup']}",
                    "nombre": "$answers.5ea0693a0c12d5a8e43d37df",
                    "company": "$answers.64ecc95271803179d68ee081",
                    "date_start": "$answers.65cbe03c6c78b071a59f481d",
                    "equipment": "$answers.64ee8349998263ad9bec20cf",
                }
            },
        ]
        res = self.cr.aggregate(query)
        list_response = [x for x in res]
        #------Format
        list_data = []
        for x in list_response:
            curp = x.get('curp','') 
            nombre = x.get('nombre','') 
            company = x.get('company','') 
            date_start = x.get('date_start','')
            folio = x.get('folio','')
            record_id = x.get('id','')
            equipment = x.get('equipment',[]) 

            #----Iteration equipment
            list_items = []
            for x in equipment:
                name_item = x.get('64ee837b0392b342498edf35','')
                desc_item = x.get('64ee837b0392b342498edf36','')
                list_items.append({'name_item':name_item,'desc_item':desc_item})
            list_data.append({
                'record_id':record_id,
                'folio':folio,
                'nombre':nombre,
                'curp':curp,
                'date_start':date_start,
                'company':company,
                '_children':list_items
            })
        return list_data;

