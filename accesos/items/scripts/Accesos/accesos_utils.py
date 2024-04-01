# -*- coding: utf-8 -*-
from datetime import datetime
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
            'colaborador_nombre_pase_entrada':'65e6515a7cc2c9fe60dfd8b7.5ea0693a0c12d5a8e43d37df',
            'colaborador_foto_pase_entrada':'65e6515a7cc2c9fe60dfd8b7.5ea35de83ab7dad56c66e045',
            'colaborador_identificacion_pase_entrada':'65e6515a7cc2c9fe60dfd8b7.65ce34985fa9df3dbf9dd2d0',
            'colaborador_curp_pase_entrada':'65e6515a7cc2c9fe60dfd8b7.5ea0897550b8dfe1f4d83a9f',
            'colaborador_status_pase_entrada':'65e6515a7cc2c9fe60dfd8b7.5ea1bd280ae8bad095055e61',
            'colaborador_rfc_pase_entrada':'65e6515a7cc2c9fe60dfd8b7.64ecc95271803179d68ee081',
            'vigencia_pase_entrada':'64ef9215b2f00d5312ca2790',
            'num_access_pase_entrada':'65e1086538c02be94fb61255',
            'status_pase_entrada':'64f20b7b018d04f897432961',
            'motivo_pase_entrada':'65e0a68a06799422eded24a6',
            'visita_area_pase_entrada':'65e8aae513dc1de0cf3cb922.65e2167be938f7075f58c66c',
            'visita_ubicacion_pase_entrada':'65e8aae513dc1de0cf3cb922.65e2167be938f7075f58c66d',
            'visita_nombre_pase_entrada':'65e8aae513dc1de0cf3cb922.65e2167be938f7075f58c66e',
            'autorizo_pase_entrada':'65e0a68a06799422eded24a8',
            'autorizo_telefono_pase_entrada':'65e0a68a06799422eded24a9',
            'nombrePlanta_planta_pase_entrada':'65ce31987cdf414c24e5e5d2.65e65159146a742ca9cd50be.65ce4088f9f1cb6b9fcbe45f',

            'plantas_pase_entrada':'65ce31987cdf414c24e5e5d2',
            'comentarios_pase_entrada':'65e0a68a06799422eded24aa',
            'permisos_pase_entrada':'65ce32228c2c5fa298cc3533',
            'accesos_pase_entrada':'65e0a9be35642d47a48cdbb3',
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
            'gafete_bitacora':'65e0b649bc72443361f99787',
            'date_in_bitacora':'65cbe03c6c78b071a59f481d',
            'date_out_bitacora':'65cbe03c6c78b071a59f481c',
            'duration_bitacora':'65cbe03c6c78b071a59f481e',
            'lista_equipo':'65f1dd9ebb61137c002b6248',
            'lista_vehicule':'65f1dd9ebb61137c002b6249',

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
        str_time = '';
        if date_start!='' and date_end!='':
            date_start = datetime.strptime(date_start, "%Y-%m-%d %H:%M:%S")
            date_end = datetime.strptime(date_end, "%Y-%m-%d %H:%M:%S")
            secondsTotal = date_end - date_start;
            secondsTotal = int(secondsTotal.seconds)
            hours = secondsTotal // 3600
            minutes = (secondsTotal % 3600) // 60
            seconds_res = secondsTotal % 60
            str_time="{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds_res)
        return str_time;

    def set_format_data_user(self, data):
        if len(data)>0:
            information = data[0];
            #----Variables Validation
            folio = information.get('folio',''); 
            name = information.get('colaborador_nombre_pase_entrada',''); 
            img = information.get('colaborador_foto_pase_entrada',''); 
            card = information.get('colaborador_identificacion_pase_entrada',''); 
            curp = information.get('colaborador_curp_pase_entrada',''); 
            status = information.get('colaborador_status_pase_entrada',''); 
            rfc = information.get('colaborador_rfc_pase_entrada',''); 
            validity = information.get('vigencia_pase_entrada',''); 
            num_access = information.get('num_access_pase_entrada',''); 
            status_pase = information.get('status_pase_entrada',''); 
            motivo = information.get('motivo_pase_entrada',''); 
            visita = information.get('visita_area_pase_entrada',''); 
            visita_ubicacion = information.get('visita_ubicacion_pase_entrada',''); 
            visita_nombre = information.get('visita_nombre_pase_entrada',''); 
            autorizo_pase = information.get('autorizo_pase_entrada',''); 
            autorizo_telefono = information.get('autorizo_telefono_pase_entrada',''); 
            comentarios_pase_entrada = information.get('comentarios_pase_entrada',''); 
            permisos_pase_entrada = information.get('permisos_pase_entrada',''); 
            accesos_pase_entrada = information.get('accesos_pase_entrada',''); 

            vehiculos_pase_entrada = information.get('vehiculos_pase_entrada',[]); 
            items_pase_entrada = information.get('items_pase_entrada',[]); 
            #----Format
            if len(img) > 0:
                img = img[0].get('file_url','');
            else:
                img = '';

            if len(card) > 0:
                card = card[0].get('file_url','');
            else:
                card = '';

            if type(curp) is list:
                curp = curp[0]

            if type(status) is list:
                status = status[0]

            if type(rfc) is list:
                rfc = rfc[0]
        

            #----Iteration Items
            list_instrucctions = [];
            for x in comentarios_pase_entrada:
                item_comment = x.get('65e0a69a322b61fbf9ed23af','');
                list_instrucctions.append(item_comment)

            #----Iteration Access
            list_access = [];
            for x in permisos_pase_entrada:
                item_access = x.get('65e6515ce63f84c46cc5342e','');
                status_access = x.get('65d6401cbc9e63afad61389d','');
                name_access = item_access.get('65ce4128e66b6e867cf71931','');
                list_access.append({'status_access':status_access,'name_access':name_access})

            #----Iteration Locations Access
            list_locations = [];
            for x in accesos_pase_entrada:
                access_location = x.get('65e6515a2bc93752355adce2',{});
                name_location = access_location.get('65e0abaea0f90e8aabf997d1',{});
                list_locations.append(name_location)

            #----Iteration Items
            list_items = [];
            for x in items_pase_entrada:
                item = x.get('65e6515b7b606391efcd503d','');
                type_item = item.get('65ce42a9650ac96bb07c2090','');
                marca_item = item.get('65d63d5b0040b43ab1f772cb','');
                model_item = item.get('65d63d5b0040b43ab1f772cb','');
                color_item = item.get('65d63da87b24c60e4422e4d3','');
                if type(color_item) is list:
                    color_item = color_item[0]

                list_items.append({'type_item':type_item,'marca_item':marca_item,'model_item':model_item,'color_item':color_item})

            #----Iteration Cars
            list_cars = [];
            for x in vehiculos_pase_entrada:
                car = x.get('65e6515cf2e45a00a2cd507e','');
                type_car = car.get('65d63e267b24c60e4422e4db','');
                model_car = car.get('65d63d5b0040b43ab1f772cb','');
                serie_car = car.get('65d63e267b24c60e4422e4dd','');
                color_car = car.get('65d63e267b24c60e4422e4de','');
                if type(color_car) is list:
                    color_car = color_car[0]

                list_cars.append({'type_car':type_car,'model_car':model_car,'serie_car':serie_car,'color_car':color_car})

            data_return = {
                'folio':folio,
                'name':name,
                'img':img,
                'card':card,
                'curp':curp,
                'status':status,
                'rfc':rfc,
                'validity':validity,
                'num_access':num_access,
                'status_pase':status_pase,
                'motivo':motivo,
                'visit':visita,
                'visit_location':visita_ubicacion,
                'visit_name':visita_nombre,
                'authorize_pase':autorizo_pase,
                'authorize_phone':autorizo_telefono,
                'list_instrucctions':list_instrucctions,
                'list_access':list_access,
                'list_locations':list_locations,
                'list_items':list_items,
                'list_cars':list_cars,
            }
            return data_return;

    def set_format_list_items(self, data):
        data_answers = [];
        data_list = data.get('listItemsData',[]);
        data_new = data.get('listNewItems',[]);
        if len(data_list)>0:
            for x in data_list:
                color_item = x.get('color_item','');
                marca_item = x.get('marca_item','');
                model_item = x.get('model_item','');
                check_item = x.get('check',False);
                type_item = x.get('type_item','');
               
                if check_item==True:
                    data_answers.append({
                        '65f1ddbc2d61fd93c82b6264':type_item,
                        '65f1ddbc2d61fd93c82b6265':marca_item,
                        '65f1ddbc2d61fd93c82b6266':model_item,
                        '65f3a268d381887a662b66cc':color_item,
                    })

        
        if len(data_new)>0:
            for x in data_new:
                color_item = x.get('inputColorItem','');
                marca_item = x.get('inputMarcaItem','');
                model_item = x.get('inputModeloItem','');
                type_item = x.get('inputTipoItem','');
                data_answers.append({
                    '65f1ddbc2d61fd93c82b6264':type_item,
                    '65f1ddbc2d61fd93c82b6265':marca_item,
                    '65f1ddbc2d61fd93c82b6266':model_item,
                    '65f3a268d381887a662b66cc':color_item,
                })
       
        print('RETURN ANSWERS',data_answers)
        return data_answers;

    def set_format_list_vehicules(self, data):
        data_answers = [];
        data_list = data.get('listVehiculesData',[]);
        data_new = data.get('listNewVehicules',[]);

        if len(data_list)>0:
            for x in data_list:
                type_car = x.get('type_car','');
                serie_car = x.get('serie_car','');
                model_car = x.get('model_car','');
                color_car = x.get('color_car','');
                check_car = x.get('check',False);
                if check_car==True:
                    data_answers.append({
                        '65f3a4039999e10d89fff2cb':type_car,
                        '65f1dde5831dc2235e2b621d':model_car,
                        '65f1dde5831dc2235e2b621e':color_car,
                    })
        if len(data_new)>0:
            for x in data_list:
                type_car = x.get('inputTipoCar','');
                marca_car = x.get('inputMarcaCar','');
                model_car = x.get('inputModeloCar','');
                color_car = x.get('inputColorCar','');
                placas_car = x.get('inputPlacasCar','');
                data_answers.append({
                    '65f3a4039999e10d89fff2cb':type_car,
                    '65f1dde5831dc2235e2b621c':marca_car,
                    '65f1dde5831dc2235e2b621d':model_car,
                    '65f1dde5831dc2235e2b621e':color_car,
                    '65f1dde5831dc2235e2b621f':placas_car,
                })

        return data_answers;

    def set_format_list_users(self, data, location):
        data_list = []
        for x in data:
            flag = False;
            name_user = x.get('colaborador_nombre_pase_entrada','')
            list_plantas = x.get('plantas_pase_entrada','')
            curp_user = x.get('colaborador_curp_pase_entrada','')
            if type(curp_user) is list:
                    curp_user = curp_user[0]

            for i in list_plantas:
                catalog_item = i.get('65e65159146a742ca9cd50be',{})
                catalog_validity = i.get('65e107d2acae1411004e1881','')
                catalog_location = catalog_item.get('65ce4088f9f1cb6b9fcbe45f','');
                #---Format
                if type(catalog_location) is list:
                    catalog_location = catalog_location[0]
                validty_datetime = datetime.strptime(catalog_validity, '%Y-%m-%d')
                if  validty_datetime >= datetime.now():
                    if catalog_location  == location:
                        flag = True;
            if flag :
                data_list.append({'name_user':name_user,'curp_user':curp_user})

        return data_list

    #---Change Record
    def set_add_record(self, data_user, data_item, data_vehicule, location = ''):
        #----Variables
        date_now = datetime.now()
        date_now_str = date_now.strftime('%Y-%m-%d %H:%M:%S')
        #-----Iteration Items
        data_format_items = self.set_format_list_items(data_item);
        #-----Iteration Vehicules
        data_format_cars = self.set_format_list_vehicules(data_vehicule);

        dic_answers = {
            f"{self.f['planta_bitacora']}":location,
            f"{self.f['curp_bitacora']}":data_user.get('curp',''),
            f"{self.f['nombre_bitacora']}":data_user.get('name',''),
            f"{self.f['rfc_bitacora']}":data_user.get('rfc',''),
            f"{self.f['visita_bitacora']}":data_user.get('visit_name',''),
            f"{self.f['acceso_bitacora']}":data_user.get('visita_ubicacion',''),
            f"{self.f['type_user_bitacora']}":'Pase de Entrada',
            f"{self.f['status_bitacora']}":'Autorizado',
            f"{self.f['date_in_bitacora']}":date_now_str,
            f"{self.f['lista_equipo']}":data_format_items,
            f"{self.f['lista_vehicule']}":data_format_cars,
        }
        #----Config
        metadata = self.lkf_api.get_metadata(form_id=self.FORM_BITACORA, user_id=self.record_user_id );
        metadata.update({
            'properties': {
                "device_properties":{
                    "System": "Script",
                    "Module": "Accesos",
                    "Process": "Add Record",
                    "Action": 'Created Record',
                    "File": "accesos_utils.py"
                }
            },
        })
       
        metadata.update({'answers':dic_answers})
        response_create = self.lkf_api.post_forms_answers(metadata)
        return response_create;

    def set_update_record(self, action):
        #----Variables
        date_now = datetime.now()
        date_out = date_now.strftime('%Y-%m-%d %H:%M:%S')
        date_in = action.get('record_date_in','')
        folio = action.get('folio','')
        duration =self.get_time_format(date_in,date_out);  
        #----Config
        dic_answers = {
            f"{self.f['date_out_bitacora']}":date_out,
            f"{self.f['duration_bitacora']}":duration,
        }
        response_update = self.lkf_api.patch_multi_record( answers=dic_answers, form_id=self.FORM_BITACORA, folios=[folio], threading=True )
        return response_update

    def set_add_record_gafete(self, curp = '', location = '', dataGafete = {}):
        print('Se ejecutan los gafetes');
        print('curp', curp);
        print('location', location);
        print('dataGafete',dataGafete);
        return ''


    #---Query Functions
    def get_alerts(self, location):
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
            f"answers.{self.f['colaborador_curp_pase_entrada']}":{"$in":[curp,[curp],'"' + curp + '"']},
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
                    "comentarios_pase_entrada": f"$answers.{self.f['comentarios_pase_entrada']}",
                    "permisos_pase_entrada": f"$answers.{self.f['permisos_pase_entrada']}",
                    "accesos_pase_entrada": f"$answers.{self.f['accesos_pase_entrada']}",
                    "vehiculos_pase_entrada": f"$answers.{self.f['vehiculos_pase_entrada']}",
                    "items_pase_entrada": f"$answers.{self.f['items_pase_entrada']}",
                }
            },
            {"$sort": {"folio":-1}},
            {'$limit':1}
        ]
        res = self.cr.aggregate(query)
        list_response = [x for x in res]
        response_format = self.set_format_data_user(list_response)
        return response_format

    def get_user_movement(self, curp):
        data_movement = {'type':'','folio':''}
        match_query = {
            "form_id":self.FORM_BITACORA,
            f"answers.{self.f['curp_bitacora']}":{"$in":[curp,[curp],'"' + curp + '"']},
        }       
        query= [{'$match': match_query },
            {"$project":
                {
                    "_id":0,
                    "folio":"$folio",
                    "date_in_bitacora": f"$answers.{self.f['date_in_bitacora']}",
                    "date_out_bitacora": f"$answers.{self.f['date_out_bitacora']}",
                }
            },
            {"$sort": {"folio":-1}},
            {'$limit':1}
        ]

        res = self.cr.aggregate(query)
        list_response = [x for x in res]

        if len(list_response) > 0:
            record = list_response[0];
            record_date_in = record.get('date_in_bitacora','');
            record_date_out = record.get('date_out_bitacora','');
            record_folio = record.get('folio','');
            if record_date_in !='' and record_date_out !='':
               data_movement['type'] = 'in';
            elif record_date_in != '' and record_date_out =='':
                data_movement['type'] = 'out';
                data_movement['folio'] = record_folio;
                data_movement['record_date_in'] = record_date_in;
        else:
            data_movement['type'] = 'in';
        return data_movement;

    def get_user_bitacora(self, curp):
        data_bitacora = []
        match_query = {
            "form_id":self.FORM_BITACORA,
            f"answers.{self.f['curp_bitacora']}":{"$in":[curp,[curp],'"' + curp + '"']},
        }       
        query= [{'$match': match_query },
            {"$project":
                {
                    "_id":0,
                    "folio":"$folio",
                    "visita": f"$answers.{self.f['visita_bitacora']}",
                    "acceso": f"$answers.{self.f['acceso_bitacora']}",
                    "date_in": f"$answers.{self.f['date_in_bitacora']}",
                    "date_out": f"$answers.{self.f['date_out_bitacora']}",
                    "duration": f"$answers.{self.f['duration_bitacora']}",
                }
            },
            {"$sort": {"folio":1}},
        ]   
        res = self.cr.aggregate(query)
        list_response = [x for x in res];
        return list_response;

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

    def get_list_users(self, location):
        match_query = {
            "form_id":self.FORM_PASE_DE_ENTRADA,
            f"answers.{self.f['status_pase_entrada']}":'activo',
        }
        
        query= [{'$match': match_query },
            {"$project":
                {
                    "_id":"$id",
                    "folio":"$folio",
                    "colaborador_nombre_pase_entrada": f"$answers.{self.f['colaborador_nombre_pase_entrada']}",
                    "colaborador_curp_pase_entrada": f"$answers.{self.f['colaborador_curp_pase_entrada']}",
                    "plantas_pase_entrada": f"$answers.{self.f['plantas_pase_entrada']}",
                }
            },
        ]
        res = self.cr.aggregate(query)
        list_response = [x for x in res]
        data_format = self.set_format_list_users(list_response, location)
        return data_format