# -*- coding: utf-8 -*-
import sys, simplejson
import os
from linkaform_api import settings
from account_settings import *
from datetime import datetime
#from linkaform_api.lkf_object import LKFBaseObject

from lkf_addons.addons.base.app import CargaUniversal
# from lkf_addons.addons.stock.app import Stock
# from lkf_addons.addons.jit.app import JIT
from jit_utils import JIT



wh_dict_loc = {'ALM GUADALAJARA':'CEDIS GUADALAJARA'}

class CargaUniversal(CargaUniversal):
    
    
    def carga_doctos_headers(self, form_id_to_load=None, read_excel_from=None, own_header=[]):
        if self.record_id:
            self.update_status_record('procesando')
        global error_records
        error_records = []
        #try:
        if True:
            """
            Obtengo los renglones y las cabeceras del excel
            """
            if not self.current_record:
                self.current_record = {'folio': 'ApiLKF', 'answers': {}}
                print('**** leyendo el archivo Excel =',read_excel_from)
                self.header, records = self.upfile.read_file(file_name=read_excel_from)
            else:
                answer_file = self.current_record['answers'][self.field_id_xls]
                file_url = answer_file[0]['file_url'] if type(answer_file) == list else answer_file['file_url']
                print('********************** file_url=',file_url)
                self.header, records = self.upfile.read_file(file_url=file_url)

            if own_header:
                self.header = own_header
            """
            Obtengo la información de la forma seleccionada del catálogo
            """
            print('form_id_to_load22222=',form_id_to_load)
            if not form_id_to_load:
                field_forma = self.answers[self.field_id_catalog_form]
                id_forma_seleccionada = field_forma[self.field_id_catalog_form_detail][0]
            else:
                id_forma_seleccionada = form_id_to_load
            form_fields = self.lkf_api.get_form_id_fields(id_forma_seleccionada)

            if not form_fields:
                return self.update_status_record('error', msg_comentarios='No se encontró la forma %s'%(str(id_forma_seleccionada)))
            else:
                fields = form_fields[0]['fields']
                # Obtengo solo los índices que necesito de cada campo
                info_fields = [{k:n[k] for k in ('label','field_type','field_id','groups_fields','group','options','catalog_fields','catalog','default_value') if k in n} for n in fields]
                #print("+++++ info_fields:",info_fields)
                # Obtengo dentro de las opciones avanzadas si el registro necesita un Folio o es automatico
                advanced_options = form_fields[0]['advanced_options']
                self.folio_manual = advanced_options.get('folio',{}).get('manual',False)
            """
            Empiezo a trabajar la carga de los documentos
            """
            # procesando el archivo zip de carga
            if self.current_record.get('answers',{}).get(self.field_id_zip,{}):
                nueva_ruta, archivo_zip, file = self.procesa_zip(self.current_record, self.record_id)
                files_dir = os.listdir(nueva_ruta)
            else:
                nueva_ruta = ''
                files_dir = []
            #print("+++ files_dir:",files_dir)
            # self.header_dict contiene un diccionario con las cabeceras del xls y su posición, por ejemplo {cabecera1: 0}, los espacios se reemplazan por _
            self.header_dict = self.make_header_dict(self.header)
            print("------- header_dict=",self.header_dict)
            # Valido que si el folio es manual venga una columna Folio en la primera columna del excel
            if self.folio_manual and (not self.header_dict.get('folio') or self.header_dict['folio'] > 0):
                return self.update_status_record('error', msg_comentarios='La forma tiene configurado el Folio manual y el archivo de carga no cuenta con la columna Folio')
            # Si el archivo de carga trae en la primera columna el campo "Folio" obtengo la lista y consulto si existen registros con ese folio en la forma correspondiente
            self.existing_records = {}
            if self.header_dict.get('folio') and self.header_dict['folio'] == 0:
                folios = [self.strip_special_characters(rec[0]) for rec in records if rec[0]]
                self.existing_records = self.get_records_existentes(id_forma_seleccionada, folios)
                #print("---------- self.existing_records:",self.existing_records)
            #print("++++ self.header_dict:",self.header_dict)
            # pos_field_dict contiene un diccionario con la posición del campo dentro de los registros del excel
            pos_field_dict = {self.header_dict[f['label'].lower().replace(' ','_')]:f for f in info_fields
                if f['label'].lower() != 'folio'
                and f['field_type'] not in ('catalog','catalog-select','catalog-detail')
                and f['label'].lower().replace(' ','_') in self.header_dict.keys()}
            # print( 'pos_field_dict=',pos_field_dict)

            self.ids_fields_no_update = [f['field_id'] for f in info_fields if f.get('label', '').lower().replace(' ','_') in self.fields_no_update]
            #print 'self.ids_fields_no_update=',self.ids_fields_no_update
            if self.header_dict.get('folio__enCampo'):
                pos_field_dict.update({self.header_dict['folio__enCampo']:f for f in info_fields if f['label'].lower() == 'folio' and not f['group']})
            #print("++++ pos_field_dict:",pos_field_dict)
            # Verifico si la carga tiene algún campo que sea de tipo Documento o Imagen
            fields_files = [pos_field_dict[p] for p in pos_field_dict if pos_field_dict[p]['field_type'] in ('file','images')]
            #print("++++ fields_files:",fields_files)
            # Obtengo un diccionario de los campos que son de tipo Catálogo
            dict_catalogs = {f['field_id']:f for f in info_fields if f['field_type']=='catalog'}
            dict_grupos = {f['field_id']:f['label'].lower().replace(' ','_') for f in info_fields if f['groups_fields']}
            #print("+++++ dict_grupos:",dict_grupos)
            #print("+++++ dict_catalogs:",dict_catalogs)
            for idc in dict_catalogs:
                ccat = dict_catalogs[idc]
                filters = ccat.get('catalog',{}).get('filters','')
                if filters:
                    info_catalog = self.lkf_api.get_catalog_id_fields(ccat.get('catalog',{}).get('catalog_id',0))
                    dict_filters = info_catalog.get('catalog',{}).get('filters',{})
                    #print('+++++ dict_filters',dict_filters)
                    ccat['catalog']['filters'] = dict_filters.get(filters, {})

                    # Reviso si hay campos repetidos en el filtro para entonces armar los $or
                    if dict_filters.get(filters):
                        list_repeated_fields = self.get_repeated_ids_in_filter(dict_filters.get(filters, {}).get('$and', []))
                        if list_repeated_fields:
                            print('list_repeated_fields =====',list_repeated_fields)
                            new_and = []
                            fields_or = {}
                            #dict_filters[filters]['$and'].append({'6205f73281bb36a6f1500000': {'$eq': 'Test01'}})
                            for rr in dict_filters.get(filters, {}).get('$and', []):
                                rr_idfield = list(rr.keys())[0]
                                if rr_idfield in list_repeated_fields:
                                    if not fields_or.get(rr_idfield):
                                        fields_or[rr_idfield] = {'$or': []}
                                    fields_or[rr_idfield]['$or'].append(rr[rr_idfield])
                                else:
                                    new_and.append(rr)
                            print('........... fields_or=',fields_or)
                            for fff in fields_or:
                                new_and.append({fff: fields_or[fff]})
                            print('........... new_and=',new_and)
                            ccat['catalog']['filters'] = {'$and':new_and}
            #print("----- dict_catalogs con filtro:", dict_catalogs)
            # Reviso si existen grupos repetitivos en el archivo de carga
            fields_groups = { f['field_id']: f['label'] for f in info_fields if f['field_type'] == 'group' }
            fields_for_sets = {}
            fields_for_catalog = {}

            dict_group_catalogs_inside = {}
            for f in info_fields:
                if f.get('group', False):
                    g_id = f['group']['group_id']
                    if f['field_type'] == 'catalog':
                        if not dict_group_catalogs_inside.get(g_id):
                            dict_group_catalogs_inside[g_id] = {}
                        if not dict_group_catalogs_inside[g_id].get( f['field_id'] ):
                            dict_group_catalogs_inside[g_id][ f['field_id'] ] = []
                    elif f['field_type'] == 'catalog-select':
                        dict_group_catalogs_inside[g_id][ f['catalog']['catalog_field_id'] ].append( f['label'] )
                    else:
                        if not fields_for_sets.get(g_id, False):
                            fields_for_sets[g_id] = []
                        fields_for_sets[g_id].append(f['label'])
                if f['field_type'] == 'catalog-select' and f.get('catalog', False):
                    c_id = f['catalog']['catalog_field_id']
                    if not fields_for_catalog.get(c_id, False):
                        fields_for_catalog[c_id] = []
                    fields_for_catalog[c_id].append(f['label'])

            list_groups = []
            for _g_id in dict_group_catalogs_inside:
                if not fields_for_sets.get(_g_id):
                    fields_for_sets[_g_id] = []
                for _c_id in dict_group_catalogs_inside[_g_id]:
                    _label_catalog = dict_catalogs[_c_id]['label']
                    for _label_select in dict_group_catalogs_inside[_g_id][_c_id]:
                        fields_for_sets[_g_id].append( '{}: {}'.format( _label_catalog, _label_select ) )

            for g in fields_groups:
                for s in fields_for_sets.get(g, []):
                    list_groups.append(u'{}:_{}'.format(fields_groups[g].lower().replace(' ', '_'), s.lower().replace(' ', '_')))
            list_catalogs = []
            for c in dict_catalogs:
                for d in fields_for_catalog.get(c, []):
                    list_catalogs.append(u'{}:_{}'.format(dict_catalogs[c]['label'].lower().replace(' ', '_'), d.lower().replace(' ', '_')))

            grupos_en_excel = {h:self.header_dict[h] for h in self.header_dict if h in list_groups}
            #print('++++ grupos_en_excel', grupos_en_excel)
            #catalogs_en_excel = {h:self.header_dict[h] for h in self.header_dict if h in list_catalogs}
            catalogs_en_excel = {}
            for h in self.header_dict:
                pos_h = self.header_dict[h]
                if ':_' in h:
                    if h in list_catalogs:
                        catalogs_en_excel.update({ h: pos_h })
                    else:
                        list_h = h.split(':_')
                        if len(list_h) == 3:
                            to_eval_in_group = list_h[0] + ':_' + list_h[2]
                            if to_eval_in_group in list_groups:
                                catalogs_en_excel.update({
                                    '{}:_{}'.format(list_h[1], list_h[2]): pos_h
                                })
            grupos_en_excel.update( catalogs_en_excel )
            if grupos_en_excel:
                pos_field_dict_grupos = {}
                for grupo_campo in grupos_en_excel:
                    pos = grupos_en_excel[grupo_campo]
                    list_grupos = grupo_campo.split(":_")
                    label_campo = list_grupos[len(list_grupos)-1]
                    #pos_field_dict_grupos.update( {pos:f for f in info_fields if label_campo == f['label'].lower().replace(' ','_') and (f['group'] or f['field_type']=='catalog-select')} )
                    for f in info_fields:
                        f_label = f['label'].lower().replace(' ','_')
                        if f['field_type']=='catalog-select':# and label_campo == f_label:
                            label_catalog = dict_catalogs[f['catalog']['catalog_field_id']]['label'].lower().replace(' ','_')
                            if (label_catalog+':_'+f_label) in grupo_campo:
                                pos_field_dict_grupos.update({pos:f})
                        if f['group']:
                            label_grupo = dict_grupos[f['group']['group_id']]
                            if grupo_campo == label_grupo+':_'+f_label:
                                pos_field_dict_grupos.update({pos:f})
                pos_field_dict.update(pos_field_dict_grupos)
            #print("------ pos_field_dict:",pos_field_dict)
            # Creo una lista con las posiciones de los campos que no son Grupos, me servirá para crear o aun no el registro
            self.not_groups = [self.header_dict[h] for h in self.header_dict if h not in list_groups and h != 'folio']
            # Metadatos de la forma donde se creará el registro
            metadata_form = self.lkf_api.get_metadata(form_id=id_forma_seleccionada )
            # Necesito un diccionario que agrupe los registros que se crearán y los que están en un grupo repetitivo y pertenecen a uno principal


            new_records = []
            records = self.get_record_answers(records, id_forma_seleccionada)

            group_records = {i:[] for i,r in enumerate(records) if [r[j] for j in self.not_groups if r[j]]}
            grupo = None
            for i, r in enumerate(records):
                if grupo == None:
                    continue
                if [r[j] for j in self.not_groups if r[j]]:
                    grupo = i
                else:
                    group_records[grupo].append(i)
            # print("++++ group_records",group_records)
            # Obtengo una lista de campos que son de tipo file o images
            file_records = [i for i in pos_field_dict if pos_field_dict[i]['field_type'] in ('file','images')]
            # Agrego información de la carga
            metadata_form.update({'properties': {"device_properties":{"system": "SCRIPT","process":"Carga Universal", "accion":'CREA Y ACTUALIZA REGISTROS DE CUALQUIER FORMA', "folio carga":self.current_record['folio'], "archive":"carga_documentos_a_forma.py"}}})

            metadata = None
            return records, pos_field_dict, files_dir, nueva_ruta, id_forma_seleccionada, dict_catalogs, group_records
        

    # def convert_datetime_to_string(self, date_value):
    #     '''
    #     Convierte un objeto datetime a una cadena en formato ISO 8601.
    #     '''
    #     if isinstance(date_value, datetime):
    #         # Si el valor es un objeto datetime, lo convertimos a formato ISO 8601
    #         return date_value.isoformat()
    #     elif isinstance(date_value, str):
    #         # Si ya es una cadena, retornamos la cadena tal cual
    #         return date_value
    #     return None  # En caso de que el valor no sea ni string ni datetime, retorna None.


    def get_fields_for_answers(self):
        
        now = datetime.now()
        
        fields = {}
        fields['user_id'] = self.user['user_id']
        fields['user_name'] = self.user['username']
        fields['assets'] = {
            "template_conf" : None,
            "followers" : [ 
                {
                    "asset_id" : 16102,
                    "email" : "linkaform@industriasmiller.com",
                    "name" : "Industrias MIller",
                    "username" : None,
                    "rtype" : "user",
                    "rules" : None
                }
            ],
            "supervisors" : [],
            "performers" : [],
            "groups" : []
        },
        fields['created_at'] = now.isoformat()
        fields['updated_at'] = now.isoformat()
        fields['editable'] = True
        fields['start_timestamp'] = 1730843497.972
        fields['end_timestamp'] = 1730843554.625
        fields['start_date'] = now.isoformat()
        fields['end_date'] = now.isoformat()
        fields['duration'] = 56
        fields['created_by_id'] = self.user['user_id']
        fields['created_by_name'] = self.user['username']
        fields['created_by_email'] = "linkaform@industriasmiller.com"
        fields['timezone'] = "America/Monterrey"
        fields['tz_offset'] = 360
        fields['other_versions'] = []
        #fields['voucher_id'] = ''        
    
        return fields
    
    
    def carga_doctos_records(self, records, pos_field_dict, files_dir, nueva_ruta, id_forma_seleccionada, dict_catalogs, group_records):
        self.field_id_error_records = '5e32fbb498849f475cfbdca3'
        metadata_form = self.lkf_api.get_metadata(form_id=id_forma_seleccionada)
        
        answer_fields = self.get_fields_for_answers()
        # print('///ANSWER FIELDS', answer_fields)
        # print(stop)
    
        # Necesito un diccionario que agrupe los registros que se crearán y los que están en un grupo repetitivo y pertenecen a uno principal
        file_records = [i for i in pos_field_dict if pos_field_dict[i]['field_type'] in ('file','images')]
        print("++++ file_records",file_records)
        # Agrego información de la carga
        metadata_form.update({'properties': {"device_properties":{"system": "SCRIPT","process":"Carga Universal", "accion":'CREA Y ACTUALIZA REGISTROS DE CUALQUIER FORMA', "folio carga":self.current_record['folio'], "archive":"carga_documentos_a_forma.py"}}})
        metadata = None
        print("***** Empezando con la carga de documentos *****")
        resultado = {'creados':0,'error':0,'actualizados':0, 'no_update':0}
        answers = {}
        total_rows = len(records)
        subgrupo_errors = []
        dict_records_to_multi = {'create': [], 'update': []}
        dict_records_copy = {'create': [], 'update': {}}
        list_cols_for_upload = list( pos_field_dict.keys() )
        for p, record in enumerate(records):
            print("========================================== >> Procesando renglon:",p)
            # if step == 'carga_stock':

            #     record[1] = wh_dict_loc[record[1]] if wh_dict_loc.get(record[1]) else record[1]
            # print('record', record)
            if p in subgrupo_errors:
                error_records.append(record+['',])
                continue
            # Recorro la lista de campos de tipo documento para determinar si el contenido en esa posición está dentro del zip de carga
            no_en_zip = [record[i] for i in file_records if record[i] and record[i] not in files_dir]
            new_record = [record[i] for i in self.not_groups if record[i] and i in list_cols_for_upload]
            if new_record and p != 0:
                if metadata.get('answers',{}):
                    proceso = self.crea_actualiza_record(metadata, self.existing_records, error_records, records, sets_in_row, dict_records_to_multi, dict_records_copy, self.ids_fields_no_update)
                    if proceso:
                        resultado[proceso] += 1
                answers = {}
            if new_record:
                sets_in_row = {p: group_records[p]}
            if no_en_zip:
                docs_no_found = ''
                docs_no_found += ', '.join([str(a) for a in no_en_zip if a])
                error_records.append(record+['Los documentos %s no se encontraron en el Zip'%(docs_no_found),])
                resultado['error'] += 1
                if new_record:
                    subgrupo_errors = group_records[p]
                    metadata = metadata_form.copy()
                    metadata.update({'answers': {}})
                continue

            answers = self.procesa_row(pos_field_dict, record, files_dir, nueva_ruta, id_forma_seleccionada, answers, p, dict_catalogs)
            print('EXAMPLE ANSWERS!!!', answers)
            if new_record:
                if self.folio_manual and not record[0]:
                    error_records.append(record+['La forma tiene configurado el folio manual por lo que el registro requiere un número de Folio'])
                    resultado['error'] += 1
                    subgrupo_errors = group_records[p]
                    continue
                metadata = metadata_form.copy()
                metadata.update({'answers': answers})
                
                metadata.update({
                    "user_id": answer_fields.get('user_id', ''),
                    "user_name": answer_fields.get('user_name', ''),
                    "assets": answer_fields.get('assets', {}),
                    "created_at": answer_fields.get('created_at', ''),
                    "updated_at": answer_fields.get('updated_at', ''),
                    "editable": answer_fields.get('editable', True),
                    "start_timestamp": answer_fields.get('start_timestamp', 0),
                    "end_timestamp": answer_fields.get('end_timestamp', 0),
                    "start_date": answer_fields.get('start_date', ''),
                    "end_date": answer_fields.get('end_date', ''),
                    "duration": answer_fields.get('duration', 0),
                    "created_by_id": answer_fields.get('created_by_id', ''),
                    "created_by_name": answer_fields.get('created_by_name', ''),
                    "created_by_email": answer_fields.get('created_by_email', ''),
                    "timezone": answer_fields.get('timezone', ''),
                    "tz_offset": answer_fields.get('tz_offset', 0),
                    "other_versions": answer_fields.get('other_versions', []),
                    "voucher_id": answer_fields.get('voucher_id', '')
                })
                
                if self.folio_manual or (self.header_dict.get('folio') and self.header_dict['folio'] == 0):
                    metadata.update({'folio':str(record[0])})
            if p == total_rows-1:
                if metadata == None:
                    metadata = metadata_form.copy()
                    metadata.update({'answers': answers})
                    sets_in_row = {
                        0: list(group_records.keys())
                    }
                proceso = self.crea_actualiza_record(metadata, self.existing_records, error_records, records, sets_in_row, dict_records_to_multi, dict_records_copy, self.ids_fields_no_update)
                if proceso:
                    resultado[proceso] += 1
                
        for item in dict_records_to_multi['create']:            
            item.pop("sets_in_row")
            print('///ITEM', simplejson.dumps(item, indent=3))
            res = self.cr.insert_one(item)
            print('///RESULT', res)
            
            # res = self.cr.insert_many(dict_records_to_multi['create'])
            # print('RESULTADO', res)
            
        #print('***************dict_records_copy=',dict_records_copy)
        #dict_sets_in_row = {}
        # if dict_records_to_multi['create']:
        #     dict_sets_in_row = {x: list_create['sets_in_row'] for x, list_create in enumerate(dict_records_to_multi['create'])}
        #     response_multi_post = self.lkf_api.post_forms_answers_list(dict_records_to_multi['create'])
        #     #print('===== response_multi_post:', response_multi_post)
        #     for x, dict_res in enumerate(response_multi_post):
        #         sets_in_row = dict_sets_in_row[x]
        #         res_status = dict_res.get('status_code', 300)
        #         if res_status < 300:
        #             resultado['creados'] += 1
        #         else:
        #             resultado['error'] += 1
        #             msg_error_sistema = self.arregla_msg_error_sistema(dict_res)
        #             for g in sets_in_row:
        #                 s = sets_in_row[g]
        #                 error_records.append(dict_records_copy['create'][g]+[msg_error_sistema,])
        #                 for dentro_grupo in s:
        #                     error_records.append(dict_records_copy['create'][dentro_grupo]+['',])
        # if dict_records_to_multi['update']:
        #     #dict_sets_in_row = {x: list_create['sets_in_row'] for x, list_create in enumerate(dict_records_to_multi['update'])}
        #     response_bulk_patch = self.lkf_api.bulk_patch(dict_records_to_multi['update'], id_forma_seleccionada, threading=True)
        #     print('===== response_bulk_patch=',response_bulk_patch)
        #     for f in response_bulk_patch:
        #         dict_res = response_bulk_patch[ f ]
        #         sets_in_row = dict_records_copy['update'][f]
        #         res_status = dict_res.get('status_code', 300)
        #         if res_status < 300:
        #             resultado['actualizados'] += 1
        #         else:
        #             resultado['error'] += 1
        #             msg_error_sistema = self.arregla_msg_error_sistema(dict_res)
        #             for g in sets_in_row:
        #                 s = sets_in_row[g]
        #                 error_records.append(records[g]+[msg_error_sistema,])
        #                 for dentro_grupo in s:
        #                     error_records.append(records[dentro_grupo]+['',])
        try:
            if files_dir:
                # Elimino todos los archivos después de que ya los procesé
                for file_cargado in files_dir:
                    os.remove(os.path.join(nueva_ruta, file_cargado))
                #os.remove(nueva_ruta+file)
                #shutil.rmtree(nueva_ruta)
        except Exception as e:
            print("********************* exception borrado",e)
            return False
        if not resultado['error']:
            if self.current_record['answers'].get(self.field_id_error_records):
                sin_file_error = self.current_record['answers'].pop(self.field_id_error_records)
            if not resultado['creados'] and not resultado['actualizados']:
                return self.update_status_record('error', msg_comentarios='Registros Creados: %s, Actualizados: %s, No actualizados por información igual: %s'%(str(resultado['creados']), str(resultado['actualizados']), str(resultado['no_update'])))
            else:
                return self.update_status_record('carga_terminada', msg_comentarios='Registros Creados: %s, Actualizados: %s, No actualizados por información igual: %s'%(str(resultado['creados']), str(resultado['actualizados']), str(resultado['no_update'])))
        else:
            if error_records:
                if self.record_id:
                    self.current_record['answers'].update( self.lkf_api.make_excel_file(self.header + ['error',], error_records, self.current_record['form_id'], self.field_id_error_records) )
                else:
                    error_file = self.lkf_api.make_excel_file(self.header + ['error',], error_records, None, self.field_id_error_records, is_tmp=True)
                    dict_respuesta = {
                        'error': 'Registros Creados: {}, Actualizados: {}, Erroneos: {}, No actualizados por información igual: {}'.format( resultado['creados'], resultado['actualizados'], resultado['error'], resultado['no_update'] )
                    }
                    dict_respuesta.update(error_file)
                    return dict_respuesta
            return self.update_status_record('error', msg_comentarios='Registros Creados: %s, Actualizados: %s, Erroneos: %s, No actualizados por información igual: %s'%(str(resultado['creados']), str(resultado['actualizados']), str(resultado['error']), str(resultado['no_update'])))

       
    def procesa_row(self, pos_field_dict, record, files_dir, nueva_ruta, id_forma_seleccionada, answers, p, dict_catalogs):
        print("entraaaa ")
        #print(stop)
        error = []
        grupo_repetitivo = {}
        fields_to_find_catalog = {}
        for pos in pos_field_dict:
            field = pos_field_dict[pos]
            #print('---------- field label = {} default_value = {}'.format(field['label'], field.get('default_value')))
            if not record[pos] and field.get('default_value'):
                record[pos] = field['default_value']
            if record[pos] or record[pos] == 0:
                if field['field_type'] in ('images','file'):
                    res_upload_docto = self.upload_docto(nueva_ruta, record[pos], id_forma_seleccionada, field['field_id'])
                    if res_upload_docto.get('error'):
                        error.append('Ocurrió un error al cargar el documento %s'%(str(record[pos])))
                    else:
                        if field['field_type'] == 'images':
                            field_add = {field['field_id']: [res_upload_docto]}
                        else:
                            field_add = {field['field_id']: res_upload_docto}
                        if field['group']:
                            group_id = field.get('group').get('group_id')
                            if not grupo_repetitivo.get(group_id):
                                grupo_repetitivo.update({group_id: {}})
                            grupo_repetitivo[group_id].update(field_add)
                        else:
                            answers.update(field_add)
                elif field['field_type'] == 'catalog-select':
                    catalog_field_id = field.get('catalog',{}).get('catalog_field_id','')
                    if not fields_to_find_catalog.get(catalog_field_id):
                        fields_to_find_catalog.update({catalog_field_id:[]})
                    fields_to_find_catalog[catalog_field_id].append({field['field_id']: {'$eq': record[pos]}})
                else:
                    if field['field_type'] == "date":
                        try:
                            record[pos] = datetime.strptime(record[pos], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
                        except Exception:
                            error.append(f'Formato de fecha incorrecto: {field["label"]}')
                    elif field['field_type'] == "datetime":
                        try:
                            record[pos] = record[pos].strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            error.append(f'Formato de fecha y hora incorrecto: {field["label"]}')
                    elif field['field_type'] == "time":
                        try:
                            record[pos] = record[pos].strftime("%H:%M:%S")
                        except Exception:
                            error.append(f'Formato de hora incorrecto: {field["label"]}')
                    elif field['field_type'] == "email":
                        email_validate = re.match(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", record[pos])
                        if not email_validate:
                            error.append('La estructura del email no es correcta: '+field['label'])
                    elif field['field_type'] == "checkbox":
                        optsXls = [opCheck.lower().strip().replace(' ', '_') for opCheck in str(record[pos]).split(',')]
                        optsAcepted = [ option['value'] for option in field['options'] ]
                        esta_en_opciones = True
                        for opxls in optsXls:
                            if opxls not in optsAcepted:
                                esta_en_opciones = False
                                break
                        if esta_en_opciones:
                            str_opts = ''
                            str_opts += ', '.join([a for a in optsXls if a])
                            record[pos] = str_opts
                        else:
                            error.append('Alguna de las opciones no esta dentro de los valores definidos: '+field['label'])
                    elif field['field_type'] in ["radio","select"]:
                        record_value = record[pos].lower().replace(' ' ,'_')
                        esta_en_opciones = False
                        for option in field['options']:
                            if record_value == option['value']:
                                esta_en_opciones = True
                                break
                        if esta_en_opciones:
                            record[pos] = record_value
                        else:
                            error.append('La opcion no esta dentro de los valores definidos: '+field['label'])
                    elif field['field_type'] in ["text", "textarea", "description"]:
                        #if isinstance(record[pos], types.UnicodeType):
                        try:
                            record[pos] = record[pos].decode('utf-8')
                        except:
                            pass
                    elif field['field_type'] == "integer":
                        try:
                            record[pos] = int(record[pos])
                        except:
                            error.append('No es posible convertir una cadena a entero: '+field['label'])
                    elif field['field_type'] == "decimal":
                        try:
                            record[pos] = float(record[pos])
                        except:
                            error.append('No es posible convertir una cadena a decimal: '+field['label'])
                    # if isinstance(pos, types.IntType):
                    #     if isinstance(record[pos], types.UnicodeType):
                    #         record[pos] = record[pos].decode('utf-8')
                    if field['group']:
                        group_id = field.get('group').get('group_id')
                        if not grupo_repetitivo.get(group_id):
                            grupo_repetitivo.update({group_id: {}})
                        grupo_repetitivo[group_id].update(self.lkf_api.make_infosync_json(record[pos], field, best_effort=True))
                    else:
                        answers.update(self.lkf_api.make_infosync_json(record[pos], field, best_effort=True))
        if fields_to_find_catalog:
            for id_cat in dict_catalogs:
                cont_cat = dict_catalogs[id_cat]
                filter_catalog = fields_to_find_catalog.get(id_cat,[])
                filter_in_catalog = cont_cat.get('catalog',{}).get('filters',{})
                if filter_in_catalog:
                    #print("----- filter_in_catalog:",filter_in_catalog)
                    filter_catalog.extend(filter_in_catalog['$and'])
                if filter_catalog:
                    catalog_id = cont_cat.get('catalog',{}).get('catalog_id','')
                    #print("+++++ filter_catalog:",filter_catalog)
                    #row_catalog = cat_utils.get_row_by_selector(catalog_id, {'$and':filter_catalog})
                    mango_query = {"selector":
                        {"answers":
                            {"$and":filter_catalog}
                        },
                        "limit":20,"skip":0}
                    row_catalog = self.lkf_api.search_catalog( catalog_id, mango_query )
                    #print("+++++ row_catalog:",row_catalog)
                    if row_catalog:
                        catalog_fields = cont_cat.get('catalog_fields',[])
                        view_fields = cont_cat.get('catalog',{}).get('view_fields',[])
                        dict_row_catalog = row_catalog[0]
                        dict_record_catalog_to_save = {}
                        for id_field_catalog in dict_row_catalog:
                            content_catalog = dict_row_catalog[id_field_catalog]
                            if id_field_catalog in view_fields:
                                dict_record_catalog_to_save.update({id_field_catalog: content_catalog})
                            elif id_field_catalog in catalog_fields:
                                if type(content_catalog) == list and content_catalog and type(content_catalog[0]) == dict and content_catalog[0].get('file_url'):
                                    content_catalog = content_catalog[0]
                                dict_record_catalog_to_save.update({id_field_catalog: [content_catalog]})
                        if cont_cat['group']:
                            group_id = cont_cat.get('group').get('group_id')
                            if not grupo_repetitivo.get(group_id):
                                grupo_repetitivo.update({group_id: {}})
                            grupo_repetitivo[group_id].update({id_cat:dict_record_catalog_to_save})
                        else:
                            answers.update({id_cat:dict_record_catalog_to_save})
                    else:
                        print('----- No se encontro info en el catalogo con el filtro = ',mango_query)
                        error.append('No se encontro informacion en el catalogo '+cont_cat['label'])
        if grupo_repetitivo:
            for id_g in grupo_repetitivo:
                cont_g = grupo_repetitivo[id_g]
                if answers.get(id_g):
                    answers[id_g].append(cont_g)
                else:
                    answers.update({id_g: [cont_g]})
        if error:
            #print("---------error:",error)
            msg_errores_row = ''
            #msg_errores_row += ', '.join([str(a) for a in error if a])
            for a in error:
                if a:
                    # if isinstance(a, types.UnicodeType):
                    #     a = a.decode('utf-8')
                    msg_errores_row += (a+', ')
            answers.update({'error': 'Registro con errores'})
            if not answers.get('dict_errors'):
                answers.update({'dict_errors':{}})
            answers['dict_errors'].update({p: msg_errores_row})
            
        print(f"Respuesta final para el registro {p}:\n ANSWERSSSS {answers}")
        return answers

    
    def get_record_answers(self, records, form_id):
        # self.load('Stock', **self.kwargs)
        self.load('JIT', **self.kwargs)
        # self.STOCK = Stock( self.settings, sys_argv=self.sys_argv, use_api=self.use_api)
        # self.JIT = JIT( self.settings, sys_argv=self.sys_argv, use_api=self.use_api)

        wh_dict = {
            'ALM MONTERREY':'Almacen Monterrey',
            'ALM MERIDA':'Almacen Merida',
            'ALM GUADALAJARA':'Almacen Guadalajara',
            'CEDIS GUADALAJARA':'Almacen CEDIS Guadalajara',
            }
        new_records = []
        if form_id == self.JIT.DEMANDA_UTIMOS_12_MES:
            for x in records[:]:
                row = []
                warehouse_id = x.pop(0)
                warehouse_name = x.pop(0)
                sku = x.pop(0)
                demand = x.pop(0)
                inv = x.pop(0)
                uom = 'unit'
                row.insert(0,'2024-10-01 12:00:00')
                row.insert(1,warehouse_name) 
                row.insert(2,uom) 
                row.insert(3,sku)
                row.insert(4,sku)
                row.insert(5,'') 
                row.insert(6,demand) 
                new_records.append(row)
        elif form_id == self.Stock.STOCK_INVENTORY_ADJUSTMENT_ID:#123136:
            for x in records[:]:
                row = []
                warehouse_id = x.pop(0)
                warehouse_name = x.pop(0)
                warehouse_name = wh_dict_loc[warehouse_name] if wh_dict_loc.get(warehouse_name) else warehouse_name
                sku = x.pop(0)
                demand = x.pop(0)
                inv = x.pop(0)
                row.insert(0,'2024-10-01 12:00:00')
                row.insert(1,'todo')
                row.insert(2,warehouse_name) 
                row.insert(3,wh_dict[warehouse_name]) 
                row.insert(4,sku)
                row.insert(5,sku)
                row.insert(6,'lote1') 
                row.insert(7,'todo') 
                row.insert(8,'') 
                row.insert(9,'') 
                row.insert(10,inv) 
                new_records.append(row)
                print('row=',row)
        return new_records
    
    # def db_options(self):
    #     print(dir(self.cr))


if __name__ == '__main__':
    class_obj = CargaUniversal(settings=settings, sys_argv=sys.argv, use_api=True)
    class_obj.console_run()
    class_obj.load('Stock', **class_obj.kwargs)
    jit_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    step = class_obj.data.get('step')
        
    if step == 'demanda':
        
        from_id = jit_obj.DEMANDA_UTIMOS_12_MES
        header = [
            'fecha',
            'almacen:_warehouse_name',
            'unidad_de_medida:_unidad_de_medida', 
            'producto:_product_code', 
            'producto:_sku',
            '', 
            'demanda_ultimos_12_meses',]

        estatus = 'demanda_cargada'
        borrar = class_obj.answers.get(jit_obj.f.get('borrar_historial'))
        if borrar == 'si':
            jit_obj.borrar_historial()
        
        #    print()
    elif step == 'carga_stock':
        from_id = class_obj.Stock.STOCK_INVENTORY_ADJUSTMENT_ID
        header = [
            'fecha',
            'status',
            'warehouse_location:_warehouse_name',
            'warehouse_location:_location',
            'actual_inventory:_product:_product_code', 
            'actual_inventory:_product:_sku', 
            'actual_inventory:_numero_de_lote', 
            'actual_inventory:_adjust_status', 
            '',
            '',
            'actual_inventory:_actual_qty', 
            ]
        estatus = 'stock_actualizado'

    #print('header', header)
    records, pos_field_dict, files_dir, nueva_ruta, id_forma_seleccionada, dict_catalogs, group_records = class_obj.carga_doctos_headers(own_header=header,form_id_to_load=from_id)
    resp_cu = class_obj.carga_doctos_records(records, pos_field_dict, files_dir, nueva_ruta, id_forma_seleccionada, dict_catalogs, group_records )
    res = class_obj.update_status_record(estatus)

