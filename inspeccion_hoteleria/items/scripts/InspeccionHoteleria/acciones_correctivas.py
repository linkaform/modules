# -*- coding: utf-8 -*-
from ast import mod
import enum
import json
from pickle import NONE
import re
import sys, simplejson
import datetime
import unicodedata
from copy import deepcopy
from collections import Counter

from inspeccion_hoteleria_utils import Inspeccion_Hoteleria

from account_settings import *
from bson import ObjectId
import datetime

class Inspeccion_Hoteleria(Inspeccion_Hoteleria):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)
        self.cr_inspeccion = self.net.get_collections(collection='inspeccion_hoteleria')
        self.fallas_dict = {}
        
        # forms
        self.ACCIONES_CORRECTIVAS_FORM = self.lkm.form_id('acciones_correctivas', 'id')
        # catalogs
        self.ACCIONES_CORRECTIVAS_CAT = self.lkm.catalog_id('acciones_correctivas')
        self.ACCIONES_CORRECTIVAS_CAT_ID = self.ACCIONES_CORRECTIVAS_CAT.get('id')
        self.ACCIONES_CORRECTIVAS_CAT_OBJ_ID = self.ACCIONES_CORRECTIVAS_CAT.get('obj_id')
        
        self.f.update({
            'desviacion': '68e6e30b5224da81e53a7156',
            'question_ref': '68e80711c6c1f1375e4b8b51',
            'accion_correctiva_comentario': '68e6e392aacdc172c9fb6533',
            'accion_correctiva_foto': '68e6e392aacdc172c9fb6534'
        })
        
        self.labels_to_exclude = [
            'Nombre de la Camarista',
            'Habitación Remodelada',
            'Fotografía',
            'Comentarios',
            '¿La habitación es suite?',
            'Comentarios Generales',
            'STATUS AUDITORIA',
            'DIAS EN PROCESO',
            'status_auditoria',
            'dias_en_proceso'
        ]
        
        self.ids_to_exclude = [
            '67f81a13a48f0a56cf242558',
            '680bb736c2009c72987ba955',
            '680bb736c2009c72987ba957',
            '680bb736c2009c72987ba959',
            '680bb736c2009c72987ba95b',
            '680bb736c2009c72987ba95d',
            '680bb736c2009c72987ba95f',
            '680bb736c2009c72987ba961',
            '680bb736c2009c72987ba963',
            '680bb736c2009c72987ba965',
            '680bb736c2009c72987ba967'
        ]
        
        self.form_ids = {
            'hi_parque_fundidora': self.HI_PARQUE_FUNDIDORA,
            'crowne_plaza_torreon': self.CROWNE_PLAZA_TORREN,
            'travo': self.TRAVO,
            'hie_tecnologico': self.HIE_TECNOLGICO,
            'wyndham_garden_mcallen': self.WYNDHAM_GARDEN_MCALLEN,
            'istay_victoria': self.ISTAY_VICTORIA,
            'hie_torreon': self.HIE_TORREN,
            'hilton_garden_silao': self.HILTON_GARDEN_SILAO,
            'ms_milenium': self.MS_MILENIUM,
            'istay_monterrey_historico': self.ISTAY_MONTERREY_HISTRICO,
            'hie_guanajuato': self.HIE_GUANAJUATO,
            'hie_silao': self.HIE_SILAO,
            'istay_ciudad_juarez': self.ISTAY_CIUDAD_JUREZ,
            'crowne_plaza_mty': self.CROWNE_PLAZA_MONTERREY,
            'hie_galerias': self.HIE_GALERIAS,
            'holiday_inn_tijuana': self.HOLIDAY_INN_TIJUANA,
        }
        
    def normaliza_texto(self, texto):
        if not isinstance(texto, str):
            return ""
        texto = texto.lower()
        texto = texto.replace(" ", "_")
        texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
        return texto
        
    def get_labels(self):
        form_data = self.lkf_api.get_form_for_answer(self.REVISON_HABITACION)
        fields = form_data[0].get('fields', [])
        for field in fields:
            if not field.get('catalog'):
                if not field.get('label') in self.labels_to_exclude and not field.get('field_id') in self.ids_to_exclude:
                    self.fallas_dict.update({
                        field.get('field_id'): field.get('label')
                    })
                    
    def get_acciones_correctivas(self, hotel, hab_num, question=None, limit=10000):
        selector = {
            f"answers.{self.Location.f['location']}": hotel,
            f"answers.{self.Location.f['area']}": hab_num,
        }
        
        if question:
            selector.update({
                f"answers.{self.f['desviacion']}": question
            })
        
        fields = ["_id", f"answers.{self.f['question_ref']}"]

        mango_query = {
            "selector": selector,
            "fields": fields,
            "limit": limit,
        }

        row_catalog = self.lkf_api.search_catalog(141320, mango_query)
        format_row_catalog = {}
        if row_catalog and limit > 1:
            format_row_catalog = [i.get(self.f['question_ref']) for i in row_catalog]
        elif row_catalog:
            format_row_catalog = row_catalog[0]
        return format_row_catalog
    
    def create_acciones_correctivas_in_catalog(self, data):
        self.get_labels()
        location_data = data.pop(self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {})
        hotel_name = location_data.get(self.Location.f['location'])
        hab_num = location_data.get(self.Location.f['area'])

        questions_with_no = {key: value for key, value in data.items() if value == 'no' and key in self.fallas_dict}
        actual_acciones_correctivas = self.get_acciones_correctivas(hotel_name, hab_num)
        if actual_acciones_correctivas:
            filtered_questions_with_no = {key: value for key, value in questions_with_no.items() if key not in actual_acciones_correctivas}
            questions_with_no = filtered_questions_with_no

        # print('====log: hotel_name', hotel_name)
        # print('====log: hab_num', hab_num)
        # print('====log: questions_with_no', questions_with_no)
        list_acciones_correctivas = []
        for key, value in questions_with_no.items():
            metadata = self.lkf_api.get_catalog_metadata(141320) #TODO: Modularizar id de catalogo de accioens correctivas
            answers = {
                self.Location.f['location']: hotel_name,
                self.Location.f['area']: hab_num,
                self.f['desviacion']: self.fallas_dict.get(key),
                self.f['question_ref']: key
            }
            metadata['answers'] = answers
            list_acciones_correctivas.append(metadata)
        if list_acciones_correctivas:
            response = self.lkf_api.post_catalog_answers_list(list_acciones_correctivas)
            if response:
                match_error = False
                for item in response:
                    if not item.get('status_code') in [200, 201, 202]:
                        match_error = True
                        print('====log: Error en la creación de la acción correctiva:', item)
                if not match_error:
                    return {'success': True, 'message': 'Acciones correctivas creadas correctamente'}
        else:
            return {'success': True, 'message': 'No hay acciones correctivas que crear'}
        
    def complete_accion_correctiva(self, data):
        # doc = self.cr_inspeccion.find_one({"_id": ObjectId('6866d0da564385449a772fbf')})
        # doc.pop('_id', None)
        # print('====log: doc', simplejson.dumps(doc, indent=4))
        accion_correctiva_cat = data.pop('68e6e2ed68928c80a3297485', {}) #TODO: Modularizar objid de acciones correctivas catalog
        hotel_name = accion_correctiva_cat.get(self.Location.f['location'])
        hab_num = accion_correctiva_cat.get(self.Location.f['area'])
        question = accion_correctiva_cat.get(self.f['desviacion'])
        accion_correctiva = self.get_acciones_correctivas(hotel_name, hab_num, question, limit=1)
        accion_correctiva_id = accion_correctiva.get(self.f['question_ref'], '') if isinstance(accion_correctiva, dict) else ''
        search_hotel = self.form_ids.get(hotel_name.replace(' ', '_').lower(), '')
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": search_hotel,
                f"answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": hotel_name,
                f"answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.Location.f['area']}": hab_num,
                f"answers.{accion_correctiva_id}": "no",
            }},
            {"$sort": {"created_at": -1}},
            {"$limit": 1},
            {"$project": {
                "_id": 1,
            }}
        ]
        resp = self.format_cr(self.cr.aggregate(query))
        inspeccion_id = self.unlist(resp if len(resp) > 0 else {}).get('_id', '')
        format_resp = self.update_record_in_inspeccion_hoteleria(record_id=inspeccion_id, falla_id=accion_correctiva_id, falla=accion_correctiva, data=data) if inspeccion_id else {"success": False, "message": "No se encontró la inspección correspondiente"}
        return format_resp

    def update_record_in_inspeccion_hoteleria(self, record_id, falla_id, falla, data):
        doc = self.cr_inspeccion.find_one({"_id": ObjectId(record_id)})
        if not doc:
            return {"success": False, "message": "Documento no encontrado"}

        field_label = doc.get('field_label', {})
        acciones_correctivas = doc.get('acciones_correctivas', {})
        media = doc.get('media', {})
        media_acciones_correctivas = doc.get('media_acciones_correctivas', {})
        comments = doc.get('comments', {})
        comments_acciones_correctivas = doc.get('comments_acciones_correctivas', {})

        if falla_id in field_label:
            acciones_correctivas[falla_id] = field_label.pop(falla_id)
            media_acciones_correctivas.setdefault(falla_id, {})['before'] = media.pop(falla_id, [])
            media_acciones_correctivas.setdefault(falla_id, {})['after'] = data.get(self.f['accion_correctiva_foto'], [])
            comments_acciones_correctivas.setdefault(falla_id, {})['before'] = comments.pop(falla_id, "")
            comments_acciones_correctivas.setdefault(falla_id, {})['after'] = data.get(self.f['accion_correctiva_comentario'], "")

            update_data = {
                "field_label": field_label,
                "field_label_acciones_correctivas": acciones_correctivas,
                "media": media,
                "media_acciones_correctivas": media_acciones_correctivas,
                "comments": comments,
                "comments_acciones_correctivas": comments_acciones_correctivas,
            }
            self.cr_inspeccion.update_one(
                {"_id": ObjectId(record_id)},
                {"$set": update_data}
            )
            self.delete_accion_correctiva_in_catalog(falla=falla)
            return {"success": True, "message": "Accion correctiva registrada correctamente"}
        else:
            return {"success": False, "message": "Accion correctiva ya registrada o no encontrada"}
       
    def delete_accion_correctiva_in_catalog(self, falla):
        res = self.lkf_api.delete_catalog_record(141320, falla.get('_id'), falla.get('_rev')) #TODO: Modularizar id
        print('====log: res delete_accion_correctiva_in_catalog', res)
       
if __name__ == '__main__':
    module_obj = Inspeccion_Hoteleria(settings, sys_argv=sys.argv, use_api=False)
    module_obj.console_run()
    data_raw = json.loads(sys.argv[2])
    option = data_raw.get('option', '')

    if option == 'create_acciones_correctivas':
        response = module_obj.create_acciones_correctivas_in_catalog(module_obj.answers)
    elif option == 'complete_accion_correctiva':
        response = module_obj.complete_accion_correctiva(module_obj.answers)

    module_obj.HttpResponse({"data": response})