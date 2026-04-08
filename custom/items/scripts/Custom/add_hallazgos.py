# -*- coding: utf-8 -*-
import sys, simplejson
from custom_utils import Custom
from bson import ObjectId
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.field_catalog_planta = '696133f0829d117f5e819e8c'
        self.field_planta = '696130ce57ba2b8308adef4d'
        self.field_area = '696133f1829d117f5e819e8d'
        self.field_hallazgos = '69614c871453042813b6d5aa'
        self.field_referencia = '69614cf3bae55c042e9659cd'
        self.field_observaciones = '69614cf3bae55c042e9659cf'
        self.field_reincidencia = '6967bd2bf429f9bdfc02b1c0'

    def get_last_record(self):
        # Se busca el último registro filtrando por Planta y Area para determinar si hubo reincidencia
        data_planta_area = self.answers.get(self.field_catalog_planta, {})
        
        match_last_record = {
            'form_id': self.form_id,
            'deleted_at': {'$exists': False},
            f'answers.{self.field_catalog_planta}.{self.field_planta}': data_planta_area.get(self.field_planta),
            f'answers.{self.field_catalog_planta}.{self.field_area}': data_planta_area.get(self.field_area)
        }

        if self.folio:
            match_last_record['folio'] = {'$nin': [self.folio]}
        
        last_record = self.cr.find_one(match_last_record, {'folio': 1, 'answers': 1}, sort=[("created_at", -1)])
        
        if not last_record:
            return {}

        print(f"... ultimo registro encontrado = {last_record['folio']}")
        return last_record.get('answers', {})

    def add_hallazgos(self):
        print('... ... Calculando grupo repetitivo Hallazgos ... ...')
        
        # Se obtienen los campos que son ponderables y su campo de comentarios
        fields_ponderables_by_page = self.get_fields_ponderables(with_comentarios_field=True)
        # print(fields_ponderables_by_page)

        # Se consulta el último registro para ver si hay reincidencias
        answers_last_record = self.get_last_record()

        # Se empieza a armar el grupo repetitivo
        list_hallazgos = []
        for field_id_ponderable, data_ponderable in fields_ponderables_by_page.items():
            if self.answers.get(field_id_ponderable) == 'no_cumple':
                list_hallazgos.append({
                    self.field_referencia: data_ponderable['label_field'],
                    self.field_observaciones: self.answers.get( data_ponderable['field_comentarios'], '' ),
                    self.field_reincidencia: 'sí' if (answers_last_record.get(field_id_ponderable) == 'no_cumple') else 'no'
                })
        
        # print(list_hallazgos)

        if list_hallazgos:
            self.answers[self.field_hallazgos] = list_hallazgos
            sys.stdout.write(simplejson.dumps({
                'status': 101,
                'replace_ans': self.answers
            }))

if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    answers = lkf_obj.answers

    lkf_obj.add_hallazgos()