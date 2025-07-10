"""
Script para crear multiples balanceo de inventario.
"""
# -*- coding: utf-8 -*-
import sys, simplejson

from jit_utils import JIT

from account_settings import *

class JIT(JIT):

    def __init__(self, settings, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)

        self.SUGERIDO_DE_COMPRAS = self.lkm.form_id('sugerido_de_compras','id')
        
    def create_multiple_inventory_balance(self, data):
        families_list = data.get(self.f['families_list'], [])
        if data.get(self.f['borrar_historial']) == 'si':
            print('borrando historial')
            self.borrar_historial()
            borrar_historial = 'no'#data.get(self.f['borrar_historial'], 'no')
        answers = {}
        list_response = []
        for family in families_list:
            ans_familia = family.upper().replace('_',' ')
            answers.update({
                self.Product.PRODUCT_OBJ_ID: {
                    self.Product.f['product_type']: ans_familia
                },
                self.f['estatus_balanceo']: 'cargar_documentos',
                self.f['borrar_historial']: borrar_historial
            })
            response = self.create_register(
                module='JIT',
                process='Creacion de Balance de Inventario',
                action='create_inventory_balance',
                file='jit/app.py',
                form_id=self.BALANCEO_DE_INVENTARIOS,
                answers=answers
            )
            list_response.append({
                'family': family,
                'response': response
            })
        return list_response
    
    def create_multiple_sugerido_compra(self, data):
        families_list = data.get(self.f['families_list'], [])
        if data.get(self.f['borrar_historial']) == 'si':
            print('borrando historial')
            self.borrar_historial()
            borrar_historial = 'no'#data.get(self.f['borrar_historial'], 'no')
        answers = {}
        list_response = []
        for family in families_list:
            ans_familia = family.upper().replace('_',' ')
            answers.update({
                self.Product.PRODUCT_OBJ_ID: {
                    self.Product.f['product_type']: ans_familia
                },
                self.f['estatus_balanceo']: 'cargar_documentos',
                self.f['borrar_historial']: borrar_historial
            })
            response = self.create_register(
                module='JIT',
                process='Creacion de Sugerido de Compras',
                action='create_mult_inventory_balance',
                file='jit/app.py',
                form_id=self.SUGERIDO_DE_COMPRAS,
                answers=answers
            )
            list_response.append({
                'family': family,
                'response': response
            })
        return list_response
        
        
    def create_register(self, module, process, action, file, form_id, answers):
        """Crea un registro en Linkaform con los metadatos y respuestas proporcionadas.

        Args:
            module (str): El nombre del módulo que está ejecutando la acción.
            process (str): El nombre del proceso que se está ejecutando.
            action (str): El nombre del script que se está ejecutando.
            file (str): La ruta del archivo donde se encuentra el app del modulo utilizado(Ej. jit/app.py).
            form_id (str): El ID de la forma en Linkaform.
            answers (dict): El diccionario de respuestas ya formateado.
            
        Returns:
            response: La respuesta de la API de Linkaform al crear el registro.
        """
        metadata = self.lkf_api.get_metadata(form_id=form_id)
        
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": module,
                    "Process": process,
                    "Action": action,
                    "File": file
                }
            },
        })
        
        metadata.update({'answers':answers})
        response = self.lkf_api.post_forms_answers(metadata)
        return response

if __name__ == '__main__':
    class_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    class_obj.console_run()
    option = class_obj.data.get('option', 'transfer')
    
    if option == 'buy':
        response = class_obj.create_multiple_sugerido_compra(data=class_obj.answers)
    elif option == 'transfer':
        response = class_obj.create_multiple_inventory_balance(data=class_obj.answers)

    sys.stdout.write(simplejson.dumps({
        'response': response,
    }))