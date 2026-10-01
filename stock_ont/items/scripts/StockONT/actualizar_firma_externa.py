# -*- coding: utf-8 -*-
import sys, simplejson
from stock_ont_utils import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.data = self.data.get('data')

    def patch_firma_externa(self, record_id, answers):
        """
        Actualiza el registro existente en la forma de Firmas de Usuarios
        Externos (form_id FORM_FIRMAS_EXTERNAS) con las respuestas ya armadas.

        Args:
            record_id: _id del registro a actualizar.
            answers (dict): respuestas {field_id: valor} a guardar.

        Returns:
            dict: respuesta de `lkf_api.patch_record`.
        """
        metadata = self.lkf_api.get_metadata(self.FORM_FIRMAS_EXTERNAS, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Firmas Externas",
                    "action": "Actualizar Firma Externa",
                    "script": "actualizar_firma_externa.py",
                    "module": "stock_ont",
                    "function": "actualizar_firma_externa",
                }
            },
            'answers': answers,
        })
        return self.lkf_api.patch_record(metadata, record_id=record_id, jwt_settings_key='APIKEY_JWT_KEY')

    def actualizar_firma_externa(self):
        """
        Recibe en self.data el `folio` del registro padre, el `operationType`
        ('transferencia' o 'salida'), la `signature` ({file_name, file_url}),
        `capturedAt` (fecha de la firma) y `signedBy` (nombre de quien firma),
        y guarda la firma en el registro de Firmas de Usuarios Externos que
        siga pendiente, marcandolo como firmado.
        """
        folio = self.data.get('folio')
        tipo_operacion = self.data.get('operationType')
        signature_data = self.data.get('signature') or {}

        if not folio or not tipo_operacion:
            self.LKFException({'title': 'Datos incompletos', 'msg': "No se recibio el folio o el tipo de operacion de la firma externa a actualizar", 'status_code': 400})
        if not signature_data.get('file_url'):
            self.LKFException({'title': 'Datos incompletos', 'msg': "No se recibio la imagen de la firma externa", 'status_code': 400})

        record = self.get_record_firma_externa(folio, tipo_operacion)
        if not record:
            self.LKFException({'title': 'Registro no encontrado', 'msg': f"No se encontro ninguna firma externa de {tipo_operacion} con folio '{folio}'", 'status_code': 404})
        if self.unlist(record.get('answers', {}).get(self.f['field_public_estatus'])) == 'firmado':
            self.LKFException({'title': 'Firma registrada', 'msg': f"La firma externa de {tipo_operacion} con folio '{folio}' ya esta firmada", 'status_code': 400})

        answers = record.get('answers', {})
        answers.update({
            self.f['field_public_firma']: {
                'file_name': signature_data.get('file_name'),
                'file_url': signature_data.get('file_url'),
            },
            self.f['field_public_fecha_firma']: self.format_fecha_evento(self.data.get('capturedAt')),
            self.f['field_public_nombre_firma']: self.data.get('signedBy'),
            self.f['field_public_estatus']: 'firmado',
        })
        return self.patch_firma_externa(record['_id'], answers)

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    try:
        resp_firma_externa = stock_obj.actualizar_firma_externa()
    except Exception as e:
        # LKFException lanza el error como JSON en el mensaje de la excepcion;
        # se regresa por stdout para que lo reciba quien consume el script por API.
        try:
            resp_firma_externa = simplejson.loads(str(e))
        except ValueError:
            resp_firma_externa = {'exception': {'msg': [str(e)]}}
        resp_firma_externa['status_code'] = resp_firma_externa.get('exception', {}).get('status_code') or 400
    stock_obj.HttpResponse({"data": resp_firma_externa})
