# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos


class Accesos(Accesos):
    pass

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()

    # ── Datos de entrada ──────────────────────────────────────
    data   = acceso_obj.data.get('data', {})
    form_id   = acceso_obj.data.get('form_id')
    option = data.get('option', '')
    nombre = data.get('nombre', data.get('name'))

    # image_source: URL remota o ruta local de la imagen
    # Ejemplos:
    #   "https://f001.backblazeb2.com/file/app-linkaform/.../ine.jpg"
    #   "/tmp/identificacion.png"
    image_source = data.get('image_source', '')
    # form_id destino donde se creará el registro (opcional)
    # Si no se manda, solo extrae y retorna el JSON sin crear registro

    # Campos extra a extraer en modo ocr genérico (opcional)
    # Ejemplo: ["numero_factura", "total", "fecha", "rfc_emisor"]
    fields = data.get('fields', [])

    # Instrucciones adicionales al modelo (opcional)
    extra_instructions = data.get('extra_instructions', '')

    # Modelo de OpenRouter a usar (opcional, usa el default del config)

    # ── Router de opciones ────────────────────────────────────
    print('option=', option)
    if not acceso_obj.ai:
        # El usuario no configuró OPENROUTER_API_KEY en account_settings.py
        response = {
            'status_code': 400,
            'msg': 'OpenRouter no está configurado. Agrega OPENROUTER_API_KEY en account_settings.py'
        }

    elif not image_source:
        response = {
            'status_code': 400,
            'msg': 'Se requiere image_source en data'
        }

    elif option == 'ocr_id':
        # Extrae datos de una identificación (INE, pasaporte, licencia)
        # Retorna JSON con los campos del documento
        response = acceso_obj.ocr_identificacion(
            image_source=image_source,
            form_id=form_id,
            name=nombre,
        )

    elif option == 'ocr_doc':
        # OCR genérico — extrae campos específicos de cualquier imagen
        response = acceso_obj.ocr_documento(
            image_source=image_source,
            fields=fields,
            extra_instructions=extra_instructions,
            form_id=form_id,
        )
    elif option == 'ocr_paquete':
        # OCR genérico — extrae campos específicos de cualquier imagen
        response = acceso_obj.ocr_paquete(
            image_source=image_source,
            fields=fields,
            extra_instructions=extra_instructions,
        )
    elif option == 'ocr_batch':
        # Procesa una lista de imágenes en batch
        # image_source puede ser lista de URLs o ruta a archivo .txt
        images = data.get('images', [])
        if not images and image_source:
            # Si mandaron un solo image_source, lo ponemos en lista
            images = [image_source]
        response = acceso_obj.ocr_batch(
            images=images,
            option_type=data.get('ocr_type', 'ocr_id'),
            form_id=form_id,
            model=model or None,
        )

    else:
        response = {'msg': 'Empty', 'valid_options': ['ocr_id', 'ocr_doc', 'ocr_batch']}

    acceso_obj.HttpResponse({'data': response})