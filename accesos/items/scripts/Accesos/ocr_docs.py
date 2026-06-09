# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos


class Accesos(Accesos):
    pass

    def ocr_vehiculo(self, image_source, fields: dict = {},
                     extra_instructions: str = None,
                     model: str = 'google/gemini-2.5-flash-lite') -> dict:
        """
        Extrae los datos de una foto de un vehículo:
        tipo, marca, modelo, año estimado, color, placas,
        número económico, condición y observaciones.

        Args:
            image_source:        URL remota, ruta local, o lista de imágenes del vehículo.
            fields:              Campos adicionales a extraer (opcional).
            extra_instructions:  Instrucciones extra al modelo (opcional).
            model:               Modelo OpenRouter a usar.
                                 Opciones recomendadas:
                                   'google/gemini-2.5-flash-lite'   ← default, rápido y barato
                                   'google/gemini-2.5-flash'        ← mejor OCR, más caro
                                   'anthropic/claude-haiku-4-5'     ← excelente para placas

        Returns:
            dict con:
                - status_code : 200 OK / 206 advertencias / 400 config / 500 error
                - data        : campos extraídos
                - msg         : mensaje de resultado

        Ejemplo de uso:
            response = acceso_obj.ocr_vehiculo(
                image_source="https://s3.../auto.jpg",
            )
            # o varias fotos del mismo vehículo:
            response = acceso_obj.ocr_vehiculo(
                image_source=[
                    "https://s3.../frente.jpg",
                    "https://s3.../lateral.jpg",
                    "https://s3.../placa.jpg",
                ],
            )
        """
        if not self.ai:
            return {'status_code': 400, 'msg': 'OpenRouter no configurado'}

        system = (
            "You are a vehicle identification specialist with expertise in "
            "reading license plates, identifying car makes and models, and "
            "assessing vehicle condition from photographs. "
            "You analyze images of cars, trucks, motorcycles, and commercial vehicles. "
            "Always respond with a single valid JSON object and nothing else — "
            "no markdown, no backticks, no explanation, no preamble."
        )

        prompt = (
            "Analyze all provided vehicle images as a single combined inspection. "
            "Images may show: front, sides, rear, license plate close-ups, or interior. "
            "All inputs refer to ONE vehicle. Extract every available field. "
            "If a field cannot be determined from the provided material, use null. "
            "\n\n"
            "Return ONLY a JSON object with this exact structure:\n"
            "{\n"
            '  "tipo_vehiculo": "string — sedan, SUV, pickup, camion, motocicleta, van, autobus, trailer, etc.",\n'
            '  "marca": "string — vehicle brand (Toyota, Ford, Nissan, Chevrolet, Honda, Kia, etc.)",\n'
            '  "modelo": "string — vehicle model name (Corolla, F-150, Sentra, Aveo, etc.)",\n'
            '  "anio_estimado": "string — model year or range if inferable (e.g. 2019 or 2018-2020)",\n'
            '  "color_principal": "string — main body color in Spanish (rojo, blanco, gris, negro, etc.)",\n'
            '  "color_secundario": "string — secondary color or trim if applicable, else null",\n'
            '  "placa": "string — license plate number exactly as visible, preserving spacing/hyphens",\n'
            '  "estado_placa": "string — Mexican state or country of the plate if identifiable",\n'
            '  "no_economico": "string — fleet or unit number painted on the vehicle if visible, else null",\n'
            '  "num_serie_vin": "string — VIN or chassis number if visible (e.g. on windshield sticker), else null",\n'
            '  "condicion": "string — bueno / regular / malo — overall visible condition of the vehicle",\n'
            '  "danios_visibles": "string — describe any dents, scratches, broken parts, or damage, else null",\n'
            '  "observaciones": "string — any notable features, modifications, stickers, cargo, or distinguishing marks",\n'
            '  "confianza": "string — alto / medio / bajo — overall confidence based on image clarity and angle"\n'
            "}"
        )

        if extra_instructions:
            prompt += f"\n\nAdditional instructions: {extra_instructions}"
        # 1. Sanitizar image_source — asegurar que sea lista de strings
        if isinstance(image_source, str):
            image_source = [image_source]
        elif isinstance(image_source, list):
            image_source = [
                img['file_url'] if isinstance(img, dict) else img
                for img in image_source
            ]

        # 1. Llamar al LLM
        raw_text = self.ai.ocr_general(image_source, system, prompt, model=model, max_tokens=1000)

        # 2. Extraer el contenido de texto
        datos = {}
        if raw_text.get('choices'):
            choices = raw_text['choices']
            if isinstance(choices, list) and len(choices) > 0:
                content = choices[0].get('message', {}).get('content')
                if content:
                    datos = content

        print('ocr_vehiculo datos=', datos)

        # 3. Normalizar (limpia markdown fences, parsea JSON, etc.)
        datos = self._ocr_normalizar(datos)

        # 4. Validar campos básicos
        errores = self._ocr_validar_id(datos)
        if errores:
            return {
                'status_code': 206,
                'msg': 'Extracción con advertencias',
                'data': datos,
                'warnings': errores,
            }

        return {'status_code': datos.get('status_code', 200), 'msg': 'OK', 'data': datos}
        
    def ocr_truck(self, image_source: list, fields: dict = {},
                           extra_instructions: str = None,
                           model: str = 'google/gemini-2.5-flash-lite') -> dict:
        """
        Extrae los datos de una foto de un paquete para identificar, 
        Proveedor (paqueteria), Remitente, Destinatario.
        Si encuentra un telefono, intenta enviar un sms o whatsapp.
        Si ecuentra un correo intenta enviar un correo.
        Podemos ver si le pudiera marcar y platicado decirle llego tu 
        paquete de MercadoLibre. O llego tu comida.

        Args:
            image_source: URL remota o ruta local de la imagen.
            model:        Modelo OpenRouter a usar (opcional).
            MODEL = "anthropic/claude-haiku-4.5"  # excelente OCR, precio razonable
            MODEL = "google/gemini-2.5-flash"  # un escalón arriba, más caro pero mejor

        Returns:
            dict con:
                - status_code: 200/201/400/500
                - data: campos extraídos por el OCR
                - msg: mensaje de resultado

        Ejemplo de uso en script:
            response = acceso_obj.ocr_paquete(
                image_source="https://s3.../ine.jpg",
            )
        """
        system = (
            "You are a certified security guard and heavy transport specialist at a manufacturing plant. "
            "Your role is to process inbound and outbound truck check-ins following CTPAT compliance standards. "
            "You specialize in identifying all types of commercial vehicles, reading transport documents, "
            "driver IDs, bills of lading, and cargo manifests. "
            "Always respond with a single valid JSON object and nothing else — no markdown, no explanation, no preamble."
        )

        prompt = (
            "Analyze all provided images and/or PDF documents as a single combined inspection. "
            "Images may include: truck exterior (front, sides, rear, undercarriage), driver ID/license, "
            "cargo documents, invoices, manifests, or trailer/container photos. "
            "All inputs refer to ONE transport event. Extract every available field. "
            "If a field cannot be determined from the provided material, use null. "
            "For boolean inspection fields: true = no findings (OK), false = findings detected, null = not visible/not applicable. "
            "\n\n"
            "Return ONLY a JSON object with this exact structure:\n"
            "{\n"

            # ── TAB 1: VEHÍCULO ──────────────────────────────────────────
            '  "vehiculo": {\n'
            '    "transportista": "string — carrier company name",\n'
            '    "tipo_accion": "string — Entrega or Recoleccion",\n'
            '    "procedencia": "string — origin state/city",\n'
            '    "tipo_vehiculo": "string — torton, doble remolque, plataforma, caja seca, caja refrigerada, volteo, pipa, low-boy, dolly, etc.",\n'
            '    "marca": "string — truck brand (Kenworth, Freightliner, International, Volvo, etc.)",\n'
            '    "modelo": "string — truck model (T680, Cascadia, etc.)",\n'
            '    "anio": "string — model year if visible",\n'
            '    "color": "string — truck cab color",\n'
            '    "placa_vehiculo": "string — tractor/cab license plate",\n'
            '    "no_economico": "string — carrier-assigned unit number (numero economico / rotulo)",\n'
            '    "material": "string — cargo description",\n'
            '    "conductor": "string — driver full name",\n'
            '    "no_licencia": "string — driver license number"\n'
            '  },\n'

            # ── TAB 2: REMOLQUES / CONTENEDORES ──────────────────────────
            '  "remolques": [\n'
            '    {\n'
            '      "tipo_remolque": "string — caja seca, caja refrigerada, plataforma, contenedor, tanque, etc.",\n'
            '      "no_sello": "string — seal number",\n'
            '      "no_caja_contenedor": "string — box/container unit number",\n'
            '      "placas_caja": "string — trailer license plate",\n'
            '      "comentarios": "string — any comments about this trailer"\n'
            '    }\n'
            '  ],\n'

            # ── TAB 3A: INSPECCIÓN 17 PUNTOS (TRACTOR) ───────────────────
            '  "inspeccion_17_puntos": {\n'
            '    "1_defensa": true,\n'
            '    "2_motor_bateria_filtros": true,\n'
            '    "3_llantas_rines": true,\n'
            '    "4_piso_tractor": true,\n'
            '    "5_tanque_combustible": true,\n'
            '    "6_cabina_dormitorio_puertas_herramientas": true,\n'
            '    "7_tanque_aire": true,\n'
            '    "8_ejes_transmision": true,\n'
            '    "9_quinta_rueda": true,\n'
            '    "10_chasis": true,\n'
            '    "11_puertas_externa": true,\n'
            '    "12_piso_externo_trailer": true,\n'
            '    "13_paredes_externas": true,\n'
            '    "14_pared_frontal_externa": true,\n'
            '    "15_techo_externo": true,\n'
            '    "16_unidad_refrigeracion": true,\n'
            '    "17_escape_mofles": true\n'
            '  },\n'
            "  // Note: inspection booleans — true = OK/no findings, false = issue detected, null = not visible\n"

            # ── TAB 3B: INSPECCIÓN 7 PUNTOS CONTENEDOR ───────────────────
            '  "inspeccion_contenedor": {\n'
            '    "altura_interior": "string — e.g. 2.5m",\n'
            '    "ancho_interior": "string — e.g. 2.4m",\n'
            '    "longitud_interior": "string — e.g. 16.1m",\n'
            '    "puntos": {\n'
            '      "1_exterior_parte_inferior": {"suciedad": null, "plagas": null, "fauna": null},\n'
            '      "2_puertas_interiores_exteriores": {"suciedad": null, "plagas": null, "fauna": null},\n'
            '      "3_pared_interior_derecha": {"suciedad": null, "plagas": null, "fauna": null},\n'
            '      "4_pared_interior_izquierda": {"suciedad": null, "plagas": null, "fauna": null},\n'
            '      "5_pared_interior_frontal": {"suciedad": null, "plagas": null, "fauna": null},\n'
            '      "6_techo_cubierta_superior": {"suciedad": null, "plagas": null, "fauna": null},\n'
            '      "7_piso_interior": {"suciedad": null, "plagas": null, "fauna": null}\n'
            '    }\n'
            '  },\n'

            # ── METADATA ─────────────────────────────────────────────────
            '  "observaciones_generales": "string — CTPAT flags, anomalies, damage, or anything unusual",\n'
            '  "confianza": "string — high / medium / low — your confidence in the extracted data based on image quality"\n'
            "}"
        )
        if not self.ai:
            return {'status_code': 400, 'msg': 'OpenRouter no configurado'}

        # 1. Extraer datos con el LLM
        # try:
        if True:
            raw_text = self.ai.ocr_general(image_source, system, prompt, model=model, max_tokens=2000)
        # except ValueError as e:
        #     return {'status_code': 500, 'msg': f'Error OCR: {e}'}
        # except Exception as e:
        #     return {'status_code': 500, 'msg': f'Error inesperado: {e}'}

        # 2. Normalizar — esto es código, no LLM
        datos = {}
        if raw_text.get('choices'):
            if isinstance(raw_text['choices'], list) and len(raw_text['choices']) >0:
                if raw_text['choices'][0].get('message',{}).get('content'):
                    datos = raw_text['choices'][0]['message']['content']
        print('datos=', datos)

        datos = self._ocr_normalizar(datos)

        # 3. Validar
        errores = self._ocr_validar_id(datos)
        if errores:
            return {
                'status_code': 206,  # partial content — extrajo pero hay campos inválidos
                'msg': 'Extracción con advertencias',
                'data': datos,
                'warnings': errores,
            }
        return {'status_code': datos.get('status_code', 200), 'msg': 'OK', 'data': datos}


if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()

    # ── Datos de entrada ──────────────────────────────────────
    print('acceso_obj.data=',acceso_obj.data)
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
    print('data=', data)
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
        print('data---', data)
        response = {
            'status_code': 400,
            'msg': 'Se requiere image_source en data'
        }
        acceso_obj.LKFException(response)

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
    elif option == 'ocr_articulo_perdido':
        # OCR genérico — extrae campos específicos de cualquier imagen
        response = acceso_obj.ocr_articulo_perdido(
            image_source=image_source
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
    elif option == 'ocr_truck':
        # Procesa una lista de imágenes en batch
        # image_source puede ser lista de URLs o ruta a archivo .txt
        images = data.get('images', [])
        if not images and image_source:
            # Si mandaron un solo image_source, lo ponemos en lista
            images = [image_source]
        response = acceso_obj.ocr_truck(
            image_source=image_source,
            fields=fields,
            extra_instructions=extra_instructions,
        )
    elif option == 'ocr_vehiculo':
        response = acceso_obj.ocr_vehiculo(
            image_source=image_source,
            fields=fields,
            extra_instructions=extra_instructions,
        )
    else:
        response = {'msg': 'Empty', 'valid_options': ['ocr_id', 'ocr_doc', 'ocr_batch','ocr_paquete','ocr_truck', 'ocr_vehiculo']}

    acceso_obj.HttpResponse({'data': response})