# coding: utf-8
import os, re, sys, simplejson
from urllib.parse import urlparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos


class Accesos(Accesos):
    pass

    def ocr_equipo(self, image_source,
                   extra_instructions: str = None,
                   model: str = 'google/gemini-2.5-flash-lite') -> dict:
        """
        Extrae los datos de una foto de un equipo/herramienta:
        tipo, marca, modelo, número de serie y color.

        Args:
            image_source: URL remota, ruta local, o lista de imágenes.
            model:        Modelo OpenRouter a usar.

        Returns:
            dict con:
                - status_code : 200 OK / 206 advertencias / 400 config / 500 error
                - data        : campos extraídos
                - msg         : mensaje de resultado
        """
        if not self.ai:
            return {'status_code': 400, 'msg': 'OpenRouter no configurado'}

        system = (
            "You are an asset identification specialist trained to analyze images "
            "of equipment, tools, computers, tablets, and electronic devices. "
            "You extract identifying information such as brand, model, serial number, "
            "and color from photographs. "
            "Always respond with a single valid JSON object and nothing else — "
            "no markdown, no backticks, no explanation, no preamble."
        )

        prompt = (
            "Analyze the provided image and extract all visible identifying information "
            "about the equipment or device shown. "
            "If a field cannot be determined from the image, use null. "
            "\n\n"
            "Return ONLY a JSON object with this exact structure:\n"
            "{\n"
            '  "tipo": "string — MUST be exactly one of: herramienta, computadora, equipo de limpieza, escalera, impresora, monitor, tablet, otro. No other values allowed.",\n'
            '  "marca": "string — brand name visible on the device (Apple, Dell, HP, Lenovo, Samsung, Makita, Dewalt, etc.), or null",\n'
            '  "modelo": "string — model name or number if visible (e.g. MacBook Pro, ThinkPad X1, iPad Pro, etc.), or null",\n'
            '  "num_serie": "string — serial number exactly as visible on label or sticker, or null",\n'
            '  "color": "string — MUST be exactly one of: Amarillo, Azul, Beige, Blanco, Cafe, Crema, Dorado, Gris, Morado, Naranja, Negro, Plateado, Rojo, Rosa, Verde, Violeta, Otro. Pick the closest match.",\n'
            '  "observaciones": "string — any notable features, damage, stickers, or distinguishing marks, or null",\n'
            '  "confianza": "string — alto / medio / bajo — overall confidence based on image clarity"\n'
            "}"
        )

        if extra_instructions:
            prompt += f"\n\nAdditional instructions: {extra_instructions}"

        # Sanitizar image_source
        if isinstance(image_source, str):
            image_source = [image_source]
        elif isinstance(image_source, list):
            image_source = [
                img['file_url'] if isinstance(img, dict) else img
                for img in image_source
            ]

        print('>>> ocr_equipo image_source=', image_source)

        raw_text = self.ai.ocr_general(image_source, system, prompt, model=model, max_tokens=1000)

        datos = {}
        if raw_text.get('choices'):
            choices = raw_text['choices']
            if isinstance(choices, list) and len(choices) > 0:
                content = choices[0].get('message', {}).get('content')
                if content:
                    datos = content

        print('ocr_equipo datos=', datos)

        datos = self._ocr_normalizar(datos)

        errores = self._ocr_validar_id(datos)
        if errores:
            return {
                'status_code': 206,
                'msg': 'Extracción con advertencias',
                'data': datos,
                'warnings': errores,
            }

        return {'status_code': datos.get('status_code', 200), 'msg': 'OK', 'data': datos}

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
            '  "tipo_vehiculo": "string — MUST be exactly one of: pick up, camión, bicicleta, remolque, moto, van, autobús, trailer, automóvil. No other values allowed.",\n'
            '  "marca": "string — vehicle brand (Toyota, Ford, Nissan, Chevrolet, Honda, Kia, etc.)",\n'
            '  "modelo": "string — vehicle model name (Corolla, F-150, Sentra, Aveo, etc.)",\n'
            '  "color_principal": "string — MUST be exactly one of: Amarillo, Azul, Beige, Blanco, Cafe, Crema, Dorado, Gris, Morado, Naranja, Negro, Plateado, Rojo, Rosa, Verde, Violeta, Otro. No other values allowed. Pick the closest match.",\n'
            '  "placa": "string — license plate number exactly as visible, preserving spacing/hyphens",\n'
            '  "estado_placa": "string — Mexican state or country of the plate if identifiable",\n'
            '  "num_serie_vin": "string — VIN or chassis number if visible (e.g. on windshield sticker), else null",\n'
            '  "condicion": "string — bueno / regular / malo — overall visible condition of the vehicle",\n'
            '  "danios_visibles": "string — describe any dents, scratches, broken parts, or damage, else null",\n'
            '  "observaciones": "string — small description, any notable features, modifications, stickers, cargo, or distinguishing marks",\n'
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
        print('>>> image_source sanitizado=', image_source)
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

    def ocr_articulo_concesionado(self, image_source,
                                   extra_instructions: str = None,
                                   model: str = 'google/gemini-2.5-flash-lite') -> dict:
        """
        Identifica un artículo concesionado a partir de su foto. Puede tratarse de:
          - Un artículo genérico identificable a simple vista (guantes, casco, chaleco,
            lentes de seguridad, herramienta, etc.), donde lo importante es reconocer
            QUÉ artículo es.
          - Un artículo con un llavero/etiqueta con un número o ID impreso (ej. "ID-360"
            o "360"), usado sobre todo en camiones/vehículos (ej. "ID-360 SPRINTER
            CORTA"). En ese caso el número tiene prioridad para la búsqueda.

        Con lo detectado busca el activo correspondiente en self.ACTIVOS_FIJOS
        (formulario "activos_fijos") y regresa su categoría, nombre de equipo y demás
        datos del registro encontrado.

        Args:
            image_source: URL remota, ruta local, o lista de imágenes del artículo.
            model:        Modelo OpenRouter a usar.

        Returns:
            dict con:
                - status_code : 200 OK / 206 advertencias (no se encontró match) / 400 config
                - data        : campos leídos de la foto + 'activo_fijo' con el registro
                                encontrado en activos fijos (o None si no hubo match)
                - msg         : mensaje de resultado
        """
        if not self.ai:
            return {'status_code': 400, 'msg': 'OpenRouter no configurado'}

        system = (
            "You are an asset identification specialist at an industrial plant, trained "
            "to recognize conceded/loaned articles (articulos concesionados). "
            "These can be generic items identifiable on sight — safety gloves, helmets, "
            "vests, goggles, harnesses, tools, etc. — or vehicles/equipment identified by "
            "a physical key tag or label with a printed number/ID (e.g. '360' or "
            "'ID-360 SPRINTER CORTA'). "
            "Your priority is to determine WHICH article this is: if a number or ID tag "
            "is visible, read it as accurately as possible; otherwise identify the article "
            "by what it visually is. "
            "Always respond with a single valid JSON object and nothing else — "
            "no markdown, no backticks, no explanation, no preamble."
        )

        prompt = (
            "Analyze the provided image and identify the conceded article shown. "
            "First check if there is a key tag or label with a printed number/ID visible "
            "(e.g. '360', 'ID-360', 'ID-360 SPRINTER CORTA') — if so, that identifier takes "
            "priority. If there is no number/ID visible, identify the article itself from "
            "what is visually shown (e.g. guantes, casco, chaleco de seguridad, lentes de "
            "seguridad, arnés, herramienta, camión, van, etc.). "
            "If a field cannot be determined from the image, use null. "
            "\n\n"
            "Return ONLY a JSON object with this exact structure:\n"
            "{\n"
            '  "numero_identificador": "string — number/ID read from a key tag or label, e.g. 360 or ID-360, or null",\n'
            '  "nombre_articulo": "string — full text read from the tag/label if any, e.g. ID-360 SPRINTER CORTA, or null",\n'
            '  "tipo_articulo": "string — what the article physically is, in Spanish (e.g. guantes, casco, chaleco de seguridad, lentes de seguridad, arnés, herramienta, camión, van, etc.). This should always be filled based on what is visible.",\n'
            '  "marca": "string — visible brand of the article/vehicle/equipment, or null",\n'
            '  "modelo": "string — visible model, or null",\n'
            '  "color": "string — main visible color, or null",\n'
            '  "observaciones": "string — any relevant detail, damage, or distinguishing marks, or null",\n'
            '  "confianza": "string — alto / medio / bajo — overall confidence based on image clarity"\n'
            "}"
        )

        if extra_instructions:
            prompt += f"\n\nAdditional instructions: {extra_instructions}"

        # Sanitizar image_source
        if isinstance(image_source, str):
            image_source = [image_source]
        elif isinstance(image_source, list):
            image_source = [
                img['file_url'] if isinstance(img, dict) else img
                for img in image_source
            ]

        print('>>> ocr_articulo_concesionado image_source=', image_source)

        raw_text = self.ai.ocr_general(image_source, system, prompt, model=model, max_tokens=1000)

        datos = {}
        if raw_text.get('choices'):
            choices = raw_text['choices']
            if isinstance(choices, list) and len(choices) > 0:
                content = choices[0].get('message', {}).get('content')
                if content:
                    datos = content

        print('ocr_articulo_concesionado datos=', datos)
        datos = self._ocr_normalizar(datos)

        activo = self._buscar_activo_fijo(
            numero=datos.get('numero_identificador'),
            nombre=datos.get('nombre_articulo'),
            tipo=datos.get('tipo_articulo'),
        )
        datos['activo_fijo'] = activo or None

        errores = self._ocr_validar_id(datos)
        if not activo:
            errores.append('No se encontró el artículo en activos fijos')

        if errores:
            return {
                'status_code': 206,
                'msg': 'Extracción con advertencias',
                'data': datos,
                'warnings': errores,
            }

        return {'status_code': 200, 'msg': 'OK', 'data': datos}

    def _buscar_activo_fijo(self, numero: str = None, nombre: str = None, tipo: str = None) -> dict:
        """
        Busca en self.ACTIVOS_FIJOS el activo que corresponde a lo detectado en la foto
        del artículo concesionado. Prioriza el match por número/ID (ej. "360" contra
        nombre_equipo "ID-360 SPRINTER CORTA"), y si no hay número usa el nombre leído en
        la etiqueta o, en su defecto, el tipo de artículo identificado visualmente
        (ej. "guantes", "casco") contra nombre_equipo, categoria y tipo_equipo.
        Regresa el registro con categoria, nombre_equipo, marca, modelo, tipo de
        equipo/vehiculo, numero de serie, placas y estatus.
        """
        import re

        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.ACTIVOS_FIJOS,
            }},
            {"$project": {
                "_id": 0,
                "folio": {"$ifNull": ["$folio", None]},
                "categoria": {"$ifNull": [f"$answers.{self.cons_f['categoria_equipo_concesion']}", None]},
                "nombre_equipo": {"$ifNull": [f"$answers.{self.f['nombre_equipo']}", None]},
                "marca": {"$ifNull": [f"$answers.{self.TIPO_DE_VEHICULO_OBJ_ID}.{self.mf['marca_vehiculo']}", None]},
                "modelo": {"$ifNull": [f"$answers.{self.TIPO_DE_VEHICULO_OBJ_ID}.{self.mf['modelo_vehiculo']}", None]},
                "tipo_vehiculo": {"$ifNull": [f"$answers.{self.TIPO_DE_VEHICULO_OBJ_ID}.{self.mf['tipo_vehiculo']}", None]},
                "tipo_equipo": {"$ifNull": [f"$answers.{self.f['tipo_equipo']}", None]},
                "numero_de_serie_chasis": {"$ifNull": [f"$answers.{self.f['numero_de_serie_chasis']}", None]},
                "placas": {"$ifNull": [f"$answers.{self.f['placas']}", None]},
                "estado": {"$ifNull": [f"$answers.{self.f['estado']}", None]},
                "estatus": {"$ifNull": [
                    f"$answers.{self.f['estatus_vehiculo']}",
                    f"$answers.{self.f['estatus']}",
                    None]},
            }},
        ]
        activos = self.format_cr(self.cr.aggregate(query))

        # 1. Match por número/ID — se le da prioridad (ej. llaveros de camiones/vehículos)
        digitos = re.sub(r'\D', '', numero) if numero else ''
        if digitos:
            for activo in activos:
                digitos_nombre = re.sub(r'\D', '', activo.get('nombre_equipo') or '')
                if digitos_nombre and digitos_nombre == digitos:
                    return activo

        # 2. Match por nombre/tipo de artículo leído o identificado visualmente
        nombres = [a['nombre_equipo'] for a in activos if a.get('nombre_equipo')]
        for texto in (nombre, tipo):
            if not texto:
                continue
            match = next((a for a in activos if a.get('nombre_equipo') == texto), None)
            if not match:
                mejor = self._match_label(texto, nombres, umbral=60)
                if mejor.get('label'):
                    match = next((a for a in activos if a.get('nombre_equipo') == mejor['label']), None)
            if not match:
                categorias = [a['categoria'] for a in activos if a.get('categoria')]
                mejor_cat = self._match_label(texto, categorias, umbral=60)
                if mejor_cat.get('label'):
                    match = next((a for a in activos if a.get('categoria') == mejor_cat['label']), None)
            if not match:
                tipos = [a['tipo_equipo'] for a in activos if a.get('tipo_equipo')]
                mejor_tipo = self._match_label(texto, tipos, umbral=60)
                if mejor_tipo.get('label'):
                    match = next((a for a in activos if a.get('tipo_equipo') == mejor_tipo['label']), None)
            if match:
                return match
        return {}

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

    # Campos del panel de la etiqueta del cartón (datos generales, no por unidad).
    _PACKING_LIST_HEADER = ('tipo_equipo', 'marca', 'modelo', 'codigo_carton', 'sku',
                            'item', 'order', 'qty', 'code', 'notes', 'confianza')

    def ocr_packing_list(self, image_source,
                          extra_instructions: str = None,
                          model: str = 'google/gemini-2.5-flash',
                          max_tokens: int = 8000) -> dict:
        """
        Procesa una o varias fotos de etiquetas de cartón (una URL o una lista de
        URLs). Cada imagen se procesa por separado con _ocr_packing_list_carton y
        sus NS se juntan en una sola lista 'serials'. Si una NS aparece en más de
        una imagen se deja una sola vez y se reporta en el nodo 'duplicados' con
        la observación correspondiente.

        Returns:
            dict con:
                - status_code : 200 OK / 206 advertencias / 400 config / 500 error
                - data        : datos generales (de la primera imagen con datos),
                                'serials' de todas las imágenes, 'cartones' con el
                                detalle por imagen y 'duplicados' (si los hay)
                - msg         : mensaje de resultado
        """
        if not self.ai:
            return {'status_code': 400, 'msg': 'OpenRouter no configurado'}

        if isinstance(image_source, str):
            image_source = [image_source]
        image_source = [
            img['file_url'] if isinstance(img, dict) else img
            for img in (image_source or [])
        ]
        image_source = [img for img in image_source if img]
        if not image_source:
            return {'status_code': 400, 'msg': 'Se requiere al menos una imagen'}

        datos = {campo: None for campo in self._PACKING_LIST_HEADER}
        serials, cartones, warnings, correcciones = [], [], [], []
        vistos = {}      # sn -> serial (primera aparición)
        duplicados = {}  # sn -> {'sn', 'imagenes'} (nombres de archivo)

        for idx, url in enumerate(image_source, start=1):
            try:
                res = self._ocr_packing_list_carton(
                    url, extra_instructions=extra_instructions,
                    model=model, max_tokens=max_tokens)
            except Exception as e:
                print('>>> ocr_packing_list error en imagen', idx, url, e)
                warnings.append(f'Imagen {idx}: error al procesar ({e})')
                continue

            data = res.get('data') or {}
            if not data:
                warnings.append(f"Imagen {idx}: {res.get('msg', 'sin datos')}")
                continue
            # Una foto que no es etiqueta puede regresar JSON válido pero sin unidades.
            if not data.get('serials'):
                warnings.append(f'Imagen {idx}: no se encontraron NS')
                continue
            warnings.extend(f'Imagen {idx}: {w}' for w in res.get('warnings') or [])
            correcciones.extend(f'Imagen {idx}: {c}' for c in data.get('correcciones') or [])

            # Los datos generales se toman de la primera imagen que los traiga.
            for campo in self._PACKING_LIST_HEADER:
                if datos[campo] is None and data.get(campo) is not None:
                    datos[campo] = data[campo]

            # Nombre del archivo de la imagen, ej. 6abf1be4b3877b8ab33880c1.jpeg
            nombre_imagen = os.path.basename(urlparse(url).path) or url
            serials_imagen = data.get('serials') or []
            cartones.append({
                **{campo: data.get(campo) for campo in self._PACKING_LIST_HEADER},
                'imagen': nombre_imagen,
                'total_serials': len(serials_imagen),
            })

            for serial in serials_imagen:
                sn = (serial.get('sn') or '').strip().upper()
                if sn and sn in vistos:
                    original = vistos[sn]
                    dup = duplicados.setdefault(sn, {'sn': sn, 'imagenes': [original['imagen'][0]]})
                    dup['imagenes'].append(nombre_imagen)
                    if nombre_imagen not in original['imagen']:
                        original['imagen'].append(nombre_imagen)
                    continue
                serial['imagen'] = [nombre_imagen]
                serial['duplicado'] = False
                if sn:
                    vistos[sn] = serial
                serials.append(serial)

        if not cartones:
            return {
                'status_code': 500,
                'msg': 'No se pudo procesar ninguna imagen',
                'warnings': warnings,
            }

        for sn, dup in duplicados.items():
            imagenes = ', '.join(dup['imagenes'])
            dup['observacion'] = f'La NS {sn} se encontró duplicada en las imágenes {imagenes}'
            vistos[sn]['duplicado'] = True

        datos['serials'] = serials
        datos['cartones'] = cartones
        if duplicados:
            datos['duplicados'] = list(duplicados.values())
        if correcciones:
            datos['correcciones'] = correcciones
        datos['labelPhotos'] = image_source
        fecha_monterrey = datetime.now(timezone.utc).astimezone(
            ZoneInfo("America/Monterrey")
        )
        datos['confirmedAt'] = fecha_monterrey.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

        if warnings:
            return {
                'status_code': 206,
                'msg': 'Extracción con advertencias',
                'data': datos,
                'warnings': warnings,
            }
        return {'status_code': 200, 'msg': 'OK', 'data': datos}

    def _ocr_packing_list_carton(self, image_source,
                                 extra_instructions: str = None,
                                 model: str = 'google/gemini-2.5-flash',
                                 max_tokens: int = 8000) -> dict:
        """
        Extrae los datos de una foto de la etiqueta de un cartón/caja de equipo
        de telecomunicaciones (ej. ONTs Huawei): los datos generales impresos en
        el panel de la caja (modelo, marca, código de cartón, SKU, item, orden,
        cantidad) y, por cada unidad individual etiquetada dentro de la caja,
        su Prod ID, número de serie (SN) y dirección MAC.

        Args:
            image_source: URL remota, ruta local, o lista de imágenes (una por
                          cara del cartón, si las etiquetas de las unidades
                          continúan en más de una cara/foto).
            model:        Modelo OpenRouter a usar. Se usa 'flash' (no 'lite')
                          por default porque las etiquetas de las unidades son
                          densas y 'lite' tiende a omitir filas.
            max_tokens:   Sube este valor si el cartón trae muchas unidades
                          (varias caras/fotos o cajas grandes).

        Returns:
            dict con:
                - status_code : 200 OK / 206 advertencias / 400 config / 500 error
                - data        : datos generales del cartón + lista de unidades
                                con su prod_id, sn y mac
                - msg         : mensaje de resultado
        """
        if not self.ai:
            return {'status_code': 400, 'msg': 'OpenRouter no configurado'}

        # "system" le da el rol/contexto al modelo (quién es y cómo debe comportarse).
        # "prompt" (abajo) es la instrucción concreta de la tarea + el JSON exacto que
        # debe regresar. Separar ambos es el mismo patrón que usan ocr_equipo/ocr_vehiculo.
        system = (
            "You are a logistics specialist trained to read carton/box labels for "
            "telecommunications equipment (ONTs, modems, routers, network gear). "
            "These labels have a header panel with the equipment type, brand, model, "
            "carton code, SKU, item, order and quantity, followed by one small label "
            "per individual unit packed inside the carton, each with its own Prod ID, "
            "serial number (SN) and MAC address. "
            "Always respond with a single valid JSON object and nothing else — "
            "no markdown, no backticks, no explanation, no preamble."
        )

        # El JSON de abajo NO es el resultado: es el "molde"/schema que se le pide al
        # modelo que llene. Cada línea documenta qué campo de la etiqueta corresponde
        # a cada llave, para que el modelo sepa exactamente dónde leer cada dato.
        # - Los campos de nivel raíz (tipo_equipo, marca, modelo, ...) son datos del
        #   cartón completo (aparecen una sola vez en el panel de la etiqueta).
        # - "serials" es una lista con una entrada por cada mini-etiqueta de unidad
        #   individual (Prod ID / SN / MAC) impresa dentro/alrededor del cartón.
        prompt = (
            "Analyze the provided carton label image(s) as a single combined carton "
            "(if there are multiple images, treat them as different faces/photos of "
            "the SAME carton/box, since the individual unit labels can continue across "
            "faces). Transcribe the header panel data and, for EVERY individual unit "
            "label visible (each printed as 'PROD ID: ...' next to 'SN: ...' and "
            "'MAC: ...'), extract its three values exactly as printed — do not skip or "
            "summarize any unit, even if there are dozens of them. "
            "If a field cannot be determined from the image, use null. "
            "\n\n"
            "CHARACTER ACCURACY (critical): SN and MAC values are uppercase hexadecimal "
            "(only 0-9 and A-F; MACs are 12 chars). In this font the letter 'B' and the digit '8' look very similar, "
            "as do 'D' and '0': read every character individually, zooming in mentally. "
            "'B' has a straight, flat vertical stroke on its LEFT side and square corners "
            "on the left; '8' is rounded/curved on BOTH sides and narrower at the waist. "
            "'D' has a straight left side; '0' is an oval curved on both sides. "
            "Do NOT infer characters from the pattern of the other units' serials — "
            "transcribe exactly what is printed on each label. "
            "Do not include the 'SN:' / 'MAC:' prefixes, spaces, colons or hyphens in the values. "
            "\n\n"
            "Return ONLY a JSON object with this exact structure:\n"
            "{\n"
            '  "tipo_equipo": "string — equipment type description printed on the label, e.g. TERMINAL PARA RED DE FIBRA OPTICA, or null",\n'
            '  "marca": "string — MARCA field, e.g. HUAWEI, or null",\n'
            '  "modelo": "string — MODELO field, e.g. Huawei OptiXstar HG8141X7b-50, or null",\n'
            '  "codigo_carton": "string — CODIGO DE CARTON value, or null",\n'
            '  "sku": "string — SKU number printed next to its barcode, or null",\n'
            '  "item": "string — ITEM code, or null",\n'
            '  "order": "string — ORDER code, or null",\n'
            '  "qty": "number — QTY value printed on the label header, or null",\n'
            '  "code": "string — CODE value, or null",\n'
            '  "notes": "string — NOTES field, e.g. HECHO EN CHINA, or null",\n'
            '  "serials": [\n'
            '    {\n'
            '      "prod_id": "string — PROD ID of this unit label",\n'
            '      "sn": "string — SN (serial number) of this unit label",\n'
            '      "mac": "string — MAC address of this unit label, or null"\n'
            '    }\n'
            '  ],\n'
            '  "confianza": "string — alto / medio / bajo — overall confidence based on image clarity"\n'
            "}"
        )

        # Instrucciones extra opcionales (ej. "el Part No. siempre empieza con 5"),
        # se agregan al final del prompt tal cual las mande quien llama la función.
        if extra_instructions:
            prompt += f"\n\nAdditional instructions: {extra_instructions}"

        # Sanitizar image_source: ocr_general espera SIEMPRE una lista de URLs/rutas.
        # Si mandan un solo string lo envolvemos en lista; si mandan una lista de dicts
        # (ej. el formato de archivos adjuntos de LinkaForm: [{'file_url': '...'}]),
        # extraemos solo la URL de cada uno.
        if isinstance(image_source, str):
            image_source = [image_source]
        elif isinstance(image_source, list):
            image_source = [
                img['file_url'] if isinstance(img, dict) else img
                for img in image_source
            ]

        print('>>> ocr_packing_list image_source=', image_source)

        # Los códigos de barras se leen antes de llamar al modelo: sus valores sirven
        # para corregir el SN/MAC al final y su orientación para enderezar las fotos.
        imagenes = self._cargar_imagenes(image_source)
        lectura = self._leer_barcodes(imagenes)

        # Si la foto viene girada (ej. de cabeza) el modelo lee el texto al revés y
        # confunde más caracteres. Se endereza y se le manda la versión derecha.
        imagenes_llm = list(image_source)
        for i, grados in enumerate(lectura['orientaciones']):
            if grados:
                print(f'>>> ocr_packing_list: enderezando imagen {i + 1} ({grados}°)')
                imagenes[i] = imagenes[i].rotate(-grados, expand=True)
                imagenes_llm[i] = self._imagen_a_data_url(imagenes[i])

        # El modelo recibe la foto completa reducida y en caracteres tan pequeños
        # confunde 'B' con '8'. Además de las fotos originales se le mandan
        # recortes ampliados de cada foto para que lea los SN/MAC con más detalle.
        recortes = self._recortes_ampliados(imagenes)
        if recortes:
            prompt += (
                f"\n\nThe first {len(image_source)} image(s) are the full carton photo(s). "
                f"The remaining {len(recortes)} images are ZOOMED, overlapping sections of "
                "those same photos: use them to read every SN and MAC character by "
                "character. Units that appear in more than one section are the SAME unit — "
                "list each unit only once."
            )

        # Llamada al modelo de OpenRouter: le mandamos la(s) imagen(es) + system + prompt.
        # Si son varias imágenes (varias caras del cartón), el modelo las analiza como
        # un solo cartón, por eso el prompt dice "different faces/photos of the SAME carton".
        raw_text = self.ai.ocr_general(imagenes_llm + recortes, system, prompt, model=model, max_tokens=max_tokens)

        # La respuesta viene con la forma típica de una API tipo OpenAI/OpenRouter:
        # {'choices': [{'message': {'content': <el JSON que pedimos>}}], ...}
        # Aquí solo navegamos esa estructura para sacar el contenido; si algo no viene
        # (ej. error del modelo), "datos" se queda como diccionario vacío.
        datos = {}
        if raw_text.get('choices'):
            choices = raw_text['choices']
            if isinstance(choices, list) and len(choices) > 0:
                content = choices[0].get('message', {}).get('content')
                if content:
                    datos = content

        print('ocr_packing_list datos=', datos)

        # _ocr_normalizar limpia texto (mayúsculas/espacios) en campos típicos de
        # identificación (curp, rfc, nombre). Un Packing List no trae esos campos,
        # así que aquí no cambia nada — se llama solo por consistencia con el resto
        # de las funciones de este archivo.
        datos = self._ocr_normalizar(datos)

        # Los recortes se traslapan, así que una unidad puede venir repetida.
        if recortes and isinstance(datos.get('serials'), list):
            unicos, vistos = [], set()
            for serial in datos['serials']:
                llave = (serial.get('prod_id') or serial.get('sn') or '').strip().upper()
                if llave and llave in vistos:
                    continue
                vistos.add(llave)
                unicos.append(serial)
            datos['serials'] = unicos

        # Igual que arriba: _ocr_validar_id valida formatos de CURP/RFC/fecha de
        # nacimiento. Para un Packing List siempre regresa una lista vacía (sin
        # advertencias), pero se deja para mantener el mismo flujo de respuesta
        # (200/206) que las demás funciones de OCR.
        errores = self._ocr_validar_id(datos)
        
        datos['labelPhotos'] = image_source
        fecha_monterrey = datetime.now(timezone.utc).astimezone(
            ZoneInfo("America/Monterrey")
        )
        datos['confirmedAt'] = fecha_monterrey.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

        # El LLM a veces confunde 'B' con '8' (y 'D' con '0') en el SN/MAC. Cada
        # unidad trae su SN y MAC también en código de barras, así que cuando se
        # logran decodificar se usan para corregir lo leído por el modelo.
        if not lectura['disponible']:
            errores.append('No está instalada la lectura de códigos de barras (zxing-cpp): '
                           'las NS/MAC no se verificaron')
        elif not lectura['valores']:
            errores.append('No se pudieron leer los códigos de barras de la foto: '
                           'las NS/MAC no se verificaron')
        else:
            correcciones, sin_verificar = self._packing_list_corregir_con_barcodes(
                datos, lectura['valores'])
            if correcciones:
                datos['correcciones'] = correcciones
            errores.extend(sin_verificar)

        modelo_series = datos.get('modelo') or ''
        marca_series = datos.get('marca') or ''
        # if modelo_series.lower() == 'huawei':
        if 'huawei' in modelo_series.lower() or 'huawei' in marca_series.lower():
            for serial in datos.get('serials', []):
                sn = serial.get('sn', '')
                serial['sn'] = f"HWTC{sn[8:]}" if sn else None

        if errores:
            return {
                'status_code': 206,
                'msg': 'Extracción con advertencias',
                'data': datos,
                'warnings': errores,
            }

        return {'status_code': datos.get('status_code', 200), 'msg': 'OK', 'data': datos}

    # Caracteres que el OCR suele confundir entre sí en etiquetas hexadecimales.
    # Se normalizan a un mismo valor para comparar lo leído por el LLM contra
    # lo decodificado de los códigos de barras.
    _OCR_CONFUSIONES = str.maketrans({'B': '8', 'D': '0', 'O': '0'})

    def _cargar_imagenes(self, image_source: list) -> list:
        """
        Descarga/abre las fotos del cartón como imágenes PIL para poder generar
        recortes ampliados y leer códigos de barras. Regresa una lista alineada
        con image_source: si Pillow no está instalado o una foto no se puede abrir,
        en su lugar va None y el flujo sigue solo con lo que lea el LLM de la foto
        original.
        """
        try:
            import io
            import requests
            from PIL import Image
        except ImportError as e:
            print('>>> _cargar_imagenes: Pillow no disponible', e)
            return [None] * len(image_source)

        imagenes = []
        for src in image_source:
            try:
                if str(src).startswith('http'):
                    resp = requests.get(src, timeout=30)
                    resp.raise_for_status()
                    img = Image.open(io.BytesIO(resp.content))
                else:
                    img = Image.open(src)
                imagenes.append(img.convert('RGB'))
            except Exception as e:
                print('>>> _cargar_imagenes error en', src, e)
                imagenes.append(None)
        return imagenes

    def _imagen_a_data_url(self, img, quality: int = 90) -> str:
        """Convierte una imagen PIL en data URL JPEG para mandarla al modelo."""
        import base64
        import io

        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=quality)
        b64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        return f'data:image/jpeg;base64,{b64}'

    def _recortes_ampliados(self, imagenes: list, columnas: int = 2, filas: int = 2,
                            traslape: float = 0.12, escala: int = 2) -> list:
        """
        Divide cada foto en una cuadrícula de secciones que se traslapan (para no
        partir una etiqueta a la mitad) y amplía cada sección. Regresa los recortes
        como data URLs JPEG listos para mandarse al modelo junto con la foto original.
        """
        from PIL import Image

        recortes = []
        for img in imagenes:
            if img is None:
                continue
            ancho, alto = img.size
            paso_x, paso_y = ancho / columnas, alto / filas
            extra_x, extra_y = paso_x * traslape, paso_y * traslape
            for fila in range(filas):
                for col in range(columnas):
                    caja = (
                        int(max(0, col * paso_x - extra_x)),
                        int(max(0, fila * paso_y - extra_y)),
                        int(min(ancho, (col + 1) * paso_x + extra_x)),
                        int(min(alto, (fila + 1) * paso_y + extra_y)),
                    )
                    rec = img.crop(caja)
                    rec = rec.resize((rec.width * escala, rec.height * escala), Image.LANCZOS)
                    recortes.append(self._imagen_a_data_url(rec))
        return recortes

    def _leer_barcodes(self, imagenes: list) -> dict:
        """
        Decodifica los códigos de barras Code128 de las imágenes del cartón.
        Las barras de cada unidad son pequeñas, por eso se intenta también con la
        imagen ampliada. Requiere zxing-cpp.

        Returns:
            dict con:
                - disponible    : False si zxing-cpp no está instalado
                - valores       : set con el texto de todos los códigos leídos
                - orientaciones : por cada imagen (alineado con 'imagenes'), los
                                  grados (0/90/180/270) en que viene girada según
                                  sus códigos de barras, o None si no se leyó ninguno
        """
        from collections import Counter

        lectura = {
            'disponible': True,
            'valores': set(),
            'orientaciones': [None] * len(imagenes),
        }
        try:
            import zxingcpp
            from PIL import Image, ImageOps
        except ImportError as e:
            print('>>> _leer_barcodes: lectura de códigos de barras deshabilitada', e)
            lectura['disponible'] = False
            return lectura

        for idx, img in enumerate(imagenes):
            if img is None:
                continue
            giros = Counter()
            try:
                img = ImageOps.grayscale(img)
                for escala in (1, 2, 3):
                    im = img if escala == 1 else img.resize(
                        (img.width * escala, img.height * escala), Image.LANCZOS)
                    for r in zxingcpp.read_barcodes(im, formats=zxingcpp.BarcodeFormat.Code128):
                        if r.text:
                            lectura['valores'].add(r.text.strip().upper())
                            # La orientación viene en grados; se redondea al cuarto de
                            # vuelta más cercano para tolerar fotos un poco chuecas.
                            giros[round(r.orientation / 90) * 90 % 360] += 1
            except Exception as e:
                print('>>> _leer_barcodes error', e)
            if giros:
                lectura['orientaciones'][idx] = giros.most_common(1)[0][0]
        print('>>> _leer_barcodes valores=', lectura['valores'])
        print('>>> _leer_barcodes orientaciones=', lectura['orientaciones'])
        return lectura

    def _packing_list_corregir_con_barcodes(self, datos: dict, barcodes: set) -> tuple:
        """
        Corrige el SN y la MAC de cada unidad usando los códigos de barras.
        Solo se reemplaza un valor si existe exactamente un código de barras que
        coincide con él salvo por confusiones B/8, D/0, O/0.

        Returns:
            tupla (correcciones, sin_verificar): las correcciones hechas (para dejar
            rastro en la respuesta) y las advertencias de los valores que no
            coinciden con ningún código de barras leído.
        """
        avisos, sin_verificar = [], []
        for serial in datos.get('serials') or []:
            for campo in ('sn', 'mac'):
                valor = re.sub(r'[\s:\-]', '', serial.get(campo) or '').upper()
                if not valor or valor in barcodes:
                    continue
                clave = valor.translate(self._OCR_CONFUSIONES)
                candidatos = [
                    b for b in barcodes
                    if len(b) == len(valor) and b.translate(self._OCR_CONFUSIONES) == clave
                ]
                if len(candidatos) == 1:
                    serial[campo] = candidatos[0]
                    avisos.append(f"{campo} corregido por código de barras: {valor} -> {candidatos[0]}")
                else:
                    sin_verificar.append(
                        f"{campo} {valor} no coincide con ningún código de barras, revisarlo manualmente")
        return avisos, sin_verificar


if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()

    # ── Datos de entrada ──────────────────────────────────────
    print('acceso_obj.data=',acceso_obj.data)
    data   = acceso_obj.data.get('data', {})
    form_id   = acceso_obj.data.get('form_id')
    option = data.get('option', '')
    nombre = data.get('nombre', data.get('name'))
    is_employee = data.get('is_employee', data.get('is_employee'))

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
    is_employee = True
    
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
            is_employee=is_employee
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
    elif option == 'ocr_articulo':
        # OCR genérico — extrae campos específicos de cualquier imagen
        response = acceso_obj.ocr_articulo_concesionado(
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
    elif option == 'ocr_persona':
        response = acceso_obj.ocr_persona(
            image_source=image_source,
            extra_instructions=extra_instructions,
        )
    elif option == 'ocr_equipo':
        response = acceso_obj.ocr_equipo(
            image_source=image_source,
            extra_instructions=extra_instructions,
        )
    elif option == 'ocr_acceso_transportista':
        response = acceso_obj.ocr_acceso_transportista(
            image_source=image_source,
            extra_instructions=extra_instructions,
        )
    elif option == 'ocr_packing_list':
        # image_source puede ser una URL o una lista de URLs (una por cartón)
        images = data.get('images') or image_source
        if isinstance(images, (str, dict)):
            images = [images]
        response = acceso_obj.ocr_packing_list(
            image_source=images,
            extra_instructions=extra_instructions,
        )
    else:
        response = {'msg': 'Empty', 'valid_options': ['ocr_id', 'ocr_doc', 'ocr_batch', 'ocr_paquete', 'ocr_truck', 'ocr_vehiculo', 'ocr_persona', 'ocr_equipo', 'ocr_articulo_perdido', 'ocr_articulo', 'ocr_packing_list']}

    acceso_obj.HttpResponse({'data': response})