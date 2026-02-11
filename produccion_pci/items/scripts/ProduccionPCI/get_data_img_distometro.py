# -*- coding: utf-8 -*-
import cv2, pytesseract, re, requests
import numpy as np

class DataImgDistometro():
    """docstring for DataImgDistometro"""
    def __init__(self):
        print('Class para leer datos de imagen distometro')
        self.metraje_maximo_aceptado = 1125

        self.preprocesamientos = {
            'v1': self.filtro_v1,
            'v2': self.filtro_v2,
            'v3': self.filtro_v3,
            'v4': self.filtro_v4,
            'v5': self.filtro_v5,
            'v6': self.filtro_v6,
            'v7': self.filtro_v7
        }

    def filtro_v7(self, roi, nombre_img, roi_resize=False):
        name_v7 = f"/tmp/pre_{nombre_img}_filtro_v7.png"
        # print(name_v7)
        ajustada = self.filtro_scaleAbs(roi, name_v7, roi_resize=roi_resize, alpha=2.5, beta=-160)

        # --- PASO 1: HSV ---
        hsv = cv2.cvtColor(ajustada, cv2.COLOR_BGR2HSV)

        # Rango para TEXTO claro/blanco-verde
        lower = np.array([0, 0, 180])      # poca saturación, muy brillante
        upper = np.array([180, 80, 255])

        mask = cv2.inRange(hsv, lower, upper)

        # --- PASO 2: limpiar máscara ---
        kernel = np.ones((3, 3), np.uint8)
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel, iterations=2)

        # --- PASO 3: invertir (texto negro sobre blanco) ---
        mask = cv2.bitwise_not(mask)

        return mask

    def extraer_texto_ocr(self, imagen, tipo="metraje", try_psm_11=False):
        if tipo == "metraje":
            patron = r"(\d+\.\d{1,2})"
            texto = pytesseract.image_to_string(imagen, config="--psm 7 --oem 3 -c tessedit_char_whitelist=0123456789.mts").strip()
            # print(f". . . . . OCR {tipo}: '{texto}'")
            match = re.search(patron, texto)
            if (re.search(r"\.\d{3}", texto) and texto.endswith('5')) or (texto.endswith('ms.')):
                # print('variante donde se encuentran 3 decimales y termina en 5')
                match = None

            # Validar caso donde viene un entero y el color de texto es gris
            if not match:
                texto = pytesseract.image_to_string(imagen, config="--psm 6 --oem 3 -c tessedit_char_whitelist=0123456789.mts").strip()
                # print(f"OCR 2 {tipo}: '{texto}'")
                match = re.search(r"(\d+(\.\d+)?)", texto)
                
                if texto.endswith('ms.'):
                    match = None

                # if not match:
                # if True:
                #     texto = pytesseract.image_to_string(imagen, config="--psm 6 --oem 3").strip()
                #     print(f"OCR 3 {tipo}: '{texto}'")

            if match:
                value_match = match.group(1)

                if float(value_match) < 10 and (texto.endswith('mts. 5') or f'{value_match}\n' in texto):
                    return []

                if float(value_match) > self.metraje_maximo_aceptado or try_psm_11:
                    texto = pytesseract.image_to_string(imagen, config="--psm 11 --oem 3 -c tessedit_char_whitelist=0123456789.mts").strip()
                    # print(f"OCR 4 {tipo}: '{texto}'")
                    match = re.search(patron, texto)
                    if match:
                        return [match.group(1), 'psm_11']

                return [value_match]
            return []
        else:  # folio
            config = "--psm 7 --oem 3 -c tessedit_char_whitelist=0123456789"
            texto = pytesseract.image_to_string(imagen, config=config).strip()
            # print(f"OCR {tipo}: '{texto}'")
            if not texto:
                texto = pytesseract.image_to_string(imagen, config="--psm 6 --oem 3 -c tessedit_char_whitelist=0123456789").strip()
                # print(f"OCR 2 {tipo}: '{texto}'")

                if not texto:
                    texto = pytesseract.image_to_string(imagen, config="--psm 11 --oem 3 -c tessedit_char_whitelist=0123456789").strip()
                    # print(f"OCR 3 {tipo}: '{texto}'")

                if not texto:
                    # Caso donde viene la imagen pegada con otro cuadro verde que dice Metraje adicional
                    texto = pytesseract.image_to_string(imagen, config="--psm 6 --oem 3")
                    texto = texto.strip().lower()
                    if 'metraje adicional' in texto:
                        return 'text_metraje_adicional'

            if texto.isdigit() and texto.startswith('0'):
                texto = str( int(texto) )
            return re.findall(r"\d+", texto)

    def detectar_recuadros_verdes(self, image, debug_path=None):
        """
        Detecta recuadros verdes y los clasifica en superior (metraje) e inferior (folio).
        Devuelve las coordenadas de los recuadros más anchos en cada zona.
        """
        hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
        lower_green = np.array([40, 100, 100])
        upper_green = np.array([70, 255, 255])
        mask = cv2.inRange(hsv, lower_green, upper_green)
        kernel = np.ones((5, 5), np.uint8)
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel)
        mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cv2.imwrite(debug_path, mask)
        h, w = image.shape[:2]
        metraje_candidates = []
        folio_candidates = []
        debug_img = image.copy()

        for cnt in contours:
            x, y, cw, ch = cv2.boundingRect(cnt)
            area = cw * ch
            aspect_ratio = cw / ch
            if area > 2000 and aspect_ratio > 1.5:
                if y < h * 0.4:
                    metraje_candidates.append((x, y, cw, ch))
                    cv2.rectangle(debug_img, (x, y), (x+cw, y+ch), (0, 255, 0), 2)
                elif y > h * 0.6:
                    folio_candidates.append((x, y, cw, ch))
                    cv2.rectangle(debug_img, (x, y), (x+cw, y+ch), (0, 0, 255), 2)
        if debug_path:
            cv2.imwrite(debug_path, debug_img)
        metraje_box = max(metraje_candidates, key=lambda b: b[2], default=None)
        folio_box = max(folio_candidates, key=lambda b: b[1], default=None)
        return metraje_box, folio_box

    def filtro_scaleAbs(self, roi, path_img, roi_resize=False, alpha=None, beta=None):
        """
        alpha: Es el Contraste
        beta: Es el Brillo
        """
        if (alpha is None) or (beta is None):
            return None

        if roi_resize:
            # Por casos donde detecta metraje 6 pero en realidad es 0
            roi = cv2.resize(roi, None, fx=3, fy=3, interpolation=cv2.INTER_CUBIC)
        
        ajustada = cv2.convertScaleAbs(roi, alpha=alpha, beta=beta)
        cv2.imwrite(path_img, ajustada)
        return ajustada

    #! Filtros para capturas ####################################################
    def filtro_v1(self, roi, nombre="metraje", roi_resize=False):
        name_v1 = f"/tmp/pre_{nombre}_filtro_v1.png"
        
        # if nombre=="metraje":
        # print(name_v1)
        
        return self.filtro_scaleAbs(roi, name_v1, roi_resize=roi_resize, alpha=1.0, beta=-60)

    def filtro_v2(self, roi, nombre="metraje", roi_resize=False):
        hsv = cv2.cvtColor(roi, cv2.COLOR_BGR2HSV)
        v = hsv[:, :, 2]
        alto, ancho = v.shape
        if alto < 60 or ancho < 200:
            v = cv2.resize(v, (ancho*3, alto*3), interpolation=cv2.INTER_CUBIC)
        name_v2 = f"/tmp/pre_{nombre}_filtro_v2.png"
        # print(name_v2)
        cv2.imwrite(name_v2, v)
        return v

    def filtro_v3(self, roi, nombre="metraje", roi_resize=False):
        name_v3 = f"/tmp/pre_{nombre}_filtro_v3.png"
        return self.filtro_scaleAbs(roi, name_v3, roi_resize=roi_resize, alpha=2.5, beta=-180)

    def filtro_v4(self, roi, nombre="metraje", roi_resize=False):
        lab = cv2.cvtColor(roi, cv2.COLOR_BGR2LAB)
        l, a, b = cv2.split(lab)
        clahe = cv2.createCLAHE(clipLimit=8.0, tileGridSize=(4,4))
        cl = clahe.apply(l)
        limg = cv2.merge((cl,a,b))
        final = cv2.cvtColor(limg, cv2.COLOR_LAB2BGR)
        gris = cv2.cvtColor(final, cv2.COLOR_BGR2GRAY)
        alto, ancho = gris.shape
        if alto < 60 or ancho < 200:
            gris = cv2.resize(gris, (ancho*4, alto*4), interpolation=cv2.INTER_CUBIC)
        name_v4 = f"/tmp/pre_{nombre}_filtro_v4.png"
        # print(f'.. .. .. name_v4= {name_v4}')
        cv2.imwrite(name_v4, gris)
        return gris

    def filtro_v5(self, roi, nombre="metraje", roi_resize=False):
        alpha = 2.0  # Contraste
        beta = 0     # Brillo
        ajustada = cv2.convertScaleAbs(roi, alpha=alpha, beta=beta)
        lab = cv2.cvtColor(ajustada, cv2.COLOR_BGR2LAB)
        l = lab[:, :, 0]
        l_inv = cv2.bitwise_not(l)
        alto, ancho = l_inv.shape
        if alto < 60 or ancho < 200:
            l_inv = cv2.resize(l_inv, (ancho*6, alto*6), interpolation=cv2.INTER_CUBIC)
        cv2.imwrite(f"/tmp/pre_{nombre}_filtro_v5.png", l_inv)
        return l_inv

    def filtro_v6(self, roi, nombre="metraje", roi_resize=False):
        alpha = 2.5  # Contraste
        beta = -160    # Brillo
        ajustada = cv2.convertScaleAbs(roi, alpha=alpha, beta=beta)
        cv2.imwrite(f"/tmp/pre_{nombre}_filtro_v6.png", ajustada)
        return ajustada

    def extract_num_mts(self, texto):
        match = re.search(r'(\d+(?:\.\d+)?)\s*mts', texto)
        if match:
            return match.group(1)
        return ""

    def metraje_mayor_limite(self, list_metraje):
        if not list_metraje:
            return False
        return float( list_metraje[0] ) > self.metraje_maximo_aceptado

    def ocr_metraje_combinado(self, roi_metraje, img_name):
        """
        Prueba varios preprocesamientos y retorna el metraje más probable.
        """
        resultados = []
        textos_ocr = []
        try_psm_11 = False
        for num_v, preproc in self.preprocesamientos.items():
            # print(f'     .. evaluando filtro {num_v}')
            pre_metraje = preproc(roi_metraje, f"{img_name}_metraje")

            metrajes = self.extraer_texto_ocr(pre_metraje, "metraje", try_psm_11=try_psm_11)
            if len(metrajes) == 2 and metrajes[1] == 'psm_11':
                try_psm_11 = True
            # print(f'        .. metrajes= {metrajes}')

            if (metrajes == ['6'] and num_v in ["v3", "v1"]) or self.metraje_mayor_limite(metrajes):
                pre_metraje = preproc(roi_metraje, f"{img_name}_metraje", roi_resize=True)
                metrajes = self.extraer_texto_ocr(pre_metraje, "metraje")
                # print(f'        .. metrajes caso 6 detectado = {metrajes}')

            # print(f'        .. metrajes= {metrajes}')

            texto_ocr = pytesseract.image_to_string(pre_metraje, config="--psm 7 --oem 3")
            textos_ocr.append(texto_ocr)
            for metraje in metrajes:
                if metraje and re.match(r"^\d+(\.\d+)?$", metraje):
                    resultados.append(metraje)
        if resultados:
            print('-- --- mts found =',resultados)
            from collections import Counter
            # metraje_final, _ = Counter(resultados).most_common(1)[0]

            # metraje_final_2 = next(
            #     (x for x in resultados if float(x) > 0),
            #     '0'
            # )
            # print(f"    -- --- metraje_final = {metraje_final} metraje_final_2 = {metraje_final_2}")

            counter = Counter(resultados)
            metraje_final, _ = max(
                counter.items(),
                key=lambda item: (
                    # PRIORIDADES para seleccionar el mejor metraje de lista de resultados
                    0 <= float(item[0]) <= self.metraje_maximo_aceptado, # mayor a cero y menor al limite maximo
                    '.' in item[0], # Tiene decimal
                    item[1] # Mayor frecuencia
                )
            )

            return metraje_final
        for texto in textos_ocr:
            num = self.extract_num_mts(texto)
            if num:
                return num
        return ""

    def ocr_folio_combinado(self, roi_folio, img_name, roi_superior, folio_record):
        """
        Prueba varios preprocesamientos y retorna el folio más probable.
        Si alguno de los candidatos coincide exactamente con img_name, lo retorna.
        """
        candidatos = []
        for num_v, preproc in self.preprocesamientos.items():
            # print(f'     .. evaluando filtro {num_v}')
            pre_folio = preproc(roi_folio, f"{img_name}_folio")
            folios = self.extraer_texto_ocr(pre_folio, "folio")
            # print(f'        .. folios= {folios}')

            if folios == 'text_metraje_adicional' and roi_superior is not None:
                # recortar un poco mas arriba
                # print('.. ... .... ..... se encuentra caso donde hay una parte inferior con texto metraje adicional')
                pre_folio = preproc(roi_superior, f"{img_name}_folio_superior")
                folios = self.extraer_texto_ocr(pre_folio, "folio")
                # print(f'        .. folios superor= {folios}')
            
            for folio in folios:
                if folio.isdigit():
                    candidatos.append(folio)
        if img_name and img_name in candidatos:
            return img_name
        # Prioriza los de exactamente 8 dígitos
        # print('FOLIOS candidatos =', candidatos)

        # Caso donde el folio que se busca ya está dentro de los candidatos se toma como bueno
        # Esto por casos donde hay letras cursivas y si hay una funcion donde se lee correcto pero otras no
        if folio_record and folio_record in candidatos:
            return folio_record

        folios_8 = [f for f in candidatos if len(f) == 8]

        # print('folios_8 =',folios_8)

        if folios_8:
            from collections import Counter
            folio_final, _ = Counter(folios_8).most_common(1)[0]
            return folio_final
        if candidatos:
            recortados_8 = [f[-8:] for f in candidatos if len(f) > 8]
            if img_name and img_name in recortados_8:
                return img_name
            folio_final = max(candidatos, key=len)
            if len(folio_final) > 8:
                return folio_final[-8:]
            return folio_final
        return ""

    def load_image_from_url(self, url):
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            raise ValueError(f"No se pudo descargar la imagen desde {url}")

        # Convertir bytes en array numpy
        img_array = np.asarray(bytearray(response.content), dtype=np.uint8)
        image = cv2.imdecode(img_array, cv2.IMREAD_COLOR)

        if image is None:
            raise ValueError("Error al decodificar la imagen desde la URL")

        return image

    def get_data_info_distometro(self, nombre_img, url_img, folio_record, by_path=False, folio_found=None):
        # images_correct = [
        #     '18126634.jpg',
        #     '30531375.jpg',
        # ]
        
        # print('Total imagenes a procesar =',len(images_correct))
        # for img in images_correct:
        #     print('====================')
        metraje, folio = None, None
        if by_path:
            image = self.load_image(url_img)
        else:
            image = self.load_image_from_url(url_img)
        metraje_box, folio_box = self.detectar_recuadros_verdes(image, debug_path=f"/tmp/{nombre_img}.png")
        if metraje_box:
            x, y, w, h = metraje_box
            roi_metraje = image[y:y+h, x:x+int(w*0.8)]
            # print('.. entra a metraje_box')
            metraje = self.ocr_metraje_combinado(roi_metraje, nombre_img)
            # print("+++ metraje =", metraje)

        if folio_found:
            folio = folio_found
        elif folio_box:
            x, y, w, h = folio_box
            # Si el recuadro es muy alto, recorta la mitad inferior
            alto_umbral = 100
            roi_superior = None
            if h > alto_umbral:
                
                mitad = h // 2
                roi_superior = image[y:y+mitad, x:x+w]
                
                # Recorta la mitad inferior
                y = y + h // 2
                h = h // 2
            
            roi_folio = image[y:y+h, x:x+w]
            

            folio = self.ocr_folio_combinado(roi_folio, nombre_img, roi_superior, folio_record)
            # print(f"+++ folio = {folio}")
            
            # Probable caso de dos cuadros verdes pero solo en uno viene el folio. Intentar con el roi superior
            if len(folio) < 8 and roi_superior is not None:
                folio = self.ocr_folio_combinado(roi_superior, nombre_img, None, folio_record)

        metraje_invalid = self.valid_metraje(metraje)
        if isinstance(metraje_invalid, dict) and metraje_invalid.get('error'):
            return metraje_invalid, None

        if folio and folio != folio_record:
            return {'error': f"Folio leido {folio} es diferente al folio del registro {folio_record}"}, folio
        
        return metraje, folio

    






    # ====================================================================
    # Aqui las funciones que tenía en la V1 del script
    # ====================================================================
    def get_green_regions(self, image, variante=None):
        hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
        
        lower_green = np.array([50, 100, 100])   # Verde más oscuro

        if variante == "00":
            lower_green = np.array([30, 40, 40])
        
        # upper_green = np.array([100, 255, 255])
        upper_green = np.array([70, 255, 255])   # Verde más claro
        mask = cv2.inRange(hsv, lower_green, upper_green)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        return contours

    def find_top_green_box(self, contours, image_shape, img_bottom=False):
        """
        Encuentra el cuadro verde más alto y válido en la parte superior de la imagen.
        Args:
            contours (List[np.ndarray]): Contornos detectados.
            image_shape (Tuple[int, int, int]): Shape de la imagen (alto, ancho, canales).
        Returns:
            Tuple[int, int, int, int]: Coordenadas del bounding box (x, y, w, h).
        Raises:
            ValueError: Si no se encuentra ningún recuadro válido.
        """
        h, w = image_shape[:2]

        # Lista de candidatos (solo cuadros verdes en parte superior)
        candidates = []
        # other_candidates = []
        for cnt in contours:
            x, y, cw, ch = cv2.boundingRect(cnt)

            aspect_ratio = cw / ch
            area = cw * ch
            
            if 2 < aspect_ratio < 10 and 3000 < area < 150000:  # filtro por forma y tamaño
                candidates.append((x, y, cw, ch))

        if not candidates:
            return {
                'error': 'No se encontró ningún cuadro verde válido.'
            }
            # raise ValueError("No se encontró ningún cuadro verde válido.")
        
        return sorted(candidates, key=lambda rect: rect[1])[ 0 if not img_bottom else -1 ]

    def crop_region(self, image, box):
        x, y, w, h = box
        padding = 5
        # return image[y:y+h, x:x+w]
        return image[y+padding:y+h-padding, x+padding:x+w-padding]

    def save_image(self, image, filename):
        cv2.imwrite(filename, image)
    
    def load_image(self, path):
        image = cv2.imread(path)
        if image is None:
            raise ValueError(f"No se pudo cargar la imagen en: {path}")
        return image

    def normalizar_numero(self, texto):
        """
        Normaliza un texto numérico eliminando puntos al inicio o final, y valida si representa 
        un número entero o flotante válido.

        Casos especiales:
            - Si el texto empieza o termina con un punto, se elimina ese punto.
            - Si el texto resultante tiene más de un punto decimal, se considera inválido.
            - Si el texto contiene solo dígitos, se interpreta como int.
            - Si el texto contiene un punto decimal entre dígitos, se interpreta como float.
            - Cualquier otro caso devuelve None.

        Args:
            texto (str): El texto que representa un posible número.

        Returns:
            int | float | None: El número como int o float si es válido, 
                                o None si no cumple las condiciones.
        """
        if not texto:
            return ""

        # Eliminar punto inicial o final
        if texto.startswith('.'):
            texto = texto[1:]
        if texto.endswith('.'):
            texto = texto[:-1]

        # Verificar si el resultado está vacío o inválido
        if not texto or texto.count('.') > 1:
            return ""

        # Validar si es entero
        if texto.isdigit():
            return texto

        # Validar si es float
        if texto.count('.') == 1:
            parte_entera, parte_decimal = texto.split('.')
            if parte_entera.isdigit() and parte_decimal.isdigit():
                return texto

        return ""

    def read_text(self, path, nombre_img, try_other_psm=False):
        green_image = self.load_image(path)

        # Convertir a escala de grises y binarizar
        gris = cv2.cvtColor(green_image, cv2.COLOR_BGR2GRAY)
        suavizado = cv2.GaussianBlur(gris, (3, 3), 0)
        binaria = cv2.adaptiveThreshold(suavizado, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 2)
        invertida = cv2.bitwise_not(binaria)
        
        self.save_image(invertida, f'/tmp/{nombre_img}_invertida.png')

        custom_config = '--psm 6 --oem 3 -c tessedit_char_whitelist=0123456789.mts'
        # custom_config = '--psm 11  --oem 3 -c tessedit_char_whitelist=0123456789.mts'
        # custom_config = '--psm 7  --oem 3 -c tessedit_char_whitelist=0123456789.mts'

        # Usar OCR para extraer texto
        texto = pytesseract.image_to_string(invertida,  config=custom_config)  

        # if 'metraje' in nombre_img:
        #     print('text [1] = ',texto)
        
        # Limpiar el texto
        texto_limpio = ''
        rows_text = texto.split('\n')
        for rw_txt in rows_text:
            rw_txt_limpio = self.normalizar_numero( rw_txt.strip() )
            if rw_txt_limpio:
                return rw_txt_limpio
            # print('---  --- ',rw_txt_limpio)

        texto_limpio = texto.split('\n')[0].strip()

        # Regresar resultado
        return self.normalizar_numero(texto_limpio)
    
    def get_text_img(self, img, contours, output_path_box, nombre_img, bottom_box=False, get_mts=False):
        green_box = self.find_top_green_box(contours, img.shape, img_bottom=bottom_box)

        if isinstance(green_box, dict) and green_box.get('error'):
            return green_box
        
        cropped = self.crop_region(img, green_box)
        self.save_image(cropped, output_path_box)
        return self.read_text(output_path_box, nombre_img, try_other_psm=get_mts)

    def valid_metraje(self, metraje):
        # Si no hay metraje, se devuelve como llegó
        if not metraje:
            return metraje
        
        # Se intenta validar valor correcto en metraje
        try:
            if isinstance(metraje, str):
                # Se eliminan espacios en blanco
                metraje = metraje.strip()
                # Si viene en formato de miles con separacion de comas
                if ',' in metraje and '.' in metraje:
                    metraje = metraje.replace(',', '')

            # Se convierte a float para verificar el limite
            float_metraje = float(metraje)
            if float_metraje > self.metraje_maximo_aceptado:
                return {'error': f'Metraje mayor al aceptado {self.metraje_maximo_aceptado}'}
        except:
            # si no se puede procesar se regresa error
            return {'error': f'No se pudo procesar el metraje {metraje}'}
        # Todo esta OK, regresamos el metraje que se recibió
        return metraje

    def get_metraje_folio(self, nombre_img, url_img, by_path=False):
        output_path = f"/tmp/{nombre_img}_metraje.png"
        output_path_folio = f"/tmp/{nombre_img}_folio.png"

        if by_path:
            image = self.load_image(url_img)
        else:
            image = self.load_image_from_url(url_img)
            
        contours = self.get_green_regions(image)

        metraje = self.get_text_img(image, contours, output_path, f"{nombre_img}_metraje")

        if isinstance(metraje, dict) and metraje.get('error'):
            return metraje, None

        if not metraje:
            metraje = self.get_text_img(image, contours, output_path, f"{nombre_img}_metraje", get_mts=True)
        # print('+++ metraje =',metraje)
        
        folio = self.get_text_img(image, contours, output_path_folio, nombre_img, bottom_box=True)

        # # Variante donde se devuelve "00" se reintenta con otro contorno
        if folio == "00":
            contours = self.get_green_regions(image, variante="00")
            print('[ERROR] se reintenta variante 00')
            folio = self.get_text_img(image, contours, output_path_folio, nombre_img, bottom_box=True)
        
        # print('+++ folio =',folio)
        if len(folio) > 8:
            folio = folio[-8:]

        metraje_invalid = self.valid_metraje(metraje)
        if isinstance(metraje_invalid, dict) and metraje_invalid.get('error'):
            return metraje_invalid, folio

        if folio and folio != nombre_img:
            return {'error': f"Folio leido {folio} es diferente al folio del registro {nombre_img}"}, folio

        return metraje, folio

    def valid_folio_mts( self, folio_obtenido, mts_obtenido, folio_orden_servicio ):
        if not folio_obtenido or not mts_obtenido:
            return False

        if len(folio_obtenido) != 8: # or folio_obtenido != folio_orden_servicio:
            return False

        if isinstance(mts_obtenido, dict) and mts_obtenido.get('error'):
            return False

        if ('.' not in mts_obtenido) and mts_obtenido.isdigit() and (0 < int(mts_obtenido) <=10):
            return False

        return True