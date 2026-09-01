# -*- coding: utf-8 -*-
"""
Adaptado de infosync_scripts/PCI/crear_vales_de_materiales.py

Calcula el material estimado (por Conexion / Area / Tecnologia / Location) a
partir de las Ordenes de Servicio de un periodo y los Kits de productos
configurados, pero a diferencia del script original NO crea registros de
Vales de Materiales ni cierra las Ordenes de Servicio: solo consulta y adjunta
el resultado como Excel al registro actual.

NOTA: Los form_id y field_id de __init__ son los MISMOS que usa el script
original de PCI (crear_vales_de_materiales.py). Se respetaron tal cual a
peticion, pero pertenecen a las formas/catalogos de la cuenta de PCI: hay que
confirmar que sean los correctos para la forma/catalogos de Accesos que
disparen este script (marcados con TODO donde no hay una forma equivalente
clara en el original).
"""
import sys, pyexcel
from linkaform_api import settings
from datetime import datetime
from dateutil.relativedelta import relativedelta
from copy import deepcopy

from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

from account_settings import *

# from accesos_utils import Accesos
from stock_ont_utils import Stock

class Stock(Stock):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        # True si la petición se hizo desde el Front de SIGA
        self.front_request = not self.current_record
        self.data = self.data.get('data',{})

        # form_id / catalog_id (iguales a crear_vales_de_materiales.py)
        self.FORM_ID_KITS = 164757 # 164626 # PREPROD
        self.CATALOG_ID_SKU = 133015 # Este ya está OK en Prod y Preprod.
        self.CATALOG_ID_TIPOS_TAREA = 56269 # Este se va a consultar la info que está en Admin.
        self.FORMS_ID_FTTH = [11044, 16343, 21954, 21953, 147977]
        self.FORMS_ID_COBRE = [10540, 25927, 25928, 25929, 147978]
        self.FORM_ID_EXP_TECNICOS = 63122

        self.map_form_name = {
            11044: "Orden de servicio METRO - FTTH",
            16343: "Orden de servicio SUR - FTTH",
            21954: "Orden de servicio NORTE - FTTH",
            21953: "Orden de servicio OCCIDENTE - FTTH",
            147977: "Orden de servicio TELNOR - FTTH",
            10540: "Orden de servicio METRO - COBRE",
            25927: "Orden de servicio SUR - COBRE",
            25928: "Orden de servicio NORTE - COBRE",
            25929: "Orden de servicio OCCIDENTE - COBRE",
            147978: "Orden de servicio TELNOR - COBRE",
        }

        self.config['JWT_ADMIN'] = self.lkf_api.get_jwt( 
            api_key='398bd78880b1675a4a8d06d8a89e712ad9b499fb', 
            user='adminpclink@operacionpci.com.mx' 
        )
        self.settings.config.update(self.config)

        self.cant_minima_folios_para_metraje = 5

        # field_id del catalogo de Contratistas/Conexiones
        self.catalogo_contratistas_fields = {
            'email': '5f344a0476c82e1bebc991d8',
            'nombre': '5f344a0476c82e1bebc991d7',
            'razon_social': '5f344a0476c82e1bebc991db',
            'id_user': '5f344a0476c82e1bebc991d6',
        }


        self.field_id_catalog_email = self.catalogo_contratistas_fields['email']
        self.field_id_catalog_nombre = self.catalogo_contratistas_fields['nombre']
        self.field_id_catalog_razon_social = self.catalogo_contratistas_fields['razon_social']
        self.field_id_catalog_id_user = self.catalogo_contratistas_fields['id_user']

        # field_id de Tipo de Tarea (por tecnologia) y Tipo de Material en la Orden de Servicio
        self.tipo_tarea_fields = {
            'ftth': 'f1054000a010000000000021',
            'cobre': 'f1054000a0100000000000a4',
        }
        self.field_id_tipo_material = '6346f10c24cc48504673c5d9'

        # field_id del catalogo de Productos/SKU
        self.catalogo_productos_fields = {
            'codigo': '61ef32bcdf0ec2ba73dec33d',
            'sku': '65dec64a3199f9a040829243',
            'nombre': '61ef32bcdf0ec2ba73dec33e',
            'tipo_de_material': '66b10b87a1d4483b5369f409',
            'unidad_medida': '65dec8423199f9a040829246',
            'relevancia': '6914a477499b225caf66fedc',
        }

        # field_id del catalogo de Tipos de Tarea (aplica material / ONT / modem)
        self.catalogo_tipos_tarea_fields = {
            'tipo_de_tarea': '5ec44fd1ac6a37c45828230a',
            'aplica_bajante_cobre': '686815963859301cd95a58a7',
            'aplica_bajante_fibra': '687e7edea637578f0fda2216',
            'aplica_ont': '687e7edea637578f0fda2217',
            'aplica_modem': '687e7edea637578f0fda2218',
        }

        # field_id de la forma de Kits (FORM_ID_KITS)
        self.sku_catalog_obj_id = '6824e631aa87c4ed5f18bcaa'
        self.kits_fields = {
            'tecnologia': '68819db58b1b1ab2e1d5ef2b',
            'tipo': '68819e548b1b1ab2e1d5ef31',
            'tipo_material': '68819e1b9e38c6f0be9066c3',
            'grupo_productos': '68819f06fd736f4a03d5eece',
            'cantidad': '68819f7b5bf2a425981fef5b',
            'unidad_medida_obj': '6824e60fb2d7f2425a31357a',
            'unidad_medida': '669efc6f47920d1b51663d29',
        }

        # field_id de las formas de Orden de Servicio (FORMS_ID_FTTH / FORMS_ID_COBRE)
        self.orden_servicio_fields = {
            'connection_email_1': 'f1054000a0100000000000d6',
            'connection_email_2': 'f1054000a010000000000007',
            'area': 'f1054000a0100000000000a2',
            'cope': 'f1054000a010000000000002',
            'fecha_liquidacion_1': 'f1054000a02000000000fa02',
            'fecha_liquidacion_2': '5a1eecfbb43fdd65914505a1',
        }

        # field_id donde se adjunta el Excel de resultado y de la forma que dispara este script
        # TODO: confirmar que esta forma sea la que se usara en Accesos para disparar la consulta
        self.field_id_file_estimacion = '6a02cc52f6e31b83d643468b'
        self.material_estimado_fields = {
            'desde': '6869fde18c41efc6988f6f32',
            'hasta': '6869fde18c41efc6988f6f33',
            'tecnologia': '6a06075ed4a7b1e477b59990',
            'estatus': '6869fe202a52ec91362e9d3e',
            'mensaje': '6869fe202a52ec91362e9d3f',
        }

        self.header_xls_fibra = [
            "Nombre de la forma", "Folio", "Conexión", "Tipo de Tarea", "Teléfono", "AREA", "COPE", "Distrito", 
            "Expediente Del Tecnico", "Técnico TAC", "Fecha de Carga Contratista", "Tipo de Material", "Alfanumérico TAC", 
            "Alfanumérico Contratista", "Alfanumérico Técnico", "Tipo de Instalación", "Metros Bajante", "Metraje Adicional", 
            "Fecha Liquidada", "Tecnico", "Estatus de Orden", "Tipo de Expediente", 
        ]

        self.header_xls_cobre = [
            "Nombre de la forma", "Folio", "Conexión", "Tipo de Tarea", "AREA", "COPE", "Distrito", "Telefono", "Tecnico", 
            "Expediente del Tecnico", "Técnico TAC", "Fecha de Carga Contratista", "Modem - Numero de Serie", 
            "Numero de Serie Contratista", "Numero de Serie Técnico", "Metros Bajante", "Fecha de Liquidacion", 
            "Estatus de Orden", "Tipo de Expediente", 
        ]

        self.header_material = [
            'Conexión', 'Area', 'Tecnología', 'Código de Producto', 'SKU', 'Nombre del producto',
            'Unidad de Medida', 'Cantidad estimada'
        ]

        # Conexión a Mongo de Admin
        from pci_get_connection_db import CollectionConnection
        colection_connection = CollectionConnection(1259, self.settings)
        self.cr_admin = colection_connection.get_collections_connection()

    def validar_periodo(self, inicio: str, fin: str, formato: str = "%Y-%m-%d"):
        """
        Valida un periodo de fechas.

        Args:
            inicio (str): Fecha de inicio, formato 'YYYY-MM-DD'.
            fin (str): Fecha de fin, formato 'YYYY-MM-DD'.
            formato (str): Formato de las fechas de entrada.

        Returns:
            tuple[bool, str|None]: (es_valido, mensaje_error).
            Si es_valido es True, mensaje_error es None.

        Raises:
            ValueError: Si alguna fecha no cumple el formato esperado.
        """
        if self.front_request:
            # Si no dan fin, se usa hoy
            fecha_fin = datetime.strptime(fin, formato) if fin else datetime.now()
            # Si no dan inicio, se calcula un mes y medio antes de fin
            if inicio:
                fecha_inicio = datetime.strptime(inicio, formato)
            else:
                fecha_inicio = fecha_fin - relativedelta(months=1, days=15)
        else:
            if not inicio or not fin:
                return False, "Se requieren las fechas de Inicio y Fin."
            
            fecha_inicio = datetime.strptime(inicio, formato)
            fecha_fin = datetime.strptime(fin, formato)

        # 1 y 2: inicio no puede ser mayor a fin (equivalente a: fin no puede ser anterior a inicio)
        if fecha_inicio > fecha_fin:
            return False, "La fecha de inicio no puede ser posterior a la fecha de fin."

        # 3: el periodo no puede exceder un mes y medio (1 mes + 15 días)
        limite = fecha_inicio + relativedelta(months=1, days=15)
        if fecha_fin > limite:
            return False, "El periodo no puede exceder un mes y medio."

        return True, { 'inicio': fecha_inicio.strftime(formato), 'fin': fecha_fin.strftime(formato) }

    def get_kits(self):
        """
        Consulta la forma de Kits y arma un diccionario:
        { tecnologia: { tipo: { tipo_material: [productos...] } } }
        """
        f = self.kits_fields
        query = [
            {'$match': {
                'form_id': self.FORM_ID_KITS,
                'deleted_at': {'$exists': False},
            }},
            {'$project': {
                'tecnologia': f'$answers.{f["tecnologia"]}',
                'tipo': f'$answers.{f["tipo"]}',
                'tipo_material': {'$ifNull': [f'$answers.{f["tipo_material"]}', 'telmex/condumex']},
                'productos': {
                    '$map': {
                        'input': f'$answers.{f["grupo_productos"]}',
                        'as': 'producto',
                        'in': {
                            'clave_producto': f'$$producto.{self.sku_catalog_obj_id}.{self.catalogo_productos_fields["codigo"]}',
                            'cantidad': f'$$producto.{f["cantidad"]}',
                            'unidad_medida': f'$$producto.{f["unidad_medida_obj"]}.{f["unidad_medida"]}',
                        },
                    },
                },
            }},
            {'$group': {
                '_id': {
                    'tecnologia': '$tecnologia',
                    'tipo': '$tipo',
                    'tipo_material': '$tipo_material',
                },
                'productos': {'$push': '$productos'},
            }},
            {'$project': {
                'tecnologia': '$_id.tecnologia',
                'tipo': '$_id.tipo',
                'productos': {'$reduce': {
                    'input': '$productos',
                    'initialValue': [],
                    'in': {'$concatArrays': ['$$value', '$$this']},
                }},
            }},
            {'$unwind': '$_id.tipo_material'},
        ]

        kits = {}
        for kit in self.cr.aggregate(query):
            kits.setdefault(kit['tecnologia'], {}) \
                .setdefault(kit['tipo'], {}) \
                .setdefault(kit['_id']['tipo_material'], []) \
                .extend(kit.get('productos', []))
        return kits

    def clean_value(self, value):
        if isinstance(value, str):
            return ILLEGAL_CHARACTERS_RE.sub("", value)
        return value

    def fix_encoding(self, text):
        if not isinstance(text, str):
            return text

        if text.startswith("="):
            return ""

        try:
            return text.encode("latin1").decode("utf-8")
        except Exception:
            return text

    def make_excel_file(self, rows, content_sheets={}):
        date = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
        file_path = f"/tmp/output_{date}.xlsx"
        if content_sheets:
            pyexcel.get_book(bookdict=content_sheets).save_as(file_path)
        else:
            pyexcel.save_as(array=rows, dest_file_name=file_path)
        return file_path

    def create_xls_file(self, form_id, file_field_id, header=[], rows_records=[], content_sheets={}, name_to_file=''):
        """
        Crea el registro de excel y lo sube a Linkaform en la forma y campo dado
        """
        if rows_records:
            rows_records = [header] + rows_records

        for row_record in rows_records:
            for c_idx, cell in enumerate(row_record):
                row_record[c_idx] = self.fix_encoding( self.clean_value(cell) )

        content_sheets = {
            sheet: [[self.fix_encoding( self.clean_value(cell) ) for cell in row] for row in rows]
            for sheet, rows in content_sheets.items()
        }

        archivo_file_name = self.make_excel_file(rows_records, content_sheets=content_sheets)
        csv_file = open(archivo_file_name, 'rb')
        csv_file_dir = {'File': csv_file}
        try:
            upload_url = self.lkf_api.post_upload_file(
                data={'form_id': form_id, 'field_id': file_field_id},
                up_file=csv_file_dir
            )
        except Exception:
            csv_file.close()
            return "No se pudo generar el archivo de resultado"

        csv_file.close()
        try:
            file_date = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
            return {
                file_field_id: [{
                    'file_name': f"{name_to_file}.xlsx" if name_to_file else f'Material estimado {file_date}.xlsx',
                    'file_url': upload_url['data']['file'],
                }]
            }
        except KeyError:
            return 'No se pudo guardar el archivo, favor de reprocesar'

    def set_status(self, status, msg=''):
        if self.front_request:
            return {status: msg}

        f = self.material_estimado_fields
        self.current_record['answers'][f['estatus']] = status
        self.current_record['answers'][f['mensaje']] = msg
        return self.lkf_api.patch_record(self.current_record, self.record_id)

    def id_user_to_int(self, val):
        if isinstance(val, int):
            return val
        return int( val.strip() )

    def get_all_products(self):
        """
        Consulta el catalogo de Productos/SKU y regresa un diccionario con clave = codigo de producto
        """
        f = self.catalogo_productos_fields
        records_products = self.lkf_api.search_catalog(self.CATALOG_ID_SKU, jwt_settings_key='APIKEY_JWT_KEY')
        return {
            str(r[f['codigo']]): {
                'sku': r.get(f['sku']),
                'nombre': self.unlist(r.get(f['nombre'])),
                'tipo_de_material': r.get(f['tipo_de_material']),
                'unidad_medida': self.unlist(r.get(f['unidad_medida'])),
                'relevancia': r.get(f['relevancia']),
            }
            for r in records_products
            if r.get(f['codigo'])
        }

    def get_tipos_tarea_aplica_material(self):
        """
        Consulta el catalogo de Tipos de Tarea y regresa solo los que aplican material/ONT/modem
        """
        f = self.catalogo_tipos_tarea_fields
        records_tipos_tarea = self.lkf_api.search_catalog(self.CATALOG_ID_TIPOS_TAREA, jwt_settings_key='JWT_ADMIN')

        dict_tipos_tarea_material = {}
        for r in records_tipos_tarea:
            aplica_bajante_cobre = r.get(f['aplica_bajante_cobre'])
            aplica_bajante_fibra = r.get(f['aplica_bajante_fibra'])
            aplica_ont = r.get(f['aplica_ont'])
            aplica_modem = r.get(f['aplica_modem'])

            if not any([aplica_bajante_cobre, aplica_bajante_fibra, aplica_ont, aplica_modem]):
                continue

            dict_tipos_tarea_material[r.get(f['tipo_de_tarea'])] = {
                'aplica_bajante_cobre': aplica_bajante_cobre or 'No',
                'aplica_bajante_fibra': aplica_bajante_fibra or 'No',
                'aplica_ont': aplica_ont or 'No',
                'aplica_modem': aplica_modem or 'No',
            }
        return dict_tipos_tarea_material

    def get_forms_by_tecnologia(self, tecnologia):
        if not tecnologia or len(tecnologia) > 1:
            return self.FORMS_ID_FTTH + self.FORMS_ID_COBRE
        str_tecnologia = self.unlist(tecnologia)
        return self.FORMS_ID_FTTH if str_tecnologia == 'fibra' else self.FORMS_ID_COBRE

    def get_records_orden_de_servicio(self, desde, hasta, tecnologia, conexiones=None, copes=None):
        print(f'... consultando OS desde {desde} hasta {hasta}')
        """
        Consulta los registros de Orden de Servicio dentro del periodo indicado y los agrupa por conexion.
        """
        f = self.orden_servicio_fields
        forms_os = self.get_forms_by_tecnologia(tecnologia)

        data_project = {
            'alfanumerico_contratista': '$answers.68e4619b219b1bd06a01a272',
            'alfanumerico_tac': {'$ifNull': ['$answers.f1054000a0200000000000a3', '$answers.f1054000a020000000000003']},
            'alfanumerico_tecnico': '$answers.68e5920a535224073205c2f3',
            'area': f'$answers.{f["area"]}',
            'connection_email': {
                '$ifNull': [
                    '$connection_email',
                    {'$ifNull': [f'$answers.{f["connection_email_1"]}', f'$answers.{f["connection_email_2"]}']}
                ]
            },
            'connection_id': '$connection_id', 
            'cope': f'$answers.{f["cope"]}',
            "distrito": {'$ifNull': ['$answers.f1054000a0100000000000d5', '$answers.f1054000a010000000000003']},
            'estatus_orden': '$answers.f1054000a030000000000002',
            'expediente': {'$ifNull': ['$answers.f1054000a0100000000000d6', '$answers.f1054000a010000000000007']},
            'fecha_carga_contratista': '$answers.5f0e23eaca2ca23aa12f21a9',
            'fecha_liquidacion': {'$ifNull': ['$answers.f1054000a02000000000fa02', '$answers.5a1eecfbb43fdd65914505a1']},
            'folio': '$folio', 
            'form_id': '$form_id', 
            'metros_bajante': {'$ifNull': ['$answers.f1054000a0200000000000d7', '$answers.f1054000a020000000000007']},
            'mts_adicionales': '$answers.f1054000a020000000000bd7',
            'record_id': '$_id',
            'tecnico': {'$ifNull': ['$answers.f1054000a02000000000fa04', '$answers.59e1280bb43fdd7cd6fc9f63']},
            'tecnico_pic': '$answers.5eb091915ae0d087df1163de',
            'telefono': '$answers.f1054000a010000000000005',
            # 'tipo_de_material': {'$ifNull': [f'$answers.{self.field_id_tipo_material}', 'telmex/condumex']},
            'tipo_de_material': 'telmex/condumex',
            'tipo_de_tarea': {'$ifNull': [f'$answers.{self.tipo_tarea_fields["ftth"]}', f'$answers.{self.tipo_tarea_fields["cobre"]}']},
            'tipo_instalacion': '$answers.f1054000a020000000000004'
        }
        data_push = {i_proj: f"${i_proj}" for i_proj in data_project}

        match_query = {
            'form_id': {'$in': forms_os},
            'deleted_at': {'$exists': False},
            # 'answers.633d9f63eb936fb6ec9bf580': {'$nin': ['degradado']}, # 20260821 Gil solicita que si salgan los degradados
            # 'answers.6a0be77a2b38ce3a6333e3fc': {'$nin': ['sí']}, # ¿Vale de Materiales? # 20260821 Gil solicita que salgan estos datos
            '$or': [
                {f'answers.{f["fecha_liquidacion_1"]}': {'$gte': f'{desde}', '$lte': f'{hasta} 23:59:59'}},
                {f'answers.{f["fecha_liquidacion_2"]}': {'$gte': f'{desde}', '$lte': f'{hasta} 23:59:59'}},
            ],
        }
        if conexiones:
            match_query['connection_id'] = {'$in': conexiones}
        if copes:
            match_query[f'answers.{f["cope"]}'] = {'$in': copes}

        # print('+++ +++ +++ match_query =',match_query)

        return self.cr_admin.aggregate([
            {'$match': match_query},
            {'$project': data_project},
            {'$group': {
                '_id': {'connection_email': '$connection_email'},
                'folios': {'$push': data_push},
            }},
        ], allowDiskUse=True)

    


    def get_contratista_by_expediente(self, expediente_os):
        """
        Consulta en la forma de Expedientes de Tecnicos y se devuelve un diccionario con el expediente y el id y email del contratista
        Args:
            expediente_os (int): Expediente de la OS
        Return:
            (None|str): Email del contratista
        """
        record_expediente = self.cr_admin.find_one({
            'form_id': self.FORM_ID_EXP_TECNICOS,
            'deleted_at': {'$exists': False},
            'answers.590a4761f851c20e60ac168c': 'activo',
            'answers.f1216500a010000000000001': int(expediente_os)
        },{
            'answers.5f344a0476c82e1bebc991d5.5f344a0476c82e1bebc991d8': 1, # Correo Electronico de la conexion
            f'answers.5f344a0476c82e1bebc991d5.{self.field_id_catalog_id_user}': 1, # ID de la conexion
            'folio': 1
        })
        if not record_expediente:
            return None, None
        
        correo_conexion = self.unlist( 
            record_expediente.get('answers', {}).get('5f344a0476c82e1bebc991d5', {}).get('5f344a0476c82e1bebc991d8') 
        )
        
        id_user_conexion = self.id_user_to_int( 
            self.unlist( 
                record_expediente.get('answers', {}).get('5f344a0476c82e1bebc991d5', {}).get(self.field_id_catalog_id_user) 
            ) 
        )
        
        return correo_conexion, id_user_conexion

    def es_expediente(self, valor):
        """
        Valida si el valor es un email, entero o contiene solo números
        Args:
            valor (str|int): Valor del expediente
        Return:
            bool: True si es entero o contiene solo números, False en cualquier otro caso
        """
        if isinstance(valor, int):
            return True
        if isinstance(valor, str):
            return valor.isdigit() and '@' not in valor
        return False

    def get_conexiones_orden_servicio(self, records_orden_servicio):
        """
        Agrupa las Ordenes de Servicio por Conexion / Area / Tecnologia / Location
        """
        grupo_conexiones, email_id_expediente = {}, {}
        folios_reclamados_y_expediente = {'reclamados': 0, 'por_expediente': 0}
        default_location = 'Almacen Fibra'

        for orden_servicio in records_orden_servicio:
            email_o_expediente = orden_servicio.get('_id', {}).get('connection_email')

            # Si el registro no tiene conexión se debe obtener a quien le pertenece el registro con el expediente
            if self.es_expediente(email_o_expediente):
                email_connection, id_connection = self.get_contratista_by_expediente( email_o_expediente )
                # 20260821 Gil solicita que salgan los folios aunque su expediente no exista en Exp de Tecnicos
                # if not email_connection:
                #     print(f'[ERROR] No se pudo encontrar el email para el expediente = {email_o_expediente}')
                #     continue
                email_id_expediente[email_connection] = id_connection
            else:
                email_connection = email_o_expediente

            grupo_conexiones.setdefault(email_connection, {})

            for orden in orden_servicio.get('folios', []):
                tecnologia_os = 'fibra' if orden['form_id'] in self.FORMS_ID_FTTH else 'cobre'

                if orden.get('connection_id'):
                    email_id_expediente[email_connection] = orden['connection_id']
                    folios_reclamados_y_expediente['reclamados'] += 1
                else:
                    folios_reclamados_y_expediente['por_expediente'] += 1

                grupo_conexiones[email_connection] \
                    .setdefault(orden.get('area'), {}) \
                    .setdefault(tecnologia_os, {}) \
                    .setdefault(default_location, []) \
                    .append(orden)

        return grupo_conexiones, email_id_expediente, folios_reclamados_y_expediente

    def apply_sort_to_products(self, list_productos, productos):
        for data_product in list_productos:
            data_product['relevancia'] = productos.get(data_product['clave_producto'], {}).get('relevancia') or 0
        return sorted(list_productos, key=lambda x: x['relevancia'])

    


    def get_all_contratistas_catalog(self):
        """
        Consulta el catalogo de Contratistas 1.0 se obtienen todos los registros y se regresa un diccionario
        con la información de cada contratista en el catalogo
        Return:
            (dict): Diccionario con el email, nombre y razon social de los contratistas en el catalogo
        """
        records_contratistas = self.lkf_api.search_catalog(59273, jwt_settings_key='JWT_ADMIN')
        return {
            self.id_user_to_int( r[ self.field_id_catalog_id_user ] ): {
                'nombre': r.get(self.field_id_catalog_nombre),
                'razon_social': r.get(self.field_id_catalog_razon_social),
                'email': r[ self.field_id_catalog_email ]
            }
            for r in records_contratistas
            if r.get(self.field_id_catalog_id_user)
        }
    
    def get_row_for_xls(self, folio_os, data_os, name_conexion, tipo_de_tarea, version=1):
        if version == 1:
            return [
                self.map_form_name.get( data_os.get('form_id'), '' ),
                folio_os, # Folio
                name_conexion, # Conexión
                tipo_de_tarea, # Tipo de Tarea
                data_os.get('telefono', ''), # Teléfono
                data_os.get('area', '').upper().replace('_', ' '), # AREA
                data_os.get('cope', '').upper().replace('_', ' '), # COPE
                data_os.get('distrito', ''),
                data_os.get('expediente', ''), # Expediente Del Tecnico
                data_os.get('tecnico_pic', ''), # Técnico PIC
                data_os.get('fecha_carga_contratista', ''), # Fecha de Carga Contratista
                data_os.get('tipo_de_material', ''), # Tipo de Material
                data_os.get('alfanumerico_tac', ''), # Alfanumérico TAC
                data_os.get('alfanumerico_contratista', ''), # Alfanumérico Contratista
                data_os.get('alfanumerico_tecnico', ''), # Alfanumérico Tecnico
                data_os.get('tipo_instalacion', ''), # Tipo de Instalacion
                data_os.get('metros_bajante', ''), # Metros Bajante
                data_os.get('mts_adicionales', ''), # Metraje Adicional
                data_os.get('fecha_liquidacion', ''), # Fecha Liquidada
                data_os.get('tecnico', ''), # Tecnico
                data_os.get('estatus_orden', ''), # Estatus de Orden
                'Calculado por Conexión' if data_os.get('connection_id') else 'Calculado por Expediente'
            ]
        elif version == 2:
            return [
                self.map_form_name.get( data_os.get('form_id'), '' ),
                folio_os, # Folio
                name_conexion, # Conexión
                tipo_de_tarea, # Tipo de Tarea
                data_os.get('area', '').upper().replace('_', ' '), # AREA
                data_os.get('cope', '').upper().replace('_', ' '), # COPE
                data_os.get('distrito'), # Distrito
                data_os.get('telefono', ''), # Teléfono
                data_os.get('tecnico_pic', ''), # Técnico PIC
                data_os.get('expediente', ''), # Expediente Del Tecnico
                data_os.get('tecnico_pic', ''), # Técnico PIC
                data_os.get('fecha_carga_contratista', ''), # Fecha de Carga Contratista
                data_os.get('alfanumerico_tac', ''), # Alfanumérico TAC
                data_os.get('alfanumerico_contratista', ''), # Alfanumérico Contratista
                data_os.get('alfanumerico_tecnico', ''), # Alfanumérico Tecnico
                data_os.get('metros_bajante', ''), # Metros Bajante
                data_os.get('fecha_liquidacion', ''), # Fecha Liquidada
                data_os.get('estatus_orden', ''), # Estatus de Orden
                'Calculado por Conexión' if data_os.get('connection_id') else 'Calculado por Expediente'
            ]

    def calcular_material_estimado(self, ordenes_de_servicio, productos, tipos_tarea_para_material, kits_products, nombre_conexion):
        """
        Recorre las ordenes de servicio de una Conexion y calcula el material estimado que le corresponde.
        A diferencia del script original, esta funcion NO crea ningun registro: solo regresa el calculo.
        """
        msgs_no_aplica, folios_aplicados_fibra, folios_aplicados_cobre = [], [], []

        folios_no_aplicados_fibra, folios_no_aplicados_cobre = [], []

        materiales_to_record = {}
        count_folios_metraje = {'fibra': 0, 'cobre': 0}
        areas, tecnologias = set(), set()

        for orden_servicio in ordenes_de_servicio:
            os_cobre = orden_servicio['form_id'] in self.FORMS_ID_COBRE
            folio_os = orden_servicio['folio']
            tipo_tarea = orden_servicio.get('tipo_de_tarea')
            tipo_os = 'cobre' if os_cobre else 'fibra'
            info_tipo_tarea_aplica = tipos_tarea_para_material.get(tipo_tarea)


            if info_tipo_tarea_aplica:
                aplica_material = info_tipo_tarea_aplica.get(f"aplica_bajante_{tipo_os}") == 'Sí'
                aplica_ont_modem = info_tipo_tarea_aplica.get('aplica_modem' if os_cobre else 'aplica_ont') == 'Sí'
                aplica = aplica_material or aplica_ont_modem
                no_aplica_por = f"Folio {folio_os} no aplica material"
            else:
                aplica = False
                no_aplica_por = f"Folio {folio_os} no aplica por Tipo de Tarea {tipo_tarea}"

            if not aplica:
                if os_cobre:
                    folios_no_aplicados_cobre.append( self.get_row_for_xls( folio_os, orden_servicio, nombre_conexion, tipo_tarea, version=2 ) )
                else:
                    folios_no_aplicados_fibra.append( self.get_row_for_xls(folio_os, orden_servicio, nombre_conexion, tipo_tarea) )
                msgs_no_aplica.append( no_aplica_por )
                continue
            
            tecnologias.add(tipo_os.upper())
            # Tipo de Material para fibra siempre será telmex/condumex
            tipo_material = orden_servicio.get('tipo_de_material') if os_cobre else 'telmex/condumex'


            list_productos = []
            if aplica_material:
                list_productos = deepcopy(kits_products.get(tipo_os, {}).get('kit', {}).get(tipo_material, []))
            if aplica_ont_modem:
                product_ont_modem = deepcopy(kits_products.get(tipo_os, {}).get('ont_/_modem', {}).get(tipo_material, []))
                if product_ont_modem:
                    list_productos.extend(product_ont_modem)

            list_products_sorted = self.apply_sort_to_products(list_productos, productos)
            areas.add(orden_servicio.get('area', ''))


            if os_cobre:
                folios_aplicados_cobre.append( self.get_row_for_xls( folio_os, orden_servicio, nombre_conexion, tipo_tarea, version=2 ) )
            else:
                folios_aplicados_fibra.append( self.get_row_for_xls(folio_os, orden_servicio, nombre_conexion, tipo_tarea) )

            for data_product in list_products_sorted:
                product = data_product['clave_producto']
                info_product = productos.get(product)
                cantidad_producto = data_product.get('cantidad') or 0

                unidad_medida = data_product.get('unidad_medida')
                if unidad_medida == "mts":
                    count_folios_metraje[tipo_os] += 1
                    if count_folios_metraje[tipo_os] < self.cant_minima_folios_para_metraje:
                        cantidad_producto = 0
                    elif count_folios_metraje[tipo_os] == self.cant_minima_folios_para_metraje:
                        cantidad_producto = cantidad_producto * self.cant_minima_folios_para_metraje

                materiales_to_record.setdefault(product, {
                    'nombre': info_product.get('nombre'),
                    'sku': info_product.get('sku'),
                    'unidad_medida': info_product.get('unidad_medida'),
                    'cantidad_estimada': 0,
                })
                materiales_to_record[product]['cantidad_estimada'] += cantidad_producto

        rows_materiales = []
        for prod_id, data_prod in materiales_to_record.items():

            if self.front_request:
                rows_materiales.append({
                    "nombre_contratista": nombre_conexion,
                    "area": self.list_to_str([a.upper().replace('_', ' ') for a in areas]),
                    "tecnologia": self.list_to_str(list(tecnologias)),
                    "id_producto": prod_id,
                    "sku": data_prod['sku'],
                    "name": data_prod['nombre'],
                    "unit": data_prod['unidad_medida'],
                    "suggestedQuantity": round(data_prod['cantidad_estimada'], 2),
                    # nsNeed: true,
                })
                continue

            rows_materiales.append([
                nombre_conexion,
                self.list_to_str([a.upper().replace('_', ' ') for a in areas]),
                self.list_to_str(list(tecnologias)),
                prod_id,
                data_prod['sku'],
                data_prod['nombre'],
                data_prod['unidad_medida'],
                round(data_prod['cantidad_estimada'], 2),
            ])

        return rows_materiales, folios_aplicados_fibra, folios_aplicados_cobre, folios_no_aplicados_fibra, folios_no_aplicados_cobre, msgs_no_aplica

    def make_xls(self, header_xls, rows_xls, field_xls, name_to_file=None, rows_fols_no_considerados=None):
        if not rows_xls:
            return

        if rows_fols_no_considerados:
            sheets_to_xls = {}
            for sheet_rows, sheet_name in [ (rows_xls, "Folios considerados"), (rows_fols_no_considerados, "NO Considerados") ]:
                if not sheet_rows:
                    continue

                sheets_to_xls[sheet_name] = [header_xls, *sheet_rows]

            response_xls_create = self.create_xls_file( self.form_id, field_xls, content_sheets=sheets_to_xls, name_to_file=name_to_file )
        else:
            response_xls_create = self.create_xls_file( self.form_id, field_xls, header=header_xls, rows_records=rows_xls, name_to_file=name_to_file )
        
        print('\n+++ +++ response_xls_create =',response_xls_create)
        self.current_record['answers'].update(response_xls_create)

    def consultar_material_estimado(self):
        """
        Punto de entrada: calcula el material estimado para el periodo indicado y adjunta
        el resultado como Excel al registro actual. No crea Vales ni cierra Ordenes de Servicio.
        """
        f = self.material_estimado_fields

        if self.front_request:
            desde = self.data.get('desde')
            hasta = self.data.get('hasta')
            tecnologia = self.data.get('tecnologia')
            wh_origen = self.data.get('almacen_origen')
        else:
            self.current_record['answers'].pop('6a032714b2194f0f517accc2', None)
            self.current_record['answers'].pop('6a83a116e0a44de46b0e9f08', None)
            self.set_status('procesando')
            desde = self.answers.get(f['desde'])
            hasta = self.answers.get(f['hasta'])
            tecnologia = self.answers.get(f['tecnologia'])
            wh_origen = None

        periodo_valido, response_periodo = self.validar_periodo(desde, hasta)
        if not periodo_valido:
            return self.set_status('error', response_periodo)

        desde = response_periodo.get('inicio')
        hasta = response_periodo.get('fin')

        dict_productos = self.get_all_products()
        tipos_tarea_para_material = self.get_tipos_tarea_aplica_material()
        kits_products = self.get_kits()

        # print('+++ +++ kits_products =',kits_products)
        # stop

        records_orden_servicio = self.get_records_orden_de_servicio(desde, hasta, tecnologia)
        # print('records_orden_servicio =',list(records_orden_servicio))
        # stop

        group_conexiones, dict_email_id_conexion, dict_folios_reclamados = self.get_conexiones_orden_servicio(records_orden_servicio)

        if not group_conexiones:
            return self.set_status('error', 'No se encontraron registros de orden de servicio con el periodo indicado')

        total_rows_materiales, total_no_aplican = [], []
        total_folios_considerados_fibra, total_folios_considerados_cobre = [], []
        total_folios_no_considerados_fibra, total_folios_no_considerados_cobre = [], []

        dict_contratistas = self.get_all_contratistas_catalog()

        for email_conexion, data_area in group_conexiones.items():
            id_conexion = dict_email_id_conexion.get(email_conexion)
            data_contratista = dict_contratistas.get(id_conexion, {})

            for area_to, data_tecnologia in data_area.items():
                for tecnologia_to, data_location in data_tecnologia.items():
                    for location_to, folios_os in data_location.items():
                        rows_materiales, folios_considerados_fibra, folios_considerados_cobre, no_aplican_fibra, no_aplican_cobre, msgs_no_aplica = self.calcular_material_estimado(
                            folios_os, dict_productos, tipos_tarea_para_material, kits_products, data_contratista.get('nombre', '')
                        )
                        total_rows_materiales.extend(rows_materiales)
                        
                        total_folios_considerados_fibra.extend(folios_considerados_fibra)
                        total_folios_considerados_cobre.extend(folios_considerados_cobre)

                        total_folios_no_considerados_fibra.extend(no_aplican_fibra)
                        total_folios_no_considerados_cobre.extend(no_aplican_cobre)

                        total_no_aplican.extend(msgs_no_aplica)

        if not self.front_request:
            self.make_xls(self.header_material, total_rows_materiales, self.field_id_file_estimacion)
            self.make_xls(
                self.header_xls_fibra, total_folios_considerados_fibra, 
                '6a032714b2194f0f517accc2', 'Órdenes de Servicio FTTH', total_folios_no_considerados_fibra
            )
            self.make_xls(
                self.header_xls_cobre, total_folios_considerados_cobre, 
                '6a83a116e0a44de46b0e9f08', 'Órdenes de Servicio COBRE', total_folios_no_considerados_cobre
            )

            self.current_record['answers']['6a062ac1fd516b4a6fd754a1'] = str( dict_folios_reclamados['reclamados'] + dict_folios_reclamados['por_expediente'] )
            self.current_record['answers']['6a062ac1fd516b4a6fd754a2'] = str( dict_folios_reclamados['reclamados'] )
            self.current_record['answers']['6a062ac1fd516b4a6fd754a3'] = str( dict_folios_reclamados['por_expediente'] )

            return self.set_status('terminado', self.list_to_str(total_no_aplican, separator='\n'))

        return total_rows_materiales

if __name__ == '__main__':
    script_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()

    response = script_obj.consultar_material_estimado()
    if not script_obj.current_record:
        script_obj.HttpResponse({"data": response})