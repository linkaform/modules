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
from copy import deepcopy

from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

from account_settings import *

from accesos_utils import Accesos


class Accesos(Accesos):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        # form_id / catalog_id (iguales a crear_vales_de_materiales.py)
        self.FORM_ID_KITS = 164626 # PREPROD
        self.CATALOG_ID_SKU = 133015 # Este ya está OK en Prod y Preprod.
        self.CATALOG_ID_TIPOS_TAREA = 56269 # Este se va a consultar la info que está en Admin.
        self.FORMS_ID_FTTH = [11044, 16343, 21954, 21953, 147977]
        self.FORMS_ID_COBRE = [10540, 25927, 25928, 25929, 147978]

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
        self.sku_catalog_obj_id = '6824e631aa87c4ed5f18bcaa' # PREPROD
        self.kits_fields = {
            'tecnologia': '68819db58b1b1ab2e1d5ef2b',
            'tipo': '68819e548b1b1ab2e1d5ef31',
            'tipo_material': '68819e1b9e38c6f0be9066c3',
            'grupo_productos': '68819f06fd736f4a03d5eece',
            'cantidad': '68819f7b5bf2a425981fef5b',
            'unidad_medida_obj': '66c659483abf1c1605559450',
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

        content_sheets = {
            sheet: [[self.clean_value(cell) for cell in row] for row in rows]
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
                    'file_name': name_to_file if name_to_file else f'Material estimado {file_date}.xlsx',
                    'file_url': upload_url['data']['file'],
                }]
            }
        except KeyError:
            return 'No se pudo guardar el archivo, favor de reprocesar'

    def set_status(self, status, msg=''):
        f = self.material_estimado_fields
        self.current_record['answers'][f['estatus']] = status
        self.current_record['answers'][f['mensaje']] = msg
        return self.lkf_api.patch_record(self.current_record, self.record_id)

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
        """
        Consulta los registros de Orden de Servicio dentro del periodo indicado y los agrupa por conexion.
        """
        f = self.orden_servicio_fields
        forms_os = self.get_forms_by_tecnologia(tecnologia)

        data_project = {
            'folio': '$folio',
            'form_id': '$form_id',
            'connection_id': '$connection_id',
            'connection_email': {
                '$ifNull': [
                    '$connection_email',
                    {'$ifNull': [f'$answers.{f["connection_email_1"]}', f'$answers.{f["connection_email_2"]}']}
                ]
            },
            'area': f'$answers.{f["area"]}',
            'cope': f'$answers.{f["cope"]}',
            'tipo_de_tarea': {'$ifNull': [f'$answers.{self.tipo_tarea_fields["ftth"]}', f'$answers.{self.tipo_tarea_fields["cobre"]}']},
            'tipo_de_material': {'$ifNull': [f'$answers.{self.field_id_tipo_material}', 'telmex/condumex']},
            'record_id': '$_id',
        }
        data_push = {i_proj: f"${i_proj}" for i_proj in data_project}

        match_query = {
            'form_id': {'$in': forms_os},
            'deleted_at': {'$exists': False},
            '$or': [
                {f'answers.{f["fecha_liquidacion_1"]}': {'$gte': f'{desde}', '$lte': f'{hasta} 23:59:59'}},
                {f'answers.{f["fecha_liquidacion_2"]}': {'$gte': f'{desde}', '$lte': f'{hasta} 23:59:59'}},
            ],
        }
        if conexiones:
            match_query['connection_id'] = {'$in': conexiones}
        if copes:
            match_query[f'answers.{f["cope"]}'] = {'$in': copes}

        return self.cr.aggregate([
            {'$match': match_query},
            {'$project': data_project},
            {'$group': {
                '_id': {'connection_email': '$connection_email'},
                'folios': {'$push': data_push},
            }},
        ])

    def get_conexiones_orden_servicio(self, records_orden_servicio):
        """
        Agrupa las Ordenes de Servicio por Conexion / Area / Tecnologia / Location
        """
        grupo_conexiones = {}
        default_location = 'Almacen'

        for orden_servicio in records_orden_servicio:
            email_connection = orden_servicio.get('_id', {}).get('connection_email')
            grupo_conexiones.setdefault(email_connection, {})

            for orden in orden_servicio.get('folios', []):
                tecnologia_os = 'fibra' if orden['form_id'] in self.FORMS_ID_FTTH else 'cobre'
                grupo_conexiones[email_connection] \
                    .setdefault(orden.get('area'), {}) \
                    .setdefault(tecnologia_os, {}) \
                    .setdefault(default_location, []) \
                    .append(orden)

        return grupo_conexiones

    def apply_sort_to_products(self, list_productos, productos):
        for data_product in list_productos:
            data_product['relevancia'] = productos.get(data_product['clave_producto'], {}).get('relevancia', 0)
        return sorted(list_productos, key=lambda x: x['relevancia'])

    def calcular_material_estimado(self, ordenes_de_servicio, productos, tipos_tarea_para_material, kits_products, nombre_conexion):
        """
        Recorre las ordenes de servicio de una Conexion y calcula el material estimado que le corresponde.
        A diferencia del script original, esta funcion NO crea ningun registro: solo regresa el calculo.
        """
        no_aplica_por_tipo_tarea = []
        materiales_to_record = {}
        count_folios_metraje = {'fibra': 0, 'cobre': 0}
        areas, tecnologias = set(), set()

        for orden_servicio in ordenes_de_servicio:
            os_cobre = orden_servicio['form_id'] in self.FORMS_ID_COBRE
            folio_os = orden_servicio['folio']
            tipo_tarea = orden_servicio.get('tipo_de_tarea')

            if tipo_tarea not in tipos_tarea_para_material:
                no_aplica_por_tipo_tarea.append(f"Folio {folio_os} no aplica por Tipo de Tarea {tipo_tarea}")
                continue

            tipo_os = 'cobre' if os_cobre else 'fibra'
            tecnologias.add(tipo_os.upper())

            info_tipo_tarea_aplica = tipos_tarea_para_material.get(tipo_tarea)
            aplica_material = info_tipo_tarea_aplica.get(f"aplica_bajante_{tipo_os}") == 'Sí'
            aplica_ont_modem = info_tipo_tarea_aplica.get('aplica_modem' if os_cobre else 'aplica_ont') == 'Sí'

            if not aplica_material and not aplica_ont_modem:
                no_aplica_por_tipo_tarea.append(f"Folio {folio_os} no aplica material")
                continue

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

            for data_product in list_products_sorted:
                product = data_product['clave_producto']
                info_product = productos.get(product)
                cantidad_producto = data_product.get('cantidad') or 0

                unidad_medida = data_product['unidad_medida']
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

        return rows_materiales, no_aplica_por_tipo_tarea

    def consultar_material_estimado(self):
        """
        Punto de entrada: calcula el material estimado para el periodo indicado y adjunta
        el resultado como Excel al registro actual. No crea Vales ni cierra Ordenes de Servicio.
        """
        self.set_status('procesando')

        f = self.material_estimado_fields
        desde = self.answers.get(f['desde'])
        hasta = self.answers.get(f['hasta'])
        tecnologia = self.answers.get(f['tecnologia'])

        dict_productos = self.get_all_products()
        tipos_tarea_para_material = self.get_tipos_tarea_aplica_material()
        

        kits_products = self.get_kits()
        print('kits_products =',kits_products)
        stop

        records_orden_servicio = self.get_records_orden_de_servicio(desde, hasta, tecnologia)
        group_conexiones = self.get_conexiones_orden_servicio(records_orden_servicio)

        if not group_conexiones:
            return self.set_status('error', 'No se encontraron registros de orden de servicio con el periodo indicado')

        total_rows_materiales, total_no_aplican = [], []

        for email_conexion, data_area in group_conexiones.items():
            for area_to, data_tecnologia in data_area.items():
                for tecnologia_to, data_location in data_tecnologia.items():
                    for location_to, folios_os in data_location.items():
                        rows_materiales, no_aplican = self.calcular_material_estimado(
                            folios_os, dict_productos, tipos_tarea_para_material, kits_products, email_conexion
                        )
                        total_rows_materiales.extend(rows_materiales)
                        total_no_aplican.extend(no_aplican)

        if total_rows_materiales:
            header_material = [
                'Conexión', 'Area', 'Tecnología', 'Código de Producto', 'SKU', 'Nombre del producto',
                'Unidad de Medida', 'Cantidad estimada'
            ]
            resp_xls = self.create_xls_file(
                self.form_id, self.field_id_file_estimacion, header=header_material, rows_records=total_rows_materiales
            )
            self.current_record['answers'].update(resp_xls)

        return self.set_status('terminado', self.list_to_str(total_no_aplican, separator='\n'))


if __name__ == '__main__':
    script_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()

    script_obj.consultar_material_estimado()
