# -*- coding: utf-8 -*-
import sys, simplejson, re
import os, time
from linkaform_api import settings
from account_settings import *
from datetime import datetime
#from linkaform_api.lkf_object import LKFBaseObject

from lkf_addons.addons.base.app import CargaUniversal, Base
# from lkf_addons.addons.stock.app import Stock 
# from lkf_addons.addons.stock.app import Stock
# from lkf_addons.addons.jit.app import JIT
from stock_utils import Stock 
from jit_utils import JIT, SIPRE
from bson import ObjectId



wh_dict_loc = {'ALM GUADALAJARA':'CEDIS GUADALAJARA'}
NOMBRE_UBICACIONES = {
    'ALM MONTERREY':'Almacen Monterrey',
    'ALM MERIDA':'Almacen Merida',
    'ALM GUADALAJARA':'Almacen Guadalajara',
    }


FAMILIAS = {
    'PENDIENTE':'00',
    'TUBOS':'01',
    'BARBETTI':'02',
    'CONSA':'03',
    'COPLEACERO':'04',
    'COBRE':'05',
    'INDEL':'06',
    'LA COLMENA':'07',
    'REYCO':'08',
    'NIPLES':'09',
    'MILLER':'10',
    'BLUE LINE':'11',
    'TEXAS':'12',
    'CABLE':'13',
    'GOLDEN (GUANTE)':'14',
    'USALOK':'15',
    'GENERAL FORGE':'16',
    'PROFLOW':'17',
    'SANITARIO':'18',
    'PROMOCIONALES':'19',
    'MANGUERAS':'20',
    'AUTOMATIZACIONES':'21',
}


class CargaUniversal(CargaUniversal):
    

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.carga_catalogs = {}

    def clean_records(self, records):
        res = []
        for idx, row in enumerate(records[:]):
            for r in row:
                if r:
                    res.append(row)
                    break
        return res

    def carga_stock_from_sipre(self, procurment_method):
        #   Se utiliza para el registro de errores
        # self.field_id_error_records = '5e32fbb498849f475cfbdca3'
        
        #   Obtiene una info de un formulario en específico que viene de metadata
        metadata_stock = self.lkf_api.get_metadata(form_id=class_obj.Stock.FORM_INVENTORY_ID)
        metadata_sales = self.lkf_api.get_metadata(form_id=jit_obj.DEMANDA_UTIMOS_12_MES)
        metadata_stock.update(self.get_complete_metadata())
        metadata_sales.update(self.get_complete_metadata())
        # Necesito un diccionario que agrupe los registros que se crearán y los que están en un grupo repetitivo y pertenecen a uno principal
        # Agrego información de la carga, se vuelve a actualizar metadata_form, agregando el campo properties
        properites = {'properties': 
            {"device_properties":{
                "system": "SCRIPT",
                "process":"Carga Universal", 
                "accion":'CREA Y ACTUALIZA REGISTROS DE CUALQUIER FORMA', 
                "folio carga":self.folio, 
                "archive":"carga_documentos_a_forma.py"
                }
            }
            }
        metadata_stock.update(properites)
        metadata_sales.update(properites)
        
        resultado = {'creados':0,'error':0,'actualizados':0, 'no_update':0}
        total_rows = len(sipre_obj.stock)   #   Longitud total de registros del diccionario "stock"
        print('total_rows=', total_rows)
        #   Manejo del subgrupo de errores
        subgrupo_errors = []
        
        #   Se crea el diccionario donde se almacenaran los registros creados o actualizados
        dict_records_copy = {'create': [], 'update': {}}
        upload_records = [] #   aquí se almacenan los registros procesados y listos para cargar
        upload_sales_records = [] #   aquí se almacenan los registros procesados y listos para cargar
        
        #   Se recorre el numero de registros, se asigna un folio y se agrega a metada_form
        today_date = datetime.today()
        for_folio = f'{today_date.month}{today_date.day}{today_date.second}'
        for p, record in enumerate(sipre_obj.stock):
            # if p > 3:
            #     continue
            # print('record=', record)
            this_metadata_stock = metadata_stock.copy() #   copia de metafa_form
            this_metadata_sales = metadata_sales.copy() #   copia de metafa_form
            this_metadata_stock.update({'folio': f"{self.folio}-{for_folio}-{p}"})
            this_metadata_sales.update({'folio': f"{self.folio}-{for_folio}-{p}"})
            answers_stock = {}
            answers_sales = {}
            
            if p in subgrupo_errors:
                error_records.append(record+['',])
                continue
            #proceso = self.crea_actualiza_record(metadata, self.existing_records, error_records, records, sets_in_row, dict_records_to_multi, dict_records_copy, self.ids_fields_no_update)

            #   Se manda a llamar procesa_row para obtener los datos, se actualiza metada agregando el diccionario answers con todos los valores
            # answers = self.procesa_row(pos_field_dict, record, files_dir, nueva_ruta, id_forma_seleccionada, answers, p, dict_catalogs)
            answers_stock = self.transform_stock_answers(record)
            answers_sales = self.transform_sales_answers(record, procurment_method)
            this_metadata_stock.update({"answers":answers_stock})
            this_metadata_sales.update({"answers":answers_sales})
            upload_records.append(this_metadata_stock)
            upload_sales_records.append(this_metadata_sales)

        #   Inserta el documento completo a mongodb
        ids = []
        stock_ids = []
        sale_ids = []
        if upload_records:
            res = self.cr.insert_many(upload_records)
            try:
                inserted_ids = res.inserted_ids
                stock_ids = [str(ids) for ids in inserted_ids]
            except:
                print('no econtro ids')
        if upload_sales_records:
            res = self.cr.insert_many(upload_sales_records)
            try:
                inserted_ids = res.inserted_ids
                sale_ids = [str(ids) for ids in inserted_ids]
            except:
                print('no econtro ids')
            # Vamos a probar sin editar ya que estamos calculando en promedio direct desde el insert            
            # res = jit_obj.lkf_api.patch_multi_record(
            #     answers={jit_obj.f['comments']:'update'}, 
            #     form_id=jit_obj.DEMANDA_UTIMOS_12_MES, 
            #     record_id=sale_ids,
            #     threading=False,
            #     )
        return stock_ids, sale_ids

    def get_complete_metadata(self):
        now = datetime.now()
        format_date = int(now.timestamp())  # Converting to timestamp
        
        fields = {}
        fields['user_id'] = self.user['user_id']
        fields['user_name'] = self.user['username']
        fields['assets'] = {
            "template_conf" : None,
            "followers" : [ 
                {
                    "asset_id" : self.user['user_id'],
                    "email" : self.user['email'],
                    "name" : "Industrias MIller",
                    "username" : None,
                    "rtype" : "user",
                    "rules" : None
                }
            ],
            "supervisors" : [],
            "performers" : [],
            "groups" : []
        }
        fields['created_at'] = now
        fields['updated_at'] = now
        fields['editable'] = True
        fields['start_timestamp'] = time.time()
        fields['end_timestamp'] = time.time()
        fields['start_date'] = now
        fields['end_date'] = now
        fields['duration'] = 0
        fields['created_by_id'] = self.user['user_id']
        fields['created_by_name'] = self.user['username']
        fields['created_by_email'] = "linkaform@industriasmiller.com"
        fields['timezone'] = "America/Monterrey"
        fields['tz_offset'] = -360
        fields['other_versions'] = []
        fields['voucher_id'] = ObjectId('672409493680d9f01f30961f')        
    
        return fields
    
    def transform_sales_answers(self, record, procurment_method):
        answers = {}
        answers = {
            jit_obj.f['fecha_demanda']: self.today_str(),
            self.Stock.WH.WAREHOUSE_OBJ_ID:{
                self.Stock.WH.f['warehouse']: record.get('almacenNombre'),
            },
            self.Stock.Product.SKU_OBJ_ID:{
                self.Stock.Product.f['product_code']: record.get('producto'),
                self.Stock.Product.f['product_sku']: record.get('producto'),
                self.Stock.Product.f['product_name']: [record.get('productoNombre'),],
                self.Stock.Product.f['family']: [record.get('familiaProducto'),]
            },
            jit_obj.f['demanda_12_meses']: record.get('ventas'),
            jit_obj.mf['consumo_promedio_diario']: jit_obj.ave_daily_demand(record.get('ventas')),
            jit_obj.f['procurment_method']: procurment_method
        }
        return answers

    def transform_stock_answers(self, record):
        answers = {}
        answers = {
            self.Stock.WH.WAREHOUSE_LOCATION_OBJ_ID:{
                self.Stock.WH.f['warehouse']: record.get('almacenNombre'),
                self.Stock.WH.f['warehouse_location']: NOMBRE_UBICACIONES[record.get('almacenNombre')],
            },
            self.Stock.Product.SKU_OBJ_ID:{
                self.Stock.Product.f['product_code']: record.get('producto'),
                self.Stock.Product.f['product_sku']: record.get('producto'),
                self.Stock.Product.f['product_name']: record.get('productoNombre'),
                self.Stock.Product.f['product_category']: record.get('lineaProducto'),
                self.Stock.Product.f['product_type']: record.get('familiaProducto'),
            },
            self.Stock.f['actual_eaches_on_hand']: record.get('inventario') + record.get('pedidoTransito', 0.0)

        }
        return answers
    

if __name__ == '__main__':
    class_obj = CargaUniversal(settings=settings, sys_argv=sys.argv, use_api=True)
    class_obj.console_run()
    class_obj.load('Stock', **class_obj.kwargs)
    jit_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    step = class_obj.data.get('step')
    option = class_obj.data.get('option', 'transfer')
    #step = 'carga_stock'
    # step = 'carga_stock'
    # for step in ['demanda']:
    #for step in [ 'carga_stock']:
    estatus = 'demanda_cargada'
    #for step in ['carga_stock']:
    sipre_obj = SIPRE()
    ans_familia = class_obj.answers.get(jit_obj.Product.PRODUCT_OBJ_ID,{}).get(jit_obj.Product.f['product_type'])
    ans_familia = ans_familia.upper().replace('_',' ')
    familia = FAMILIAS.get(ans_familia)
    if not familia:
        class_obj.LKFException('Familia {ans_familia} no econtrada')
    if jit_obj.answers.get(jit_obj.f['borrar_historial']) == 'si':
        jit_obj.borrar_historial(method=option)
    sipre_obj.stock = sipre_obj.get_stock_and_demand(familia)
    stock = class_obj.carga_stock_from_sipre(procurment_method=option)
    res = class_obj.update_status_record(estatus)

