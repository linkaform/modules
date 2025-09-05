# -*- coding: utf-8 -*-

import sys, simplejson, requests
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.jit.app import JIT
from lkf_addons.addons.stock.app import Stock




today = date.today()
year_week = int(today.strftime('%Y%W'))


class JIT(JIT, Stock):

    def __init__(self, settings, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        # self.load('Product')
        self.load('Product', **self.kwargs)
        self.load(module='Product', module_class='Warehouse', import_as='WH', **self.kwargs)

        self.BALANCEO_DE_INVENTARIOS = self.lkm.form_id('balanceo_de_inventarios','id')
        
        self.f.update({
            'cantidad_salida': '6442e4cc45983bf1778ec17d',
            'fecha_salida_multiple': '000000000000000000000111',
            'folio_sipre': '682f4a14ba348a104e5a399d',
            'grupo_productos_salida_multiple': '6442e4537775ce64ef72dd69',
            'lot_number_salida': '620a9ee0a449b98114f61d77',
            'peso':'68590322a36cc9e84ba64740',
            'product_code_salida': '61ef32bcdf0ec2ba73dec33d',
            'product_group': '6442e4537775ce64ef72dd69',
            'renglon_sipre': '6835d529be0b0618ec80d233',
            'status_salida_multiple': '6442e4537775ce64ef72dd6a',
            'sku_salida': '65dec64a3199f9a040829243',
            'wh_name': '6442e4831198daf81456f274',
            'wh_location': '65ac6fbc070b93e656bd7fbe',
            'wh_name_dest': '65bdc71b3e183f49761a33b9',
            'wh_location_dest': '65c12749cfed7d3a0e1a341b',
            'families_list': '68647f867ac81846e75a58e5',
            'estatus_balanceo': '5e32fbb498849f475cfbdca2',
            'stock_actual': '686c07da60e400ee128f6f43',
            'stock_en_transito': '686c07da60e400ee128f6f44',
            'compra_sugerida': '686c0804e38e5ab6338f6f83',
        })

    def ave_daily_demand(self, demanda_12_meses):
        demanda = 0
        conf_data = self.get_config()
        dias_laborales_consumo = conf_data.get('dias_laborales_consumo',360)
        factor_crecimiento_jit = conf_data.get('factor_crecimiento_jit')
        if demanda_12_meses:
            demanda = f"{demanda_12_meses/dias_laborales_consumo:.2f}"
        return demanda

    def borrar_historial(self, method):
        #TODO comprobar en la forma de inventory que se borra
        print('arranca borrar')
        forms = [
            self.DEMANDA_UTIMOS_12_MES,
            self.REGLAS_REORDEN,
            self.STOCK_INVENTORY_ADJUSTMENT_ID,
            self.FORM_INVENTORY_ID,
            self.PROCURMENT,
        ]
        self.cr.delete_many({
            'form_id':{'$in':forms},
            '$or': [
                {f"answers.{self.f['procurment_method']}": method},
                {f"answers.{self.f['procurment_method']}": {'$exists': False}},
            ]
        }) #    or _delete
        print('termine de borrar...')
        return True
        
    def update_procurmet(self, records, **kwargs):
        response = []
        for rec in records:
            response.append(self.lkf_api.patch_multi_record( answers = rec, form_id=self.PROCURMENT, record_id=[rec.get('_id'),]))
        return response

    def upsert_procurment(self, product_by_warehouse, method, **kwargs):
        target_family = self.answers.get(self.Product.PRODUCT_OBJ_ID, {}).get(self.Product.f['family'])
        response = {}
        for wh, create_records in product_by_warehouse.items():
            existing_records = self.get_procurments(warehouse=wh, procurment_method=method)
            update_records = []
            
            filtered_records = []
            seen_products = set()
            
            for product in create_records:
                if self.Product.SKU_OBJ_ID in product:
                    product_code = product[self.Product.SKU_OBJ_ID].get(self.f['product_code'])
                    sku = product[self.Product.SKU_OBJ_ID].get(self.f['sku'])
                    
                    key = f"{product_code}_{sku}_{wh}"
                    
                    if key in seen_products:
                        # print(f"Duplicado detectado y eliminado: {product_code} en {wh}")
                        continue
                    
                    if target_family:
                        family = product[self.Product.SKU_OBJ_ID].get(self.f['family'], [])
                        if isinstance(family, list) and target_family not in family:
                            continue
                        elif isinstance(family, str) and family != target_family:
                            continue
                    
                    filtered_records.append(product)
                    seen_products.add(key)
            
            create_records = filtered_records
            
            # existing_skus = [prod['sku'] for prod in existing_procurments]
            for product in create_records[:]:
                if self.Product.SKU_OBJ_ID in list(product.keys()):
                    product_code = product[self.Product.SKU_OBJ_ID].get(self.f['product_code'])
                    sku = product[self.Product.SKU_OBJ_ID].get(self.f['sku'])
                    for existing_record in existing_records:
                        if existing_record.get('product_code') == product_code and \
                            existing_record.get('sku') == sku and \
                            existing_record.get('procurment_method') == method:
                            product.update({'_id':existing_record.get('_id')})
                            update_records.append(product)
                            try:
                                create_records.remove(product)
                            except ValueError:
                                pass
                            break
            response = self.update_procurmet(update_records, **kwargs)
            response += self.create_procurment(create_records, **kwargs)

        return response

    # def get_rutas_transpaso(self):
    #     all_prod = self.Product.get_product_catalog()
    #     res = {}
    #     self.product_data = {}
    #     for p in all_prod:
    #         res[ p.get(self.Product.f['product_code'])] =  p.get(self.Product.f['sku_percontainer'])
    #         # self.product_data.update({sku:{
    #         #     'linea':p.get(self.Product.f['linea'])
    #         #     'familia':p.get(self.Product.f['product_category'])
    #         #     }})
    #     res = { p.get(self.Product.f['product_code']): p.get(self.Product.f['sku_percontainer']) for p in all_prod}
    #     return res

    def get_procurment_transfers(self,qty, product_code, sku, warehouse, location, uom=None, schedule_date=None, status='programmed'):
        self.set_rutas_transpaso()
        routes = self.ROUTE_RULES.get(product_code,{}).get(sku,{}).get(warehouse,{}).get(location)
        if routes:
            # try:
            if True:
                warehouse_from = list(routes.keys())[0]  # "WAREHOUSE"
                warehouse_location_from = list(routes[warehouse_from].keys())[0]  # "location"
                standar_pack = routes[warehouse_from][warehouse_location_from]['standar_pack']
            # except:
            #     warehouse_from = None
            #     warehouse_location_from = None
            #     standar_pack = 1
            return  {'warehouse': warehouse_from, 'warehouse_location':warehouse_location_from, 'standar_pack':standar_pack}
        return {}

    def model_procurment(self, qty, product_code, sku, warehouse, location, uom=None, schedule_date=None, \
        bom=None, status='programmed', procurment_method='buy'):
        answers = {}
        if procurment_method == 'transfer':
           tranfer_data = self.get_procurment_transfers(qty, product_code, sku, warehouse, location, uom=uom, schedule_date=schedule_date, status=status)
           answers[self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID] = {}
           if tranfer_data.get('warehouse'):
               answers[self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID][self.WH.f['warehouse_dest']] = tranfer_data['warehouse']
           if tranfer_data.get('warehouse_location'):
               answers[self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID][self.WH.f['warehouse_location_dest']] = tranfer_data['warehouse_location']
           standar_pack = tranfer_data.get('standar_pack', 1)
        else:
            standar_pack = self.ROUTE_RULES.get(str(product_code),{}).get(warehouse)
            if not standar_pack:
                standar_pack = 1

        config = self.get_config(*['uom'])

        if not schedule_date:
            schedule_date = self.today_str()
        if not location:
            location = self.get_warehouse_config('tipo_almacen', 'abastacimiento', 'warehouse_location')
        if not uom:
            uom = config.get('uom')
        answers[self.Product.SKU_OBJ_ID] = {}
        answers[self.Product.SKU_OBJ_ID][self.f['product_code']] = product_code
        answers[self.Product.SKU_OBJ_ID][self.f['sku']] = sku
        answers[self.UOM_OBJ_ID] = {}
        answers[self.UOM_OBJ_ID][self.f['uom']] = uom
        answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID] = {}
        answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID][self.WH.f['warehouse']] = warehouse
        answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID][self.WH.f['warehouse_location']] = location
        answers[self.mf['procurment_date']] = self.today_str()
        answers[self.mf['procurment_method']] = procurment_method
        answers[self.mf['procurment_qty']] = self.calc_shipment_pack(qty, standar_pack)
        answers[self.mf['procurment_status']] = status
        answers[self.mf['procurment_schedule_date']] = schedule_date
        return answers
    
    def model_procurment_without_qty(self, product_code, sku, warehouse, location, uom=None, schedule_date=None, \
        bom=None, status='programmed', procurment_method='buy'):
        answers = {}
        if procurment_method == 'transfer':
           tranfer_data = self.get_procurment_transfers(0, product_code, sku, warehouse, location, uom=uom, schedule_date=schedule_date, status=status)
           answers[self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID] = {}
           if tranfer_data.get('warehouse'):
               answers[self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID][self.WH.f['warehouse_dest']] = tranfer_data['warehouse']
           if tranfer_data.get('warehouse_location'):
               answers[self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID][self.WH.f['warehouse_location_dest']] = tranfer_data['warehouse_location']
           standar_pack = tranfer_data.get('standar_pack', 1)

        config = self.get_config(*['uom'])

        if not schedule_date:
            schedule_date = self.today_str()
        if not location:
            location = self.get_warehouse_config('tipo_almacen', 'abastacimiento', 'warehouse_location')
        if not uom:
            uom = config.get('uom')
        answers[self.Product.SKU_OBJ_ID] = {}
        answers[self.Product.SKU_OBJ_ID][self.f['product_code']] = product_code
        answers[self.Product.SKU_OBJ_ID][self.f['sku']] = sku
        answers[self.UOM_OBJ_ID] = {}
        answers[self.UOM_OBJ_ID][self.f['uom']] = uom
        answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID] = {}
        answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID][self.WH.f['warehouse']] = warehouse
        answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID][self.WH.f['warehouse_location']] = location
        answers[self.mf['procurment_date']] = self.today_str()
        answers[self.mf['procurment_method']] = procurment_method
        answers[self.mf['procurment_qty']] = 0
        answers[self.mf['procurment_status']] = status
        answers[self.mf['procurment_schedule_date']] = schedule_date
        return answers

    def balance_warehouse(self, warehouse=None, location=None, product_code=None, sku=None, status='active', method='transfer'):
        product_rules = self.get_reorder_rules(
            warehouse=warehouse, 
            location=location, 
            product_code=product_code, 
            sku=sku, 
            status=status,
            method=method)
        res = []

        product_by_warehouse = {}
        product_codes = [r['product_code'] for r in  product_rules if r.get('product_code')]
        self.set_rutas_transpaso()
        for rule in product_rules:
            product_code = rule.get('product_code')
            sku = rule.get('sku')
            warehouse = rule.get('warehouse')
            product_by_warehouse[warehouse] = product_by_warehouse.get(warehouse,[])
            location = rule.get('warehouse_location')
            #product_stock = self.Stock.get_product_stock(product_code, sku=sku,  warehouse=warehouse, location=location)
            if method == 'buy':
                product_stock_buy = Stock.get_products_inventory(self, product_code, status='active')
                product_stock = {}
                actuals = 0.0
                for prod in product_stock_buy:
                    actuals += prod.get('actuals', 0.0)
                    product_stock.update({
                        'product_code': prod.get('product_code'),
                        'family': prod.get('family', ''),
                        'nombre_producto': prod.get('nombre_producto', ''),
                        'categoria': prod.get('categoria', ''),
                        'actuals': actuals,
                    })
                product_stock = [product_stock]
            else:            
                # TODO: Refactorizar codigo para evitar repeticion
                product_stock_transfer = Stock.get_products_inventory(self, product_code, warehouse, location, status='active')
                product_stock = {}
                actuals = 0.0
                for prod in product_stock_transfer:
                    actuals += prod.get('actuals', 0.0)
                    product_stock.update({
                        'product_code': prod.get('product_code'),
                        'family': prod.get('family', ''),
                        'nombre_producto': prod.get('nombre_producto', ''),
                        'categoria': prod.get('categoria', ''),
                        'actuals': actuals,
                    })
                product_stock = [product_stock]
            #product_stock = {'actuals':0}
            if isinstance(product_stock, list) and len(product_stock):
                product_stock = product_stock[0]
            else:
                product_stock = {}
            order_qty = self.exec_reorder_rules(rule, product_stock)
            #! ===============
            if order_qty:
                ans = self.model_procurment(order_qty, product_code, sku, warehouse, location, procurment_method=method)
            else:
                ans = self.model_procurment_without_qty(product_code, sku, warehouse, location, procurment_method=method)
            stock_en_transito = 0.0 #! Obtener stock en transito
            family = product_stock.get('family', '')
            nombre_producto = product_stock.get('nombre_producto', '')
            categoria = product_stock.get('categoria', '')
            actuals = product_stock.get('actuals', 0.0)
            max_stock = rule.get('max_stock', 0.0)
            min_stock = rule.get('min_stock', 0.0)
            compra_sugerida = round(max_stock - (actuals + stock_en_transito))
            ans[self.Product.SKU_OBJ_ID].update({
                self.f['family']: [family],
                self.f['product_name']: [nombre_producto],
                self.f['product_category']: [categoria],
            })
            ans.update({
                self.f['min_stock']: min_stock,
                self.f['max_stock']: max_stock,
                self.f['stock_actual']: actuals,
                self.f['stock_en_transito']: stock_en_transito,
                self.f['compra_sugerida']: compra_sugerida
            })
            product_by_warehouse[warehouse].append(ans)
        #! ==============
        response = self.upsert_procurment(product_by_warehouse, method=method)
        return response

class SIPRE:

    def __init__(self):
        self.host = "http://162.215.128.43:808/api/"
        self.token_endpoint = "AuthResponse/GetToken"
        self.stock_endpoint = "WhiReStock/Resumen/{}/{}"
        self.create_xfer_endpoint = "WhiTransfer/Crear"
        self.user = "katusak"
        self.passcode = "8572"

    def api_request(self, url, data={}, method='POST', content_type='application/json'):
        headers = {
            "accept": "*/*",
            "Content-Type": content_type
        }
        if hasattr(self, 'token'):
            headers.update({'Authorization': f"Bearer {self.token}"})
        try:
            if method =='POST':
                response = requests.post(url, json=data, headers=headers)
            elif method == 'GET':
                response = requests.get(url, headers=headers)
            
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            return response.json()  # Assuming the response is JSON
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

    def create_xfer_spire(self, warehouse_source, warehouse_target, product_code, quantity):
        if not hasattr(self, 'token'):
            self.get_token()
        endpoint = self.create_xfer_endpoint
        data = {
            "warehouseTarget": warehouse_target,
            "warehouseSource": warehouse_source,
            "productCode": product_code,
            "quantity": quantity,
            "token": self.token
        }
        response = self.api_request(f"{self.host}{endpoint}", data=data, method='POST')
        print('data=', simplejson.dumps(data, indent=3))
        print('response=', simplejson.dumps(response, indent=3))
        if response.get('estatusCode') == 500:
            raise self.LKFException(response.get('mensaje', "Error al crear el transpaso"))
        result = response.get('resultado',{})
        return result

    def get_config(self, *args, **kwargs):
        if not self.GET_CONFIG:
            # print(dddd)
            match_query ={ 
                 'form_id': self.CONFIGURACIONES,  
                 'deleted_at' : {'$exists':False},
            } 
            if 'query' in kwargs:
                match_query.update(kwargs['query'])
            project_ids = self._project_format(self.config_fields)

            aggregate = [
                {'$match': match_query},
                {'$limit':kwargs.get('limit',1)},
                {'$project': project_ids },
                ]
            self.GET_CONFIG =  self.format_cr(self.cr.aggregate(aggregate) )
        result = {}
        for res in self.GET_CONFIG:
            args = args or list(self.config_fields.keys())
            result = {arg:res[arg] for arg in args if res.get(arg)}
        return result if result else None

    def get_stock_and_demand(self, familia):
        if not hasattr(self, 'token'):
            self.get_token()
        endpoint = self.stock_endpoint.format(self.token, familia)
        response = self.api_request(f"{self.host}{endpoint}", method='GET')
        result = response.get('resultado',{})
        return result

    def get_token(self):
        data = {
              "userCode": self.user,
              "passcode": self.passcode
            }
        response = self.api_request(f"{self.host}{self.token_endpoint}", data)
        result = response.get('resultado',{})
        self.token = result.get('token')
        return True

