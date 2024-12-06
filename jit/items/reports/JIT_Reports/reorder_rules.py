# -*- coding: utf-8 -*-
import sys, simplejson
from math import floor
import json
from datetime import timedelta, datetime

from linkaform_api import settings, base
from account_settings import *

from jit_report import Reports
from lkf_addons.addons.product.app import Product
from lkf_addons.addons.product.app import Warehouse
from lkf_addons.addons.jit.app import JIT
from lkf_addons.addons.stock.app import Stock
from itertools import zip_longest


class Reports(Reports):

    def almancenes_por_porcentaje_set(self, pos, standar_pack, stock_cedis, update_perc=.01):
        stock_max = self.get_stock_data(self.warehouse_percentage, pos)
        stock_qty = self.piezas_porcentaje(stock_max, update_perc)
        stock_qty = round(self.calc_shipment_pack(stock_qty, standar_pack), 2)
        stock_cedis -= stock_qty
        if stock_cedis < 0 and stock_qty > standar_pack:
            stock_cedis += stock_qty
            n_stock_cedis, n_stock_qty = self.almancenes_por_porcentaje_set(pos, standar_pack, stock_cedis, update_perc=update_perc/2)
            if n_stock_qty > n_stock_cedis:
                if n_stock_cedis > 0:
                    return n_stock_cedis, n_stock_qty
                stock_cedis -= stock_qty
                return stock_cedis, 0
            # else:
            #     # stock_qty = 0
            #     stock_cedis = -1
        return stock_cedis, stock_qty

    def send_next_product(self, data):
        m = []
        almancenes_max={}
        for w, v in data.items():
            if not isinstance(v, dict):
                continue
            amax = v['stock_max']
            m.append( amax)
            almancenes_max[amax] = v
            m.sort()
            m.reverse()
        m.sort()
        m.reverse()
        return m, almancenes_max

    def set_warehouse_tranfer_order(self, m, almancenes_max):
        p = []
        for wmax in m:
            v = almancenes_max[wmax]
            if not isinstance(v, dict):
                continue
            w = v['almacen']
            porce = v['porce']
            if porce in p:
                porce = p[-1] + 1
            p.append(porce)
            self.warehouse_percentage[porce] = v
            #sku_warehouses.append(w)
        p.sort()
        return p

    def almancenes_por_porcentaje(self, sku_data):
        self.warehouse_percentage = {}
        for sku, data in sku_data.items():
            standar_pack = self.ROUTE_RULES.get(str(sku),{})
            sku_warehouses = []
            m , almancenes_max = self.send_next_product(data)
            if data['stock_final'] >= 0:
                for wmax in m:
                    w = almancenes_max[wmax]['almacen']
                    sku_data[sku][w]['traspaso'] = sku_data[sku][w]['stock_to_move']
                continue
            stock_cedis = data['actuals']
            #TODO PONER EL VALOR CORRECTO....
            full = False
            if data['actuals'] == 0:
               data['stock_final'] = 0
            while_warning = {}
            while stock_cedis > 0:
                m , almancenes_max = self.send_next_product(data)
                p = self.set_warehouse_tranfer_order(m, almancenes_max)
                if not p:
                    break
                for idx, f in enumerate(p):
                    if (idx+1) >= len(p):
                        stock_cedis = -1
                        break
                    warehouse = self.warehouse_percentage[f]['almacen']
                    while_warning[warehouse] = while_warning.get(warehouse,[])
                    if stock_cedis in while_warning[warehouse]:
                        stock_cedis = -1
                    else:
                        while_warning[warehouse].append(stock_cedis)
                    porce = self.warehouse_percentage[f]['porce']
                    if warehouse not in sku_warehouses:
                        sku_warehouses.append(warehouse)
                    if porce < 100:
                        perc_difference = (p[idx+1] - p[idx])/100
                        stock_cedis, stock_qty = self.almancenes_por_porcentaje_set(f, standar_pack, stock_cedis, update_perc=perc_difference)
                        if stock_cedis <= 0:
                            break
                        sku_data[sku]['stock_final'] = stock_cedis
                        sku_data[sku][warehouse]['traspaso'] += stock_qty
                        new_stock_total = sku_data[sku][warehouse]['traspaso'] + sku_data[sku][warehouse]['actuals']
                        self.update_stock_percentage(f, new_stock_total)

            tomove = 0 
            for w in sku_warehouses:
                sku_data[sku][w]['traspaso'] = round(sku_data[sku][w]['traspaso'],2)
                tomove +=  round(sku_data[sku][w]['traspaso'],2)
            sku_data[sku]['stock_final'] = sku_data[sku]['actuals'] - tomove
        return sku_data

    def update_stock_percentage(self, postion, stock_to_move):
        if  self.warehouse_percentage[postion]['stock_max'] == 0:
            self.warehouse_percentage[postion]['porce'] = 0
        else:
            self.warehouse_percentage[postion]['porce'] = round(stock_to_move / self.warehouse_percentage[postion]['stock_max'] * 100,2)
        return True
                       
    def get_product_by_type(self, product_type, product_line=""):
        product_field = None
        # Base query with both product_type and product_line (if not empty)
        mango_query = {
            "selector": {
                "$and": [
                    {"answers." + prod_obj.f['product_type']: {"$eq": product_type}},
                    # Only add the product_line filter if it's not empty
                    {"answers." + prod_obj.f['product_category']: {"$eq": product_line}} if product_line else {}
                ]
            },
            "limit": 10000,
            "skip": 0
        }
    
        # Clean up any empty $and conditions if product_line was empty
        mango_query["selector"]["$and"] = [clause for clause in mango_query["selector"]["$and"] if clause]

        # Execute query and return records
        record = prod_obj._labels_list(prod_obj.lkf_api.search_catalog(prod_obj.PRODUCT_ID, mango_query), prod_obj.f)
        # print('RECORD', record)
        return record
    
    def get_procurments(self, warehouse=None, location=None, product_code=None, sku=None, status='programmed', group_by=False):
        match_query ={ 
                'form_id': procurment_obj.PROCURMENT,  
                'deleted_at' : {'$exists':False},
                #todo fix balance para q suba con stauts
                # f'answers.{procurment_obj.mf["jit_procurment_status"]}': status,
            }
        if product_code:
            match_query.update({
                f"answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f['product_code']}": {'$in': product_code}
                })
            
        # match_query.update({
        #     f"answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f['product_code']}": "750200301011"})
        if sku:
            match_query.update({
                f"answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f['sku']}":sku
                })
        if warehouse:
            match_query.update({
                f"answers.{warehouse_obj.WAREHOUSE_LOCATION_OBJ_ID}.{warehouse_obj.f['warehouse']}":warehouse
                })
        if location:
            match_query.update({
                f"answers.{warehouse_obj.WAREHOUSE_LOCATION_OBJ_ID}.{warehouse_obj.f['warehouse_location']}":location
                })
        query = [
            {'$match': match_query},
            {'$project':{
                    '_id':0,
                    'date':f'$answers.{procurment_obj.mf["procurment_date"]}',
                    'date_schedule':f'$answers.{procurment_obj.mf["procurment_schedule_date"]}',
                    'procurment_method':f'$answers.{procurment_obj.mf["procurment_method"]}',
                    'procurment_qty':f'$answers.{procurment_obj.mf["procurment_qty"]}',
                    'product_code':f'$answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f["product_code"]}',
                    'sku':f'$answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f["sku"]}',
                    'uom':f'$answers.{procurment_obj.UOM_OBJ_ID}.{procurment_obj.f["uom"]}',
                    'warehouse':f'$answers.{warehouse_obj.WAREHOUSE_LOCATION_OBJ_ID}.{warehouse_obj.f["warehouse"]}',
                    'warehouse_location':f'$answers.{warehouse_obj.WAREHOUSE_LOCATION_OBJ_ID}.{warehouse_obj.f["warehouse_location"]}',
            }}]
        # print('query=', simplejson.dumps(query, indent=3))
        return procurment_obj.format_cr(procurment_obj.cr.aggregate(query))

    def get_report_filters(self, filters=[], product_code_aux=None):
        mango_query = {
            "selector": {
                "_id": {"$gt": None}
            },
            "limit": 10000,
            "skip": 0
        }

        if 'inventory' in filters:
            if product_code_aux:
                mango_query['selector'] = {f"answers.{prod_obj.f['product_type']}": product_code_aux}

            res = prod_obj.lkf_api.search_catalog(123105, mango_query)

            product_categories = []

            for item in res:
                category = item.get(prod_obj.f['product_category'])
                if category and category not in product_categories:
                    product_categories.append(category)

        return product_categories
    
    def format_catalog_product(self, data_query, id_field):
        list_response = []
        for item in data_query:
            wharehouse = item.get(id_field,'')
            if wharehouse not in list_response and wharehouse !='':
                list_response.append(wharehouse)

        list_response.sort()
        return list_response

    def get_catalog_product_field(self, id_field):
        query = {"form_id":123150, 'deleted_at':{'$exists':False}}

        # Obtener los ids distintos y filtrar los None
        db_ids = [item for item in self.cr.distinct("answers.66dfc4d9a306e1ac7f6cd02c.61ef32bcdf0ec2ba73dec343", query) if item is not None]
        
        match_query = { 
            'deleted_at':{"$exists":False},
        }

        mango_query = {
            "selector": {
                "answers": {
                    "$and": [match_query]
                }
            },
            "limit": 10000,
            "skip": 0
        }
        
        res = script_obj.lkf_api.search_catalog(123105, mango_query)
        res_format = reorder_obj.format_catalog_product(res, id_field)
        
        return res_format
    
    def get_max_stock(self, data):
        res = {x['warehouse'].lower().replace(' ','_'):x['stock_maximum'] for x in data}
        return res

    def generate_report_info(self):
        products = reorder_obj.get_product_by_type(product_type=product_family, product_line=product_line)
        product_dict = {x['product_code']: x for x in products}
        product_code = list(product_dict.keys())
                    
        # print('product_code=',product_code)
        procurment = reorder_obj.get_procurments(product_code=product_code)
        if not procurment:
            return None
        product_stock = stock_obj.get_products_inventory(product_code=product_code, warehouse=warehouse_info)
        # print('product_stock',product_stock)
        stock_cedis = stock_obj.get_products_inventory(product_code=product_code, warehouse=warehouse_cedis)
        stock_cedis_dict = {x['product_code']: x['actuals'] for x in stock_cedis}
        # print('stock_cedis_dict=',stock_cedis_dict)
                    
        res_first = reorder_obj.reorder_rules_warehouse(product_code=product_code)
        stock_dict = {}

        for item in procurment:
            # if item['sku'] != '750200301057':
            #     continue
            code = item['product_code']
            warehouse = item['warehouse'].lower().replace(' ', '_')
            if warehouse not in self.warehouses:
                self.warehouses.append(warehouse)
                print('warehouse_code=',warehouse)
            actuals = stock_cedis_dict.get(code, 0)
            if not product_dict.get(code):
                continue
            product_name = product_dict[code]['product_name']
            product_category = product_dict[code]['product_category']
            product_type = product_dict[code]['product_type']

            # Inicializa el producto en el diccionario de stock si no existe
            stock_dict[code] = stock_dict.get(code, {
                'sku': item['sku'],
                'desc_producto': product_name,
                'line': product_category,
                'familia': product_type,
                'stock_mty': 0,
                'stock_gdl': 0,
                'stock_max': 0,
                'stock_merida': 0,
                'actuals': actuals,  # proviene de stock_cedis_dict
                'percentage_stock_max': 0,
                'stock_final': actuals,  # Inicializamos stock_final con los actuales
            })

            # Actualiza stock_to_move, asegurándose de no exceder el stock disponible
            available_stock = stock_dict[code].get('stock_final', 0)
            # stock_to_move = min(item['procurment_qty'], available_stock)
            stock_to_move = item['procurment_qty']
            # print('code=',code)
            # print('warehouse=',warehouse)
            # print('stock_to_move=',stock_to_move)
            # print('actuals=',actuals)
            # print('item=',item['procurment_qty'])
            stock_dict[code].update({f'stock_to_move_{warehouse}': round(stock_to_move, 2)})

            #vaoms a dejarlo que se vaya a negativo y solo trabajamos con los negativos...
            stock_dict[code]['stock_final'] = round(stock_dict[code]['stock_final'] - stock_to_move, 2)

            # Verificación para evitar que stock_final sea negativo
            #if stock_dict[code]['stock_final'] >= item['procurment_qty']:
                # Si hay suficiente stock para el traspaso
            #     stock_dict[code]['stock_final'] = round(stock_dict[code]['stock_final'] - item['procurment_qty'], 2)
            # else:
            #     # Si no hay suficiente stock, se ajusta a 0
                # stock_dict[code]['stock_final'] = 0
        # Ahora actualiza la información de los productos en el stock de los almacenes
        for x in product_stock:
            code = x['product_code']
            warehouse = x['warehouse'].lower().replace(' ', '_')          
            if stock_dict.get(code):
                if 'cedis' in warehouse:
                    warehouse = warehouse.replace('cedis', 'alm')
                # elif warehouse == 'alm_guadalajara':
                #     stock_dict[code]['actuals_alm_guadalajara'] = 0
                else:
                    stock_dict[code][f'actuals_{warehouse}'] = x['actuals']
                
        # Actualiza las reglas de stock máximo para cada producto
        for x in res_first:
            code = x['product_code']
            warehouse = x['warehouse'].lower().replace(' ', '_')
            if stock_dict.get(code):
                stock_dict[code][f'stock_max_{warehouse}'] = x['stock_maximum']
                if x['stock_maximum'] == 0:
                    stock_dict[code][f'p_stock_max_{warehouse}'] = 0
                else:
                    stock_dict[code][f'p_stock_max_{warehouse}'] = round((stock_dict[code].get(f'actuals_{warehouse}', 0) / x['stock_maximum']) * 100, 2)
                # print('*********************')
                # print('********************* stock dict', simplejson.dumps(stock_dict[code], indent=3))
                # print('********************* stock dict', stock_dict[code])
        #stock_dict = self.double_check(stock_dict)
        stock_list = list(stock_dict.values())
        return stock_list
    
    def build_data_from_report(self, stock_list):
        data = {}
        # Extraer los nombres de los almacenes dinámicamente desde las claves que contienen 'stock_max_alm_'
        # for key in stock_list[0]:
        # print('222stock_list=', simplejson.dumps(stock_list, indent=4))
        warehouse_keys = [key.split('stock_max_alm_')[-1] for key in stock_list[0].keys() if 'p_stock_max_alm_' in key]
        for product in stock_list:
            sku = product['sku']
            data[sku] = {
                'actuals': product.get("actuals", 0),
                'stock_final': product.get("stock_final", 0),
                }
            # print('PROD', data)
            # Iterar sobre los almacenes extraídos dinámicamente
            for warehouse_key in warehouse_keys:
                # Usamos las claves formateadas para acceder a la información de stock final y max
                warehouse_code = f'alm_{warehouse_key}'
                # Si el almacén está en los datos del producto, lo agregamos al diccionario
                if f'stock_max_{warehouse_code}' in product:
                    # Obtengo los valores del stock y los demás parámetros
                    stock_max = product.get(f'stock_max_{warehouse_code}', 0)
                    actuals = product.get(f'actuals_{warehouse_code}', 0)
                    p_stock_max = product.get(f'p_stock_max_{warehouse_code}', 0)
                    stock_to_move = product.get(f'stock_to_move_{warehouse_code}', 0)
                    
                    # Si el almacén no existe en el diccionario, lo creamos
                    if warehouse_key not in data:
                        data[sku][warehouse_key] = {
                            'almacen': warehouse_key,
                            'stock_max': stock_max,
                            'stock_to_move': stock_to_move,
                            'porce': p_stock_max,
                            'traspaso': 0,  # Suponiendo que la clave 'traspaso' es 0 por defecto
                            'actuals': actuals,
                        }
                    else:
                        # Si el almacén ya existe, solo actualizamos los valores de stock_max, porce, actuals
                        data[sku][warehouse_key]['stock_max'] = stock_max
                        data[sku][warehouse_key]['porce'] = p_stock_max
                        data[sku][warehouse_key]['actuals'] = actuals
            
        return data
    
    def get_stock_data(self, data, idx):
        stock_warehouse = data[idx]
        stock_max = stock_warehouse['stock_max']
        return stock_max

    def piezas_porcentaje(self, stock_max, porcentaje):
        piezas = porcentaje * stock_max / 100 *100
        return piezas
        
    def warehouse_transfer_update(self):
        self.ROUTE_RULES = self.get_rutas_transpaso()
        stock_list = self.generate_report_info()  # Lista de productos
        if not stock_list:
            return [{}]
        data = self.build_data_from_report(stock_list)  # Genera los datos del informe
        # print('data=',datad)
        data = self.almancenes_por_porcentaje(data)
        return self.arrage_data(stock_list, data)
        # warehouse_percentage_response = self.warehouses_by_percentage()  # Respuesta con almacenes y traspasos
        return data
        
    def get_porcentaje_final(self, data):
        max_stock = 0
        to_move = 0
        actuals = 0
        vals = {}
        res = {}
        for key, value in data.items():
            if 'stock_max_' in key:
                if 'p_' in key:
                    continue
                warehouse =  key.split('stock_max_')[-1]
                max_stock = value
                vals[warehouse] = vals.get(warehouse,{})
                vals[warehouse]['max_stock'] = value
            elif 'stock_to_move_' in key:
                warehouse =  key.split('stock_to_move_')[-1]
                to_move = value
                vals[warehouse] = vals.get(warehouse,{})
                vals[warehouse]['to_move'] = value
            elif 'actuals_' in key:
                warehouse =  key.split('actuals_')[-1]
                vals[warehouse] = vals.get(warehouse,{})
                vals[warehouse]['actuals'] = value
        for wh, v in vals.items():
            final_stock_percentage = 0
            if v.get('max_stock'):
                final_stock_percentage = (v.get('to_move',0) + v.get('actuals',0)) / v.get('max_stock',1)*100
            res[f'final_stock_percentage_{wh}'] = round(final_stock_percentage,2)

        return res

    def arrage_data(self, stock_list, data):
        res = []
        for row in stock_list:
            sku = row['sku']
            sku_data = data[sku]
            row['stock_final'] =  sku_data['stock_final']
            for key, value in sku_data.items():
                if not isinstance(value, dict):
                    continue
                row[f'stock_to_move_alm_{key}'] =  value['traspaso']
                row.update(self.get_porcentaje_final(row))
                for w in self.warehouses:
                    row[f'actuals_{w}'] = row.get(f'actuals_{w}',0)
                    row[f'stock_to_move_{w}'] = row.get(f'stock_to_move_{w}',0)
        return stock_list

                        
        
        
if __name__ == "__main__":
    reorder_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    script_obj = base.LKF_Base(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()
    prod_obj = Product(settings, sys_argv=sys.argv, use_api=True)
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    procurment_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    warehouse_obj = Warehouse(settings, sys_argv=sys.argv, use_api=True)

    data = reorder_obj.data
    data = data.get('data',[])
    option = data.get('option','get_report')
    #option = 'get_report'
    product_family = data.get('product_family', 'TUBOS')
    product_line = data.get('product_line', '')
    warehouse_info = data.get('warehouse', '')
    warehouse_cedis = 'CEDIS GUADALAJARA'
    reorder_obj.warehouses = []

    if option == 'get_report':
        #reorder_obj.warehouse_transfer_update(),
        script_obj.HttpResponse({
         "stockInfo": reorder_obj.warehouse_transfer_update(),
        })

    elif option == 'get_catalog':
        warehouse_types_catalog = warehouse_obj.get_all_stock_warehouse()
        product_type = reorder_obj.get_catalog_product_field(id_field=prod_obj.f['product_type'])
        script_obj.HttpResponse({
            "dataCatalogWarehouse": warehouse_types_catalog,
            "dataCatalogProductFamily": product_type,
        })

    elif option == 'get_product_line':
        filters = ['inventory',]
        product_code_aux = data.get("product_code")
        products_categorys = reorder_obj.get_report_filters(filters, product_code_aux=product_code_aux)

        script_obj.HttpResponse({
            "product_line": products_categorys,
        })