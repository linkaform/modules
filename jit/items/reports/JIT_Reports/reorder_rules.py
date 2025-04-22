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


class Reports(Reports):
    
    def estrucutra_rutas(self):
        estructura = {}
        for p in self.get_rutas_transpaso():
            code = p['product_code']
            wh = p['warehouse']
            wh_loc = p['warehouse_location']
            wh_dest = p['warehouse_dest']
            wh_loc_dest = p['warehouse_location_dest']
            standar_pack = p['standar_pack']
            sku = p['sku']

            estructura.setdefault(code, {}) \
                      .setdefault(wh, {}) \
                      .setdefault(wh_loc, {}) \
                      .setdefault(wh_dest, {}) \
                      .setdefault(wh_loc_dest, {
                          'product_code': code,
                          'standar_pack': standar_pack,
                          'sku': sku,
              })

        return estructura

    def almancenes_por_porcentaje(self, sku_data):
        self.warehouse_percentage = {}
        # print('t',t)
        # self.ROUTE_RULES = {x['product_code']:x for x in self.get_rutas_transpaso(sku) if x.get('product_code')}
        
        for sku, data in sku_data.items():
            standar_pack = self.ROUTE_RULES.get(str(sku),{})
            sku_warehouses = []
            # standar_pack {'product_code': '750200310481', 'sku': '750200310481', 'standar_pack': 1.0, 'warehouse': 'CEDIS GUADALAJARA', 'warehouse_location': 'Almacen CEDIS Guadalajara', 'warehouse_dest': 'ALM MERIDA', 'warehouse_location_dest': 'Almacen Merida'}

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
                    warehouse = self.warehouse_percentage[f]['almacen']
                    if (idx+1) >= len(p):
                        if len(p) == 1:
                            print('brake 4888888888888888888888', idx)
                            stock_cedis = - 1
                        break
                    porce = self.warehouse_percentage[f]['porce']
                    if warehouse not in sku_warehouses:
                        sku_warehouses.append(warehouse)
                    if porce < 100:
                        perc_difference = (p[idx+1] - p[idx])/100

                        stock_cedis, stock_qty = self.almancenes_por_porcentaje_set(f, standar_pack, stock_cedis, update_perc=perc_difference)
                        if stock_cedis < 0:
                            break
                        sku_data[sku]['stock_final'] = stock_cedis
                        sku_data[sku][warehouse]['traspaso'] += stock_qty
                        new_stock_total = sku_data[sku][warehouse]['traspaso'] + sku_data[sku][warehouse]['actuals']
                        self.update_stock_percentage(f, new_stock_total)
                        while_warning[warehouse] = while_warning.get(warehouse,[])
                        if stock_cedis in while_warning.get(warehouse,[]):
                            stock_cedis = -1
                        else:
                            while_warning[warehouse].append(stock_cedis)
                # print('============== END ================', stock_cedis)

            tomove = 0 
            for w in sku_warehouses:

                sku_data[sku][w]['traspaso'] = round(sku_data[sku][w]['traspaso'],2)
                tomove +=  round(sku_data[sku][w]['traspaso'],2)
            sku_data[sku]['stock_final'] = sku_data[sku]['actuals'] - tomove
        return sku_data

    def almancenes_por_porcentaje_set(self, pos, standar_pack, stock_cedis, update_perc=.01):
        stock_max = self.get_stock_data(self.warehouse_percentage, pos)
        stock_qty = self.piezas_porcentaje(stock_max, update_perc)
        stock_qty = round(self.calc_shipment_pack(stock_qty, standar_pack), 2)
        stock_cedis -= stock_qty
        if stock_cedis < 0 and stock_qty > standar_pack:
            stock_cedis += stock_qty
            n_stock_cedis, n_stock_qty = self.almancenes_por_porcentaje_set(pos, standar_pack, stock_cedis, update_perc=update_perc/2)
            return n_stock_cedis, n_stock_qty
            # if n_stock_qty > n_stock_cedis:
            #     if n_stock_cedis > 0:
            #         print('aaaaaaaaaaaaaaa', n_stock_cedis,n_stock_qty )
            #         return n_stock_cedis, n_stock_qty
            #     stock_cedis -= stock_qty
            #     print('vvvvvvvvvvvvvvvvvvv',stock_cedis)
            #     return stock_cedis, 0
            # else:
            #     print('else??????/')
            #     # stock_qty = 0
            #     # stock_cedis = -1
        return stock_cedis, stock_qty

    def arrage_data(self, stock_list, data):
        res = []
        for row in stock_list:
            sku = row['sku']
            # if sku == '750200302529':
            #         print('111row=',row)
            sku_data = data[sku]
            row['stock_final'] =  sku_data['stock_final']
            for key, value in sku_data.items():
                if not isinstance(value, dict):
                    continue
                row[f'stock_to_move_alm_{key}'] =  value['traspaso']
                # if sku == '750200302529':
                #     print('row=',row)
                #     print('===========================================')
                #     print('key=', key)
                #     print('value=', value)
                #     print('traspaso=', value['traspaso'])
                #     print('asi queda...', row[f'stock_to_move_alm_{key}'])
                #     print('asi queda... row=', row)
                row.update(self.get_porcentaje_final(row))
                for w in self.warehouses:
                    # print('row...',row)
                    row[f'actuals_{w}'] = row.get(f'actuals_{w}',0)
                    row[f'stock_to_move_{w}'] = row.get(f'stock_to_move_{w}',0)
        return stock_list

    def build_data_from_report(self, stock_list):
        data = {}
        # Extraer los nombres de los almacenes dinámicamente desde las claves que contienen 'stock_max_alm_'
        # for key in stock_list[0]:
        warehouse_keys = []
        for row in stock_list:
            for key in row:
                if 'stock_max_alm_' in key:
                    w = key.split('stock_max_alm_')[-1]
                    if w not in warehouse_keys:
                        warehouse_keys.append(w)
        # warehouse_keys = [key.split('stock_max_alm_')[-1] for key in stock_list[0].keys() if 'p_stock_max_alm_' in key]
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

    def eval_procurment(self, proc, procurment_warehouses):
        product_code = proc['product_code']
        sku = proc['sku']
        warehouse = proc['warehouse']
        needed = proc['procurment_qty']
        actuals = self.product_stock_by_wh.get(warehouse).get(product_code,0)
        proc['status'] = 404
        proc['stock'] = actuals
        for proc_warehouse in procurment_warehouses:
            if needed <= 0:
                continue
            stock_warehouse = self.product_stock_by_wh.get(proc_warehouse).get(product_code,0)
            standar_pack = self.get_procurments_standar_pack(sku, proc_warehouse)
            print(f'sku: {sku} standar_pack: {standar_pack}')
            max_stock = self.max_stock_by_warehouse.get(warehouse, {}).get(product_code,0)
            if max_stock > 0:
                proc['percentage_start'] = round(actuals / max_stock,2)
            if stock_warehouse > max_stock:
                available_to_share = stock_warehouse - max_stock
                proc['from'] = proc_warehouse
                if available_to_share > needed:
                    proc['status'] = 200
                    proc['handover'] = round(self.calc_shipment_pack(needed, standar_pack), 2)
                    proc['stock_final'] = round(proc['handover'] + actuals,2)
                    if max_stock > 0:
                        proc['percentage_finish'] = round(proc['stock_final'] / max_stock,2)
                    self.product_stock_by_wh[proc_warehouse][product_code] = stock_warehouse - needed
                    return proc
                elif available_to_share < needed:
                    proc['status'] = 200
                    proc['handover'] = round(self.calc_shipment_pack(needed, standar_pack), 2)
                    proc['stock_final'] = round(proc['handover'] + actuals,2)
                    if max_stock > 0:
                        proc['percentage_finish'] = round(proc['stock_final'] / max_stock,2)
                    self.product_stock_by_wh[proc_warehouse][product_code] = stock_warehouse - available_to_share
        return proc

    def format_catalog_product(self, data_query, id_field):
        list_response = []
        for item in data_query:
            wharehouse = item.get(id_field,'')
            if wharehouse not in list_response and wharehouse !='':
                list_response.append(wharehouse)

        list_response.sort()
        return list_response

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
        record = prod_obj._labels_list(self.lkf_api.search_catalog(prod_obj.PRODUCT_ID, mango_query), prod_obj.f)
        # print('RECORD', record)
        return record
    
    def get_procurments(self, warehouse=None, location=None, product_code=None, sku=None, status='programmed', group_by=False):
        match_query ={ 
                'form_id': procurment_obj.PROCURMENT,  
                'deleted_at' : {'$exists':False},
                # f'answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f["sku"]}':'750200301069'
                #todo fix balance para q suba con stauts
                # f'answers.{procurment_obj.mf["jit_procurment_status"]}': status,
            }
        # if product_code:
        #     match_query.update({
        #         f"answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f['product_code']}": {'$in': product_code}
        #         })
            
        # match_query.update({
        #     f"answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f['product_code']}": "750200301071"})
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
        # print('cr',procurment_obj.cr)
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
        
        res = reorder_obj.lkf_api.search_catalog(123105, mango_query)
        res_format = reorder_obj.format_catalog_product(res, id_field)
        
        return res_format
    
    def get_max_stock(self, data):
        res = {x['warehouse'].lower().replace(' ','_'):x['stock_maximum'] for x in data}
        return res

    def get_procurments_standar_pack(self, sku, procurment_warehouses):
        standar_pack = self.ROUTE_RULES.get(str(sku),{})
        #TODO QUITAR CEDIS
        spack = standar_pack['CEDIS GUADALAJARA']['Almacen CEDIS Guadalajara']
        location = {
            'ALM GUADALAJARA':'Almacen Guadalajara',
            'ALM MONTERREY':'Almacen Monterrey',
            }
        return spack.get(procurment_warehouses,{}).get(location[procurment_warehouses],{}).get('standar_pack',1)

    def get_stock_by_warehouse(self, product_codes, warehouses):
        res = {}
        for wh in warehouses:
            stock_data = stock_obj.get_products_inventory(product_code=product_codes, warehouse=wh)
            res[wh] = {d['product_code']:d['actuals'] for d in stock_data}
        return res

    def get_procurment_by_warehouse(self, data):
        res = {}
        for d in data:
            wh = d['warehouse']
            res[wh] = res.get(wh,{})
            pcode = d['product_code']
            stock_maximum = d['stock_maximum']
            res[wh][pcode] = stock_maximum
        return res

    def generate_report_info(self):
        products = reorder_obj.get_product_by_type(product_type=product_family, product_line=product_line)
        # product_dict = {x['product_code']: x for x in products}
        product_dict = {}
        product_code = []
        for x in products:
            pcode = x['product_code']
            product_dict[pcode] = {
                'sku' : x['product_code'],
                'desc' : x['product_name'],
                'product_type' : x['product_type'],
                'line' : x['product_category'],
            }
            product_code.append(x['product_code'])

        # product_code = list(product_dict.keys())

        procurment = reorder_obj.get_procurments(product_code=product_code)
        if not procurment:
            return None
        
        # unique_warehouses = sorted({item['warehouse'] for item in procurment})
        unique_warehouses = ['ALM GUADALAJARA','ALM MONTERREY']
        self.product_stock_by_wh = self.get_stock_by_warehouse(product_code, unique_warehouses)
        # print('product_stock_by_wh=',product_stock_by_wh)

        # product_stock = stock_obj.get_products_inventory(product_code=product_code, warehouse=warehouse_info)
        # print('product_stock',product_stock)
        # stock_cedis = stock_obj.get_products_inventory(product_code=product_code)
        # stock_cedis_dict = {x['product_code']: x['actuals'] for x in stock_cedis}
                    
        res_first = reorder_obj.reorder_rules_warehouse(product_code=product_code)
        self.max_stock_by_warehouse = self.get_procurment_by_warehouse(res_first)
        stock_dict = {}

        result = {}
        for warehouse in unique_warehouses:
            procurment_xfer = []
            procurment_na = []
            stock_dict[warehouse] = stock_dict.get(warehouse,[])
            for proc in procurment:
                procurment_warehouses = [item for item in unique_warehouses if item not in proc['warehouse']]
                sku = proc['sku']
                proc.update(product_dict[sku])
                p_warehouse = proc.get('warehouse')
                if warehouse == p_warehouse:
                    proc = self.eval_procurment(proc, procurment_warehouses)
                    if proc['status'] == 200:
                        procurment_xfer.append(proc)
                    elif proc['status'] == 404:
                        procurment_na.append(proc)
            if warehouse == 'ALM MONTERREY':
                result['tableFirst'] = procurment_xfer
                result['tableSecond'] = procurment_na
            elif warehouse == 'ALM GUADALAJARA':
                result['tableThird'] = procurment_xfer
                result['tableFourth'] = procurment_na
            else:
                result['tableFifth'] = procurment_xfer
                result['tableSixth'] = procurment_na


        return result

        # for item in procurment:
        #     print('item=', item)
        #     code = item['product_code']
        #     warehouse = item['warehouse'].lower().replace(' ', '_')
        #     # if item['sku'] == '750200302529':
        #     # #     continue
        #     if warehouse not in self.warehouses:
        #         self.warehouses.append(warehouse)
        #     # actuals = stock_cedis_dict.get(code, 0)
        #     if not product_dict.get(code):
        #         continue
        #     product_name = product_dict[code]['product_name']
        #     product_category = product_dict[code]['product_category']
        #     product_type = product_dict[code]['product_type']

        #     # Inicializa el producto en el diccionario de stock si no existe
        #     stock_dict[code] = stock_dict.get(code, {
        #         'sku': item['sku'],
        #         'desc_producto': product_name,
        #         'line': product_category,
        #         'familia': product_type,
        #         'actuals': item['actuals'],  # proviene de stock_cedis_dict
        #         'percentage_stock_max': 0,
        #         'stock_final': item.get()'stock_final'],  # Inicializamos stock_final con los actuales
        #         'handover': item['handover'],  # Inicializamos stock_final con los actuales
        #     })
        #     # Actualiza stock_to_move, asegurándose de no exceder el stock disponible
        #     available_stock = stock_dict[code].get('stock_final', 0)
        #     # stock_to_move = min(item['procurment_qty'], available_stock)
        #     stock_to_move = item['procurment_qty']
        #     stock_dict[code].update({f'stock_to_move_{warehouse}': round(stock_to_move, 2)})
        #     # if item['sku'] == '750200302529':
        #     # #     continue
        #     #     print('stock_dict=', stock_dict[code])
        #     #vaoms a dejarlo que se vaya a negativo y solo trabajamos con los negativos...
        #     stock_dict[code]['stock_final'] = round(stock_dict[code]['stock_final'] - stock_to_move, 2)

        #     # Verificación para evitar que stock_final sea negativo
        #     #if stock_dict[code]['stock_final'] >= item['procurment_qty']:
        #         # Si hay suficiente stock para el traspaso
        #     #     stock_dict[code]['stock_final'] = round(stock_dict[code]['stock_final'] - item['procurment_qty'], 2)
        #     # else:
        #     #     # Si no hay suficiente stock, se ajusta a 0
        #         # stock_dict[code]['stock_final'] = 0
        # # Ahora actualiza la información de los productos en el stock de los almacenes
        # for x in product_stock:
        #     code = x['product_code']
        #     warehouse = x['warehouse'].lower().replace(' ', '_')          
        #     if stock_dict.get(code):
        #         if 'cedis' in warehouse:
        #             warehouse = warehouse.replace('cedis', 'alm')
        #         # elif warehouse == 'alm_guadalajara':
        #         #     stock_dict[code]['actuals_alm_guadalajara'] = 0
        #         else:
        #             stock_dict[code][f'actuals_{warehouse}'] = x['actuals']
                
        # # Actualiza las reglas de stock máximo para cada producto
        # for x in res_first:
        #     code = x['product_code']
        #     warehouse = x['warehouse'].lower().replace(' ', '_')
        #     if stock_dict.get(code):
        #         stock_dict[code][f'stock_max_{warehouse}'] = x['stock_maximum']
        #         if x['stock_maximum'] == 0:
        #             stock_dict[code][f'p_stock_max_{warehouse}'] = 0
        #         else:
        #             stock_dict[code][f'p_stock_max_{warehouse}'] = round((stock_dict[code].get(f'actuals_{warehouse}', 0) / x['stock_maximum']) * 100, 2)
        #         # print('********************* stock dict', simplejson.dumps(stock_dict[code], indent=3))
        # # print('*********************')
        # # print('********************* stock dict', stock_dict)
        # #stock_dict = self.double_check(stock_dict)
        # stock_list = list(stock_dict.values())
        # return stock_list
    
    def get_stock_data(self, data, idx):
        stock_warehouse = data[idx]
        stock_max = stock_warehouse['stock_max']
        return stock_max

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

    def piezas_porcentaje(self, stock_max, porcentaje):
        piezas = porcentaje * stock_max / 100 *100
        return piezas
 
    def send_next_product(self, data):
        m = []
        almancenes_max={}
        idx = 0
        for w, v in data.items():
            idx += .01
            if not isinstance(v, dict):
                continue
            amax = v['stock_max']
            if amax in m:
                amax += idx
            m.append(amax)
            almancenes_max[amax] = v
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

    def update_stock_percentage(self, postion, stock_to_move):
        if  self.warehouse_percentage[postion]['stock_max'] == 0:
            self.warehouse_percentage[postion]['porce'] = 0
        else:
            self.warehouse_percentage[postion]['porce'] = round(stock_to_move / self.warehouse_percentage[postion]['stock_max'] * 100,2)
        return True

    # def warehouse_transfer_update(self):
    #     self.ROUTE_RULES = self.get_rutas_transpaso()

    #     stock_list = self.generate_report_info()  # Lista de productos
    #     print('stock list', stock_list)
    #     if not stock_list:
    #         return [{}]
    #     data = self.build_data_from_report(stock_list)  # Genera los datos del informe
    #     data = self.almancenes_por_porcentaje(data)
    #     # print('daataaa=',data)
    #     return self.arrage_data(stock_list, data)
    #     # warehouse_percentage_response = self.warehouses_by_percentage()  # Respuesta con almacenes y traspasos
    #     # return data
        
    def warehouse_transfer_update2(self):
        # self.ROUTE_RULES = self.get_rutas_transpaso()
        self.ROUTE_RULES = self.estrucutra_rutas()
        stock_list = self.generate_report_info()  # Lista de productos
        if not stock_list:
            return [{}]
        return stock_list
        # data = self.build_data_from_report(stock_list)  # Genera los datos del informe
        # print('------------------------data',data)
        # data = self.almancenes_por_porcentaje(data)
        # # print('daataaa=',data)
        # return self.arrage_data(stock_list, data)
        # # warehouse_percentage_response = self.warehouses_by_percentage()  # Respuesta con almacenes y traspasos
        # # return data                        
        
if __name__ == "__main__":
    reorder_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    reorder_obj.console_run()
    # 

    data = reorder_obj.data
    data = data.get('data',[])
    option = data.get('option','get_report')
    #option = 'get_report'
    product_family = data.get('product_family', 'TUBOS')
    product_line = data.get('product_line', '')
    warehouse_info = data.get('warehouse', '')
    # warehouse_cedis = 'CEDIS GUADALAJARA'
    reorder_obj.warehouses = []


    if option == 'get_catalog':
        warehouse_obj = Warehouse(settings, sys_argv=sys.argv, use_api=True)
        prod_obj = Product(settings, sys_argv=sys.argv, use_api=True)
        # warehouse_types_catalog = warehouse_obj.get_all_stock_warehouse()
        product_type = reorder_obj.get_catalog_product_field(id_field=prod_obj.f['product_type'])
        reorder_obj.HttpResponse({
            "dataCatalogWarehouse": [],
            "dataCatalogProductFamily": product_type,
        })

    elif option == 'get_product_line':
        filters = ['inventory',]
        product_code_aux = data.get("product_code")
        products_categorys = reorder_obj.get_report_filters(filters, product_code_aux=product_code_aux)

        reorder_obj.HttpResponse({
            "product_line": products_categorys,
        })
    else:
        # reorder_obj.warehouse_transfer_update(),
        stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
        procurment_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
        prod_obj = stock_obj.Product
        warehouse_obj = stock_obj.WH
        # prod_obj = procurment_obj.Product
        # warehouse_obj = procurment_obj.WH
        # stock_obj = procurment_obj.Stock
        # prod_obj = Product(settings, sys_argv=sys.argv, use_api=True)
        reorder_obj.HttpResponse({
         # "stockInfo": reorder_obj.warehouse_transfer_update(),
         "data": reorder_obj.warehouse_transfer_update2(),
        })