# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.jit.app import JIT
from lkf_addons.addons.product.app import Product

today = date.today()
year_week = int(today.strftime('%Y%W'))



class JIT(JIT):

   def __init__(self, settings, sys_argv=None, use_api=False, **kwargs):
     super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
     self.Product.f["sku"] = '6205f73281bb36a6f1573358'
     f = {
         "month": '6206b9ae8209a9677f9b8bd9',
         "year": '6206b9ae8209a9677f9b8bda',
         }
     self.procurments = []

     if hasattr(self, 'f'):
         self.f.update(f)
     else:
         print('vaa  A IGUALSAR')
         self.f = f

   def allocate_warehouse(self, warehouse_orders, source_document):
     for warehouse, orders in warehouse_orders.items():
         for order in orders:
             if order.get('allocation'):
                 order['alloctaion_group'] = order.get('alloctaion_group',[])
                 order_needs = order['qty']
                 qty_allocated = 0
                 print('3order=', order)
                 for proc_data in order['allocation']:
                     proc_info = {}
                     if proc_data.get('allocated') and proc_data['allocated'] > 0:
                         print('proc_data=', proc_data)
                         order_needs -= proc_data['allocated']
                         proc_info['allocation_source_document'] = proc_data['folio']
                         proc_info['allocation_qty'] = float(proc_data['allocated'])
                         proc_info['allocation_source_document_type'] = source_document
                         qty_allocated += float(proc_data['allocated'])
                         proc_info['allocation_proc_method'] = 'produce'
                         order['alloctaion_group'].append(proc_info)
                 order['qty_allocated'] = sum(i.get('allocation_qty', 0) for i in order.get('alloctaion_group',[]))
                 order['qty_available'] = order['qty'] - order['qty_allocated']
                 if order_needs == 0:
                     order['fufilled_status'] = 'completed'
                 else:
                     order['fufilled_status'] = 'partial'
                 order['procurment_status'] = 'schedule'
                 answers = self._lables_to_ids(order)
                 folio = answers.pop('folio')
                 record_id = answers.pop('_id')
                 record_data = self.get_record_by_id(record_id)
                 # ans = aa['answers']
                 record_data['answers'].update(answers)
                 # answers = { '66d92acdb22bcdcc2f341ebf': 'produce'}
                 # answers['form_id'] = self.DEMANDA_PLAN
                 res = self.lkf_api.patch_record(data=record_data, record_id=record_id)
                 print('res=',res)
                 # res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.DEMANDA_PLAN, folios=[folio,], threading=False )

   def do_allocation(self, orders, source_document):
        print('************** allocating orders ************* ')
        for product_code, product_sku in orders.items():
            for sku, orders_by_sku in product_sku.items():
                # self.allocate_location(product_code, sku, orders_by_sku)
                self.allocate_warehouse(orders_by_sku.get('warehouse',[]), source_document='demand')

   def do_update_procurments(self):
      procs_by_folio = {}
      folios = []
      for p in self.procurments:
         p['qty_allocated'] =  sum(item.get('allocation_qty', 0) for item in p.get('alloctaion_group',[]))
         p['qty_available'] =  p['qty'] - p['qty_allocated']
         if p['qty_available'] == 0:
            p['fufilled_status'] = 'completed'
         else:
            p['fufilled_status'] = 'partial'
         folios.append(p['folio'])
         procs_by_folio[p['folio']] = self._lables_to_ids(p)
      # self.procurments = procs
      # folios = ['146220-9908','146218-9908']
      record_ids = self.get_record_by_folios(folios=folios, form_id=self.PROCURMENT)
      for record_data in record_ids:
         folio = record_data['folio']
         record_data['answers'].update(procs_by_folio[folio])
         res = self.lkf_api.patch_record_list(record_data)
   
   def get_open_allocations(self, product_code, sku, warehouse=None, location=None, client=None, **kwargs):
        kwargs.update( {
            'query':{
                'match_stage': {
                    f"answers.{self.f['procurment_method']}":'produce',
                    f"answers.{self.f['procurment_status']}":'programmed'},
                                },

                # 'group_stage':{'_id':{
                #                     'year':'$year',
                #                     'month':'$month',
                #                     'product_code':'$product_code',
                #                     'sku':'$product_sku',
                #                     'location':'$location',
                #                     'warehouse':'$warehouse',
                #                 },
                #                 'qty_by_folios': {'$push':{'qty':'$qty', 'folio':'$folio'}},
                #                 'qty':{"$sum":'$qty'},
                #                 'qty_allocated':{"$sum":'$qty_allocated'},
                #                 'qty_available':{"$sum":'$qty_available'},
                #                 },
                'sort_query': {
                    'sort_stage':{
                        'product_code':1,
                        'product_sku':1,
                        'location':1,
                        'warehouse':1,
                        'client':1,
                        'year':1,
                        'month':1,
                        }
                }}
            
        )
        query = self.query_procurment_record(product_code, sku, **kwargs)
        # print('procurment query=', simplejson.dumps(query, indent=3))
        open_procurments = self.format_cr(self.cr.aggregate(query))
        return open_procurments

   def can_allocate_orders(self, orders, **kwargs):
      #TODO ARAGE BY DATE
      new_orders ={}
      for product_code, product_sku in orders.items():
         new_orders[product_code] = {}
         for sku, orders_by_sku in product_sku.items():
            print('sku=', sku)
            print('orders_by_sku=', orders_by_sku)
            self.procurments = self.get_open_allocations(product_code, sku)
            print('##########################')
            print('PROCURMENTS=', self.procurments)
            print('##########################')
            #self.can_allocate_location(product_code, sku, orders_by_sku)
            orders_by_sku = self.can_allocate_warehouse(product_code, sku, orders_by_sku)
            new_orders[product_code][sku]= orders_by_sku
      return new_orders

   def create_procurment(self, threading=True):
     #call thread function
     self.lkf_post_multirecords(metadata, threading=threading)

   def can_allocate_location(self, product_code, sku, orders_by_sku):
     # sum_location
     # location_toleracnce?
     #     get_other_location
      dict_res ={}
      for location, location_orders in orders_by_sku['location'].items():
         loc_orders = []
         for order in location_orders:
             order_qty = order['qty']
             order_date = location_orders['date']
             order = self.find_procurment_for_order(order)
             loc_orders.append(order)
             print('order allocation 3', order['allocation'])
         dict_res[warehouse] = loc_orders
      orders_by_sku['location'] = dict_res
      return orders_by_sku

   def can_allocate_warehouse(self, product_code, sku, orders_by_sku):
      dict_res ={}
      for warehouse, warehouse_orders in orders_by_sku['warehouse'].items():
         wh_orders = []
         for order in warehouse_orders:
             order_qty = order['qty']
             order = self.find_procurment_for_order(order)
             wh_orders.append(order)
         dict_res[warehouse] = wh_orders
      orders_by_sku['warehouse'] = dict_res
      return orders_by_sku

   def find_procurment_for_order(self, order, source='demand'):
     print('--------------------- find ----------------------------')
     #entra orden por orden a buscar fit
     product_code = order.get('product_code')
     sku = order.get('product_sku')
     location = order.get('location')
     warehouse = order.get('warehouse')
     order = self._labels(order)
     qty = order.get('qty',0)
     print('qty=',qty)
     print('****************** TODO *********************')
     print('en vez de obtener el allocation group y la cantidad, vamos a buscar que demanda')
     print('ha sido utilizada en algun proc, tenemos que hacer un unwind y buscar por el source document')
     print('y a lo mejor tambien por el tipo q sea demad')
     print('---- todo-----')
     print('hay que obtener los allocations de procurments que estan en los distintos tipos de docuemntos')
     print('ej, en las demndas, en las producciones, en los stock de movimiento, etc, saber siempre con quien estoy comprometido')
     qty_allocated = sum(item.get('allocation_qty', 0) for item in order.get('alloctaion_group',[]))
     qty_needs =  order.get('qty',0) - qty_allocated
     allocated_on =[]
     print('FOLIO=',order['folio'])
     print('qty_allocated=',qty_allocated)
     print('qty_needs=',qty_needs)
     for proc in self.procurments:
         if qty_needs <= 0:
             break 
         proc_product_code = proc.get('product_code')
         proc_sku = order.get('product_sku')
         proc_location = proc.get('location')
         proc_warehouse = proc.get('warehouse')
         proc_qty = proc.get('qty')
         print('proc', proc)
         alloctaion_group = proc.get('alloctaion_group')
         if alloctaion_group:
            proc_avilabe = proc_qty - sum(item.get('allocation_qty', 0) for item in alloctaion_group)
         else:
            proc_avilabe = proc.get('qty')
         if proc_avilabe <= 0:
            continue
         print('proc_product_code', product_code)
         print('proc_sku', proc_sku)
         print('proc_location', proc_location)
         print('proc_warehouse', proc_warehouse)
         print('proc_qty', proc_qty)
         print('proc_avilabe', proc_avilabe)
         if product_code != proc_product_code:
            print('PRODUCT CODE NOT FOUND')
            continue
         if sku != proc_sku:
            print('SKU NOT FOUND')
            continue
         if location and location != proc_location:
            print('LOCATION NOT FOUND')
            continue
         if warehouse and warehouse != proc_warehouse:
            print('WAREHOUSE NOT FOUND')
            continue
         if proc_avilabe >= qty_needs:
            print('>>>>>>>>>>>>>>>>>')
            proc['allocated'] = qty_needs
            proc['qty_available'] = proc_avilabe - qty_needs
            proc['qty_allocated'] += qty_needs
            proc['update'] = True
            proc['alloctaion_group'].append({
               'allocation_qty':7,
               'allocation_source_document': order['folio'],
               'allocation_source_document_type': source,
            })
            allocated_on.append(deepcopy(proc))
            qty_needs = 0
         elif proc_avilabe < qty_needs:
            print('<<<<<<<<<<<<<<<<')
            print('<proc[qty_allocated]=<',proc['qty_allocated'])
            proc['allocated'] = proc_avilabe
            proc['qty_allocated'] += proc_avilabe
            proc['qty_available'] = 0
            qty_needs -= proc_avilabe
            proc['update'] = True
            proc['alloctaion_group'].append({
               'allocation_qty':proc_avilabe,
               'allocation_source_document': order['folio'],
               'allocation_source_document_type': source,
               })
            allocated_on.append(deepcopy(proc))
         else:
            print('*******************************')
         print('=========== ending proc============')
         print(proc)
         print('qty_needs',qty_needs)
         print('=======================')
     order['allocation'] = allocated_on
     print('order allocation 1', order['allocation'])
     print('*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-')
     return order

   def get_aggreate_operator(self, aggregate_query, operator):
     #get_operator = [list(element.keys())[0] for element in query ]
     stage_query = next((element for element in aggregate_query if list(element.keys())[0] == operator), None)
     if stage_query and stage_query.get(operator):
         return stage_query[operator]
     return stage_query

   def get_demand_needs(self, product_code=None, sku=None, warehouse=None, location=None, client=None, **kwargs):
     print('run run_demand_procurements',)
     kwargs.update( {
         'query':{
             'match_stage': {
                              f"answers.{self.f['procurment_method']}":'produce',
                              "$or":[
                                  {f"answers.{self.f['procurment_status']}":'programmed'},
                                  {
                                      f"answers.{self.f['procurment_status']}":'schedule',
                                      f"answers.{self.f['allocation_status']}":{'$ne':'completed'},
                                  }
                                 ]
                            }, 
             # 'group_stage':{'_id':{
             #                     'product_code':'$product_code',
             #                     'product_sku':'$product_sku',
             #                     'year':'$year',
             #                     'month':'$month',
             #                     'location':'$location',
             #                     'warehouse':'$warehouse',
             #                     'client':'$client',
             #                 },
             #                 'qty_by_folios': {'$push':{'qty':'$qty', 'folio':'$folio'}},
             #                 'qty':{"$sum":'$qty'},
             #                 },
             'sort_stage': {
                     'product_code':1,
                     'product_sku':1,
                     'location':1,
                     'warehouse':1,
                     'client':1,
                     'year':1,
                     'month':1,
                 }}
             }
         
     )

     query = self.query_demand_plan(product_code=None, sku=None, warehouse=None, location=None, client=None, **kwargs)
     # print('query-', simplejson.dumps(query, indent=3))
     # project = {
     #  "$project":{
     #     "_id":0,
     #     "product_code": "$_id.product_code",
     #     "product_sku": "$_id.product_sku",
     #     "year": "$_id.year",
     #     "month": "$_id.month",
     #     "location": "$_id.location",
     #     "warehouse": "$_id.warehouse",
     #     "client": "$_id.client",
     #     "qty_by_folios": "$qty_by_folios",
     #     "qty": "$qty",
     #  }
     # }
     # query.append(project)
     demand =  self.cr.aggregate(query)

     res = {}
     for d in demand:
         product_code = d['product_code']
         sku = d['product_sku']
         location = d.get('location')
         warehouse = d.get('warehouse')
         res[product_code] = res.get(product_code,{})
         res[product_code][sku] = res[product_code].get(sku,{})
         res[product_code][sku]['location'] = res[product_code][sku].get('location',{})
         res[product_code][sku]['warehouse'] = res[product_code][sku].get('warehouse',{})
         res[product_code][sku]['others'] = res[product_code][sku].get('others',[])
         if location:
             res[product_code][sku]['location'][location] = res[product_code][sku]['location'].get(location,[])
             res[product_code][sku]['location'][location].append(d)
         if warehouse:
             res[product_code][sku]['warehouse'][warehouse] = res[product_code][sku]['warehouse'].get(warehouse,[])
             res[product_code][sku]['warehouse'][warehouse].append(d)
         else:
             res[product_code][sku]['others'].append(d)
     return res

   def query_demand_plan(self, product_code=None, sku=None, warehouse=None, location=None, client=None, **kwargs):
     sort_stage = {} 
     group_stage = {}
     match_query ={ 
              'form_id': self.DEMANDA_PLAN,  
              'deleted_at' : {'$exists':False},
          }
     project_stage = {
                 '_id':1,
                 'folio':'$folio',
                 'product_code':f'$answers.{self.Product.SKU_OBJ_ID}.{self.Product.f["product_code"]}',
                 'product_sku':f'$answers.{self.Product.SKU_OBJ_ID}.{self.Product.f["sku"]}',
                 'procurment_method':f'$answers.{self.f["procurment_method"]}',
                 'procurment_status':f'$answers.{self.f["procurment_status"]}',
                 'year':f'$answers.{self.f["year"]}',
                 'month':f'$answers.{self.f["month"]}',
                 'demand_date':f'$answers.{self.f["demand_date"]}',
                 'demand_hour':f'$answers.{self.f["demand_hour"]}',
                 'qty':f'$answers.{self.f["qty"]}',
                 'qty_allocated':f'$answers.{self.f["qty_allocated"]}',
                 'qty_allocated':f'$answers.{self.f["qty_allocated"]}',
                 'alloctaion_group':f'$answers.{self.f["alloctaion_group"]}',
                 'location':f'$answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.WH.f["warehouse_location"]}',
                 'warehouse':f'$answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.WH.f["warehouse"]}',
                 'client':f'$answers.{self.CLIENTE_CAT_OBJ_ID}.{self.f["client"]}',
         }
     if kwargs.get('query'):
         query_dict = kwargs['query']
         if query_dict.get('query_replace'):
             if query_dict.get('match_stage'):
                match_query = query_dict['match_stage']

             if query_dict.get('group_stage'):
                group_stage = query_dict['group_stage']
         else:
             if query_dict.get('match_stage'):
                 match_query.update(query_dict['match_stage'])

             if query_dict.get('group_stage'):
                 group_stage.update(query_dict['group_stage'])

     match_query.update(self.Product.match_query(product_code=product_code, sku=sku) )
     match_query.update(self.WH.match_query(warehouse=warehouse, location=location) )
     # match_query.update(self.get_date_query(date_from=today, date_field_id=self.f['demand_date']))
     query = [
         {'$match': match_query},
         {'$sort': {'created_at': 1}}
     ]
     if project_stage:
         query += [{'$project':project_stage}]
     if group_stage:
         query += [{'$group':group_stage}]
     if sort_stage:
         query += [{'$sort':sort_stage}]
     return query

   def query_apply_args(self, q, **kwargs):
     match_query = self.get_aggreate_operator(q, '$match')
     project_stage = self.get_aggreate_operator(q, '$project')
     group_stage = self.get_aggreate_operator(q, '$group')
     sort_stage = self.get_aggreate_operator(q, '$sort')
     new_q = []
     if kwargs.get('query'):
         query_dict = kwargs['query']
         if query_dict.get('query_replace'):
             if query_dict.get('match_stage'):
                match_query = query_dict['match_stage']

             if query_dict.get('project_stage'):
                project_stage = query_dict['project_stage']

             if query_dict.get('group_stage'):
                group_stage = query_dict['group_stage']

             if query_dict.get('sort_stage'):
                sort_stage = query_dict['sort_stage']
         else:
             if query_dict.get('match_stage'):
                 match_query.update(query_dict['match_stage'])
             if query_dict.get('project_stage'):
                 project_stage.update(query_dict['project_stage'])

             if query_dict.get('group_stage'):
                 group_stage.update(query_dict['group_stage'])

             if query_dict.get('sort_stage'):
                 sort_stage.update(query_dict['sort_stage'])
     if match_query:
         new_q += [{'$match':match_query}]
     if project_stage:
         new_q += [{'$project':project_stage}]
     if group_stage:
         new_q += [{'$group':group_stage}]
     if sort_stage:
         new_q += [{'$sort':sort_stage}]
     return new_q

   def query_procurment_record(self, product_code=None, sku=None, warehouse=None, location=None, client=None, status=None, **kwargs):
     sort_stage = {} 
     group_stage = {}
     match_query ={ 
              'form_id': self.PROCURMENT,  
              'deleted_at' : {'$exists':False},
          }
     project_stage = {
                 '_id':0,
                 'folio':'$folio',
                 'alloctaion_group':f'$answers.{self.f["alloctaion_group"]}',
                 'product_code':f'$answers.{self.Product.SKU_OBJ_ID}.{self.Product.f["product_code"]}',
                 'product_sku':f'$answers.{self.Product.SKU_OBJ_ID}.{self.Product.f["sku"]}',
                 'date':f'$answers.{self.f["procurment_schedule_date"]}',
                 'year':f'$answers.{self.f["year"]}',
                 'month':f'$answers.{self.f["month"]}',
                 'qty':{"$ifNull": [f'$answers.{self.f["procurment_qty"]}',0]},
                 'qty_allocated':{"$ifNull": [f'$answers.{self.f["qty_allocated"]}',0]},
                 'qty_available':{"$ifNull": [f'$answers.{self.f["qty_available"]}',0]},
                 'location':f'$answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.WH.f["warehouse_location"]}',
                 'warehouse':f'$answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.WH.f["warehouse"]}',
         }
     match_query.update(self.Product.match_query(product_code=product_code, sku=sku) )
     match_query.update(self.WH.match_query(warehouse=warehouse, location=location) )
     query = [
         {'$match': match_query},
         {'$project': project_stage},
         {'$sort': {'created_at': 1}}
     ]
     query = self.query_apply_args(query, **kwargs)
     return query

   def rearage_allocations(self, procuments=[], orders=[], **kwargs):
     if not get_allocated_procuments:
         procuments = self.get_open_allocations(**kwargs)
     if not orders:
         orders = self.get_demand_needs(**kwargs)

   def run_demand_procurements(self, product_code=None, sku=None, warehouse=None, location=None, client=None, **kwargs):
     # Corre las distitnas etpas del abastacimiento
     #Compas
     #TODO
     
     #Documentos
     #TODO

     #Inventarios
     #TODO

     #Produccion

     order_needs = self.get_demand_needs(product_code=product_code, sku=sku, warehouse=warehouse, location=location, client=client, **kwargs)
     can_not = []
     can = []

     orders = self.can_allocate_orders(order_needs, **kwargs)
     print('orders=', orders)
     #print ya hizo las allocations q pudo
     #hay que actualizar los procurments asi como las orders
     self.do_allocation(orders, source_document='demand')
     self.do_update_procurments()

     order_needs = self.get_demand_needs(product_code=product_code, sku=sku, warehouse=warehouse, location=location, client=client, **kwargs)
     print('orders2=', orders)
     #hay que crear un muevo procurment.... para llenar todos los espacios... 
     #hay que crer para el mas antiguo un procurment.. y luego buscar para los otros
     #and so and dos....
     #vamos a crear un for para cada una de las ordenes vamos a correr todo el proceso....
     #1.- crear su procurment... 
     # res = self.procurment_method(procurment_obj, qty, bom)
     # self.create_procurment(can)
     # res = self.do_production(procurment_obj, qty, bom, from_procurment=True)
