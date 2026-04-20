# -*- coding: utf-8 -*-
import sys, simplejson, random
import threading
from stock_utils import Stock

from account_settings import *


class Stock(Stock):
    """
    Stock management utility class that extends the base Stock class.
    Provides additional functionality for inventory management, stock movements,
    and ONTs (Order Number Tracking) processing.
    
    Attributes:
        proceso_onts (bool): Flag indicating if ONTs processing is enabled
        FORM_CATALOG_DIR (dict): Mapping between form IDs and catalog IDs
        f (dict): Form field mappings
        answer_label (dict): Labels for form answers
        FOLDER_FORMS_ID (str): ID of the stock forms folder
        mf (dict): Media field mappings
        max_sets (int): Maximum number of records per set for processing
    """

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        """
        Initialize the Stock utility class.
        
        Args:
            settings (dict): Configuration settings
            folio_solicitud (str, optional): Request folio number
            sys_argv (list, optional): System arguments
            use_api (bool, optional): Flag to indicate API usage
            **kwargs: Additional keyword arguments
        """
        super().__init__(settings, folio_solicitud=folio_solicitud, sys_argv=sys_argv, use_api=use_api)



    def ont_lot_match(self, product_lot):
        match_query ={ 
         'form_id': self.FORM_INVENTORY_ID,  
         'deleted_at' : {'$exists':False},
         } 
        if type(product_lot) == str:
            match_query.update({f"answers.{self.f['product_lot']}": product_lot})
        elif type(product_lot) == list:
            match_query.update({f"answers.{self.f['product_lot']}":{"$in": product_lot}})

        return match_query

    def get_onts_status(self, onts):
        """
        Busca las ONTs en la base de datos y las agrupa por status:
        - not_found:     el serial no existe en el inventario
        - not_available: existe pero su inventory_status es 'done'
        - active:        existe y su inventory_status es 'active'
        """
        match_query = self.ont_lot_match(onts)
        # cols = {
        #     f'answers.{self.f["lot_number"]}': 1,
        #     f'answers.{self.f["inventory_status"]}': 1,
        #     f'answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.WH.f["warehouse"]}': 1,
        #     f'answers.{self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.WH.f["warehouse_location"]}': 1,
        # }
        cr_data = self.cr.find(match_query, {'answers':1, 'folio':1})

        # Indexamos los resultados por serial -> lista de registros
        found = {}
        records = {}
        for doc in cr_data:
            answers = doc.get('answers', {})
            serial  = answers.get(self.f['lot_number'])
            status  = answers.get(self.f['inventory_status'])
            if not serial:
                continue
            if serial not in found:
                found[serial] = []
            found[serial].append(status)
            records[serial] = doc
            if records[serial].get('_id'):
                records[serial]['_id'] = str(records[serial].get('_id'))

        result = {
            'not_found':     [],
            'not_available': [],
            'active':        {},
        }

        for ont in onts:
            if ont not in found:
                result['not_found'].append(ont)
            elif 'active' in found[ont]:
                # Tiene al menos un registro activo
                result['active'][ont] = records[ont]
            else:
                # Solo registros 'done'
                result['not_available'].append(ont)

        return result

    def onr_lot_match(self, product_lot):
        match_query ={ 
         'form_id': self.FORM_INVENTORY_ID,  
         'deleted_at' : {'$exists':False},
         } 
        if type(product_lot) == str:
            match_query.update({f"answers.{self.f['product_lot']}": product_lot})
        elif type(product_lot) == list:
            match_query.update({f"answers.{self.f['product_lot']}":{"$in": product_lot}})

        return match_query

    def warehouse_out_onts(self, onts):
        # ── 1. Agrupar ONTs por status ──────────────────────────────────────────
        ont_status = self.get_onts_status(list(onts.keys()))

        # ── 2. Buscar destino: almacén tipo 'Client' (Producción) ───────────────
        dest_warehouse_info = self.WH.get_warehouse(warehouse_type='Client')
        if len(dest_warehouse_info) > 0:
            dest_warehouse = dest_warehouse_info[0]
        else:
            self.LKFException("No se econtro almance de tipo Cliente")
        dest_location = self.WH.get_warehouse_locations(dest_warehouse)
        if len(dest_location) > 0:
            dest_location = dest_location[0]
        else:
            self.LKFException("No se econtro Ubicaciones definias en el almance: ", dest_warehouse)

        # ── 3. Agrupar ONTs activas por almacén origen ──────────────────────────
        # key: (from_warehouse, from_location) -> lista de (serial, answers)
        groups = {}
        not_available = list(ont_status['not_available'])

        for serial, doc in ont_status['active'].items():
            answers = doc.get('answers', {})

            if answers.get(self.f['inventory_status']) != 'active':
                not_available.append(serial)
                continue

            wh_info        = answers.get(self.WH.WAREHOUSE_LOCATION_OBJ_ID, {})
            from_warehouse = wh_info.get(self.WH.f['warehouse'])
            from_location  = wh_info.get(self.WH.f['warehouse_location'])

            key = (from_warehouse, from_location)
            if key not in groups:
                groups[key] = []
            groups[key].append((serial, answers))

        # ── 4. Estructuras compartidas protegidas con Lock ──────────────────────
        response = {}
        lock     = threading.Lock()

        def move_group_by_warehouse(from_warehouse, from_location, items):
            """
            Hilo individual: crea UNA salida de almacén con todas las ONTs
            que comparten el mismo almacén origen.
            items: lista de (serial, answers)
            """
            move_group_lines = []
            for serial, answers in items:
                move_group_lines.append({
                    self.CATALOG_INVENTORY_OBJ_ID: {
                        self.f['product_code']: answers[self.Product.SKU_OBJ_ID][self.f['product_code']],
                        self.f['product_sku']:  answers[self.Product.SKU_OBJ_ID][self.f['product_sku']],
                        self.f['lot_number']:   serial,
                    },
                    self.f['move_group_qty']:          1,
                    self.f['folio_produccion']:        onts[serial].get('folio_produccion'),
                    self.f['telefono']:                onts[serial].get('telefono'),
                    self.f['tipo_de_tarea']:           onts[serial].get('tipo_de_tarea'),
                    self.f['expediente_de_tecnico']:   onts[serial].get('expediente_de_tecnico'),
                    self.f['nombre_del_contratista']:  onts[serial].get('nombre_del_contratista'),
                    self.f['socio_comercial']:         onts[serial].get('socio_comercial'),
                    self.f['division']:                onts[serial].get('division'),
                    self.f['area']:                    onts[serial].get('area'),
                    self.f['cope']:                    onts[serial].get('cope'),
                })

            new_record = {
                self.f['inventory_status']:    'active',
                self.f['stock_status']:        'to_do',
                self.f['stock_move_comments']: 'SALIDA DESDE PRODUCCION',
                self.f['fecha_recepcion']:     self.today_str(date_format='datetime'),
                self.WH.WAREHOUSE_LOCATION_OBJ_ID: {
                    self.WH.f['warehouse']:          from_warehouse,
                    self.WH.f['warehouse_location']: from_location,
                },
                self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID: {
                    self.WH.f['warehouse_dest']:          dest_warehouse,
                    self.WH.f['warehouse_location_dest']: dest_location,
                },
                self.f['move_group']: move_group_lines,
            }

            metadata = self.lkf_api.get_metadata(self.STOCK_ONE_MANY_ONE)
            metadata['answers'] = new_record
            metadata['folio']   = f'PROD-{str(int(random.random()*1000))}'
            res = self.lkf_api.post_forms_answers(metadata)

            # Registrar la respuesta para cada serial del grupo
            with lock:
                for serial, _ in items:
                    response[serial] = res

        # ── 5. Lanzar UN hilo por almacén origen ───────────────────────────────
        threads = []
        for (from_warehouse, from_location), items in groups.items():
            print(f'Almacén origen: {from_warehouse} / {from_location} — {len(items)} ONTs')
            t = threading.Thread(
                target=move_group_by_warehouse,
                args=(from_warehouse, from_location, items)
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # ── 6. Respuesta del web service ────────────────────────────────────────
        return {
            'moved':         response,
            'not_available': not_available,
            'not_found':     ont_status['not_found'],
        }

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    data = stock_obj.data.get('data', {})
    onts = data.get("onts",[])
    onts =  {
        "HWTC6D8856B5":{
            "folio_produccion":str(int(random.random()*1000)),
            "telefono":"132465790",
            "tipo_de_tarea":"TIPO DE TAREA 1 ",
            "expediente_de_tecnico":"EXPEDIENTE DEL TECNICO 1",
            "nombre_del_contratista":"NOMBRE DEL CONTRTAISTA 1",
            "socio_comercial":"SOCIO COMERICAL 1",
            "division":"DIVISION 1",
            "area":"AREA 1",
            "cope":"COPE 1",
            },
        "HWTC2F548BB6":{
            "folio_produccion":str(int(random.random()*1000)),
            "telefono":"2222222222",
            "tipo_de_tarea":"TIPO DE TAREA 2",
            "expediente_de_tecnico":"EXPEDIENTE DEL TECNICO 2",
            "nombre_del_contratista":"NOMBRE DEL CONTRTAISTA 2",
            "socio_comercial":"SOCIO COMERICAL 2",
            "division":"DIVISION 2",
            "area":"AREA 2",
            "cope":"COPE 2",
            },
        "HWTC8F50C4B5":{
            "folio_produccion":str(int(random.random()*1000)),
            "telefono":"133333333",
            "tipo_de_tarea":"TIPO DE TAREA 3",
            "expediente_de_tecnico":"EXPEDIENTE DEL TECNICO 3",
            "nombre_del_contratista":"NOMBRE DEL CONTRTAISTA 3",
            "socio_comercial":"SOCIO COMERICAL 3",
            "division":"DIVISION 3",
            "area":"AREA 3",
            "cope":"COPE 3",
            }, 
        }
    response = stock_obj.warehouse_out_onts(onts)
    print('response', simplejson.dumps(response))
    sys.stdout.write(simplejson.dumps(response))
    
