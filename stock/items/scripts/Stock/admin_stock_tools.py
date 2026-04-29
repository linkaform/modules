# -*- coding: utf-8 -*-
import sys, simplejson, copy, math, requests

from stock_utils import Stock

from account_settings import *

CHUNK_SIZE = 2000
FOLIOS_URL = 'https://b2.linkaform.com/file/app-linkaform/public-client-126/58592/5fd3c8ac0b74ca8fcdea91b7/69f16d9a93c77d4ddf1eade9.txt'

class Stock(Stock):

    def read_folios_url(self, url=FOLIOS_URL):
        """Descarga el archivo de folios desde una URL, uno por línea."""
        print(f'Descargando folios desde: {url}')
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        folios = [line.strip() for line in response.text.splitlines() if line.strip()]
        print(f'Folios cargados: {len(folios)}')
        return folios

    def get_active_stock(self, folios=[]):
        query = {
            "deleted_at":{"$exists":False},
            "form_id": self.FORM_INVENTORY_ID,
            f"answers.{self.f['status']}":"active",
            }
        if folios:
            query.update({"folio": {"$in": folios}})
        res = self.cr.find(query,{'_id':1})
        # print('query', query)
        return [str(x['_id']) for x in res]

    def update_stock_status(self, url=FOLIOS_URL):
        """
        Se busca en la collecion de Stock el cache para hacer update
        """
        folios = self.read_folios_url(url)
        total = len(folios)
        total_chunks = math.ceil(total / CHUNK_SIZE)
        answer = {self.f['product_grading_pending']: 'auditado'}

        print(f'Total folios en archivo : {total}')
        print(f'Chunk size              : {CHUNK_SIZE}')
        print(f'Total chunks            : {total_chunks}')
        print('-' * 50)
        total_activos = 0
        total_parcheados = 0
        errores = []

        for i in range(0, total, CHUNK_SIZE):
            chunk = folios[i: i + CHUNK_SIZE]
            chunk_num = (i // CHUNK_SIZE) + 1
            print(f'[{chunk_num}/{total_chunks}] Folios {i+1}–{min(i+CHUNK_SIZE, total)}')

            # Filtrar en Mongo solo los que siguen activos
            activos = self.get_active_stock(chunk)
            print(f'  Activos encontrados en este chunk: {len(activos)}/{len(chunk)}')
            total_activos += len(activos)

            if not activos:
                print('  Sin activos en este chunk, saltando...')
                continue

            # Parchear solo los activos
            print(f'  Parcheando {len(activos)} registros...', end=' ')
            try:
                res = self.lkf_api.patch_multi_record(
                    answer,
                    self.FORM_INVENTORY_ID,
                    record_id=activos,
                    threading=True,
                    max_workers=24,
                )
                total_parcheados += len(activos)
                print(f'OK')
            except Exception as e:
                print(f'ERROR: {e}')
                errores.append({'chunk': chunk_num, 'activos': activos, 'error': str(e)})

        print('-' * 50)
        print(f'Proceso terminado.')
        print(f'  Activos encontrados  : {total_activos}')
        print(f'  Parcheados           : {total_parcheados}')
        print(f'  Chunks con error     : {len(errores)}')
        if errores:
            print('  Chunks fallidos      :', [e['chunk'] for e in errores])
        return errores


        # records = self.get_active_stock()
        # print('va a acutalziar ', len(records), 'registros...')
        # answer = {self.f['product_grading_pending']:'auditado'}
        # res = self.lkf_api.patch_multi_record(answer, self.FORM_INVENTORY_ID,  record_id=records, threading=True, max_workers=24)
        # return res


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    option = stock_obj.data.get("option",'')
    print('option', option)
    if option == 'update_status':
        print('corriendo update')
        response = stock_obj.update_stock_status()
    print('Termina....')



