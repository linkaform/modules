# -*- coding: utf-8 -*-
"""
Variante de pci_get_connection_db.py que no depende de lkfpwd: la contraseña
de Mongo la obtiene linkaform_api desde la API (get_mongo_password) usando el
usuario y API key de la cuenta destino.
"""
from linkaform_api import network

class Contratista:
    def __init__(self, config):
        self.config = config

class CollectionConnection:
    def __init__(self, connection_id, settings_instance, username, api_key):
        # Copia superficial: config trae objetos con locks (clientes/conexiones) que deepcopy
        # no puede copiar. Solo se reemplazan llaves de primer nivel, asi que basta.
        config = dict(settings_instance.config)

        # Si se queda el MONGODB_URI de la cuenta actual, se conecta con esas credenciales
        config.pop('MONGODB_URI', None)
        config.update({
            'ACCOUNT_ID': connection_id,
            'MONGODB_USER': 'account_{}'.format(connection_id),
            'MONGODB_PASSWORD': '', # vacio = linkaform_api la pide a la API
            'USERNAME': username,
            # do_get busca primero API_KEY y luego APIKEY, se ponen las dos
            'API_KEY': api_key,
            'APIKEY': api_key,
        })

        # Creamos una conexión de red con la configuración de la cuenta destino
        self.net_contratista = network.Network(Contratista(config))

        # Obtenemos las colecciones y las almacenamos en un atributo
        self.collection_contratista = self.net_contratista.get_collections()

    def get_collections_connection(self):
        """ Metodo para obtener las colecciones """
        return self.collection_contratista
