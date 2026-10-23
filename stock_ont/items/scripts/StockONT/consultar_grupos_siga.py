# -*- coding: utf-8 -*-
import sys
from stock_ont_utils import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def consultar_grupos_siga(self):
        """
        Itera todos los grupos de administracion de SIGA e imprime el nombre
        del grupo y los usuarios que lo integran.

        Returns:
            dict: {role: {'id_group': id, 'users': {id_usuario: username}}}
        """
        all_groups = (
            self.siga_admin_groups +
            self.siga_wh_manager_groups +
            self.siga_wh_operator_groups +
            self.siga_wh_admin_assistant_groups
        )

        resultado = {}
        for role, id_group in all_groups:
            users = self._get_users_group(id_group)
            print(f"\n=== {role} (grupo {id_group}) - {len(users)} usuario(s)")
            for user_id, username in users.items():
                print(f"    {user_id}: {username}")
            resultado[role] = {'id_group': id_group, 'users': users}

        return resultado

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_grupos = stock_obj.consultar_grupos_siga()
    stock_obj.HttpResponse({"data": resp_grupos})
