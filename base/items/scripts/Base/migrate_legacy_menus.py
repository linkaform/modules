# coding: utf-8
import sys, simplejson
from concurrent.futures import ThreadPoolExecutor, as_completed
from linkaform_api import settings
from account_settings import *

from migrate_menus import Accesos

class Accesos(Accesos):
    """
    Version corregida de Accesos.migrate_menus() (ver migrate_menus.py):
    - get_menus_x_user() estaba hardcodeado a un solo usuario ('$in': [10]).
    - migrate_menus() siempre creaba con post_forms_answers, sin revisar si
      el usuario ya tenia un registro en CONFIGURACION_MENUS -- corriendolo
      dos veces para el mismo usuario dejaba un registro duplicado (ver
      clave10_menus_registro_duplicado_ultimo_gana).
    Vive en un archivo aparte para no tocar migrate_menus.py, que tiene otras
    utilidades sin relacion con esta migracion.
    """

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def get_menus_x_users(self, user_ids=None):
        """
        Igual que get_menus_x_user(), pero sin el usuario hardcodeado: por
        default trae TODOS los usuarios con registro en CONFIGURACION_ACCESOS
        (via get_users_ids, ya excluye la cuenta padre), o solo los que se
        pasen explicitamente en user_ids.
        """
        target_ids = user_ids if user_ids is not None else self.get_users_ids()
        if not target_ids:
            return []

        query = [
            {"$match": {
                "form_id": self.CONF_ACCESOS,
                "deleted_at": {"$exists": False},
                f"answers.{self.EMPLOYEE_OBJ_ID}.{self.mf['id_usuario']}": {'$in': target_ids}
            }},
            {"$project": {
                "_id": 0,
                "username": f"$answers.{self.EMPLOYEE_OBJ_ID}.{self.mf['username']}",
                "user_id": f"$answers.{self.EMPLOYEE_OBJ_ID}.{self.mf['id_usuario']}",
                "menus": f"$answers.{self.conf_accesos_fields['menus']}",
            }},
            {"$group": {
                "_id": "$username",
                "user_id": {"$first": "$user_id"},
                "menus":   {"$first": "$menus"},
            }},
            {"$project": {
                "_id": 0,
                "username": "$_id",
                "user_id": 1,
                "menus": 1,
            }}
        ]
        return self.format_cr(self.cr.aggregate(query))

    def _existing_menu_record_id(self, user_id):
        query = [
            {"$match": {
                "form_id": self.MENUS_FORM,
                "deleted_at": {"$exists": False},
                f"answers.{self.USUARIOS_OBJ_ID}.{self.menu_form_fields['usuario_id']}": user_id,
            }},
            {"$sort": {"_id": -1}},
            {"$limit": 1},
            {"$project": {"_id": 1}},
        ]
        record = self.format_cr(self.cr.aggregate(query), get_one=True)
        return record.get('_id') if record else None

    def _process_legacy_user(self, user, dry_run):
        """
        Migra un solo usuario. No comparte estado mutable con otros hilos
        (cada llamada arma su propio `metadata`/`answers`), para poder
        correrse dentro de un ThreadPoolExecutor sin condiciones de carrera.
        """
        username = user['username']
        user_id = self.unlist(user['user_id'])
        menus = user['menus'] or []

        new_menus = []
        for menu in menus:
            new_menus.extend(self.PERMISSION_MODULE_MAP.get(menu, []))
        details_menus = self.get_format_user_menus(filter_keys=new_menus)

        clear_menus = []
        for menu in details_menus:
            clear_menus.append({
                f"{self.MENUS_CATALOG_OBJ_ID}": {
                    self.menu_form_fields['menu']: menu['menu'],
                    self.menu_form_fields['seccion']: menu['seccion'],
                    self.menu_form_fields['elemento']: menu['elemento'],
                    self.menu_form_fields['key']: [menu['key']],
                    self.menu_form_fields['plataforms']: [menu['plataforms']],
                }
            })

        answers = {
            self.USUARIOS_OBJ_ID: {
                self.mf['username']: username,
                self.mf['id_usuario']: [user_id],
            },
            self.menu_form_fields['elementos']: clear_menus,
        }

        existing_id = self._existing_menu_record_id(user_id)
        action = "update" if existing_id else "create"

        if dry_run:
            return {
                "user_id": user_id,
                "username": username,
                "action": action,
                "menu_count": len(clear_menus),
            }

        metadata = self.lkf_api.get_metadata(form_id=self.MENUS_FORM)
        metadata.update({'answers': answers})
        if existing_id:
            metadata['_id'] = existing_id
            res = self.net.patch_forms_answers(metadata)
        else:
            metadata.pop('_id', None)
            res = self.lkf_api.post_forms_answers(metadata)
        return {
            "user_id": user_id,
            "username": username,
            "action": action,
            "status_code": res.get('status_code'),
            "error": res.get('json') or res.get('content'),
        }

    def migrate_legacy_menus(self, user_ids=None, dry_run=False):
        """
        Migra CONFIGURACION_ACCESOS (legacy) a CONFIGURACION_MENUS para todos
        los usuarios con registro legacy (o solo los indicados en user_ids).
        Actualiza el registro existente si el usuario ya tenia uno en
        CONFIGURACION_MENUS, en vez de crear uno duplicado.

        Corre un usuario a la vez tarda lo mismo sin importar cuantos
        usuarios se pidan (costo fijo alto por invocacion) mas 1
        search_catalog secuencial por usuario -- con cuentas grandes esto
        choca con el mismo timeout de gateway que resync_all_permissions.
        Se paralela por usuario igual que esa funcion.
        """
        menus_x_user = self.get_menus_x_users(user_ids=user_ids)

        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self._process_legacy_user, user, dry_run): user
                for user in menus_x_user
            }
            for future in as_completed(futures):
                user = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    results.append({
                        "user_id": self.unlist(user.get('user_id')),
                        "username": user.get('username'),
                        "action": "error",
                        "error": str(e),
                    })

        return results


if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv)
    script_obj.console_run()
    data = script_obj.data.get('data', {})

    user_ids = data.get('user_ids')
    dry_run = bool(data.get('dry_run'))

    response = script_obj.migrate_legacy_menus(user_ids=user_ids, dry_run=dry_run)
    script_obj.HttpResponse({"data": response})
