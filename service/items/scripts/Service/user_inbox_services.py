# -*- coding: utf-8 -*-
import sys, simplejson

from service_utils import Service

from account_settings import *


class Service(Service):

    def delete_all_new_inbox(self, status='new'):
        sup_users = self.supe_users()
        del_user_inbox = {}

        user_inbox = self.lkf_api.get_user_inbox(sup_users, threading=True)
        for user_id, inboxes in user_inbox.items():
            new_inbox = self.get_inbox_by_status(user_id, inboxes, status='new')
            if new_inbox:
                inbox_to_delete = [i['key'] for i in new_inbox]
                if inbox_to_delete:
                    del_user_inbox[user_id] = inbox_to_delete
        del_res = self.delete_inboxes(del_user_inbox)
        return del_res
   
    def delete_inboxes(self, inboxes):
        print('========== DELETE ======')
        print('inboxes=', inboxes)
        res = self.lkf_api.delete_users_inbox(inboxes, threading=True)
        return res

    def get_inbox_by_status(self, user_id, inboxes, status='new'):
        if not inboxes:
            print('no inobx foud')
            return []
        res = []
        for inbox in inboxes:
            doc = inbox.get('doc',{})
            if doc.get('status') and doc['status'] == status:
                res.append(inbox)
        return res

    def notify_all_new_inbox(self, status='new'):
        # self.load('Stock', **self.kwargs)
        sup_users = self.supe_users()
        user_inbox = self.lkf_api.get_user_inbox(sup_users, threading=True)
        for user_id, inboxes in user_inbox.items():
            new_inbox = self.get_inbox_by_status(user_id, inboxes, status='new')
            if new_inbox:
                self.send_notification(user_id, new_inbox)

    def send_notification(self, user, inboxes):
        result = []
        user_data = self.lkf_api.get_user_by_id(user)
        data = {
            f"{self.envio_correo_fields['email_from']}":'noreplay@linkaform.com',
            f"{self.envio_correo_fields['titulo']}":f"Tienes {len(inboxes)} actividades pendientes",
            f"{self.envio_correo_fields['nombre']}":user_data['first_name'],
            f"{self.envio_correo_fields['email_to']}":user_data['email'],
            f"{self.envio_correo_fields['msj']}": 'Mensaje Automatico sobre Notificaciones'
        }
        data = {
            'email_from':'noreplay@linkaform.com',
            'titulo':f"Tienes {len(inboxes)} actividades pendientes",
            'nombre':user_data['first_name'],
            'email_to':user_data['email'],
            'mensaje': 'Mensaje Automatico sobre Notificaciones'
        }
        for inbox in inboxes:
            # print('inbox', simplejson.dumps(inbox, indent=4))
            doc = inbox['doc']
            folio = doc.get('folio')
            name = doc.get('name')
            name = doc.get('name')
            record_json = doc.get('record_json')
            answers = record_json.get('answers')
            # print('answers==s', answers)
            localidad_cat = answers.get(self.LOCALIDADES_OBJ_ID,{})
            localidad = localidad_cat.get(self.mf['localidad'])
            inbox_id = inbox['id']
            protocol = self.settings.config['PROTOCOL']
            host = self.settings.config['HOST']
            host = self.settings.config['HOST']
            url = f'{protocol}://{host}/#/records/detail/{inbox_id}'
            set_data ={
                    f"{self.mf['folio']}": folio,
                    f"{self.mf['url']}":url,
                    f"{self.mf['forma']}":doc['title'],
                    f"{self.mf['comentarios']}":'Recordatorio de Realizar Auditoria'
                }
            self.inboxes_groups.append(set_data)
        res = self.send_email_by_form(data)
        result.append(res)
        return result

    def supe_users(self):
        users = self.lkf_api.get_supervised_users()
        # users = [
        #     {'user_id':15360},
        #     {'user_id':11381},
        #     {'user_id':15481},
        #     ]
        del_inbox = []
        sup_users = []
        for user in users:
            user_id = user['user_id']
            sup_users.append(user_id)

        return sup_users

if __name__ == '__main__':
    service_obj = Service(settings, sys_argv=sys.argv)
    service_obj.console_run()
    #current record
    data = service_obj.data.get('data',{})
    option = data.get("option", service_obj.data.get('option'))
    print('option', option)
    if option == 'delete':
        user_inbox = service_obj.delete_all_new_inbox()
        print('catalog user_inbox to delete', user_inbox)
    elif option == 'notify':
        user_inbox = service_obj.notify_all_new_inbox()
        print('catalog user_inbox to delete', user_inbox)

