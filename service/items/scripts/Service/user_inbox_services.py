# -*- coding: utf-8 -*-
import sys, simplejson, time

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
   
    def delete_inboxes(self, user_id):
        inboxes = self.to_delete
        res = self.lkf_api.delete_users_inbox(user_id, inboxes, threading=False)
        return res

    def eval_inbox(self, inbox):
        answers = inbox.get('doc',{}).get('record_json',{}).get('answers')
        program_date = answers.get(self.f['first_date'])
        due_date = answers.get(self.f['due_date'])
        today = time.time()
        epoch_program_date = self.date_2_epoch(program_date)
        epoch_due_date = self.date_2_epoch(due_date)
        work_window = epoch_due_date - epoch_program_date
        time_left = epoch_due_date  - today
        to_delete = []
        if time_left < 0:
            # la tarea ya cumplio con su due date.. hay que borrrla
            self.to_delete.append(inbox)
        if work_window > 259200:
            #verifica si la programacion es mayor 3 dias 
            #3 dias == a 259200 segundos
            if time_left > 86400 and time_left < 82800:
                # faltan entre 23 y 24 hr para ejecutar la tarea y no se ha 
                # realizado.. enviar notificacion
                self.send_1_day_notification.append(inbox)
        if time_left >  1425  and time_left < 7200:
            # faltan entre 23.75 y 48 hrs para que se vensa 
            self.send_1_hr_notification.append(inbox)

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

    def send_notification(self, user, inboxes, due_time=""):
        result = []
        self.inboxes_groups = []
        user_data = self.lkf_api.get_user_by_id(user)
        if due_time:
            due_time = f"en {due_time} hora(s)"
        data = {
            f"{self.envio_correo_fields['email_from']}":'noreplay@linkaform.com',
            f"{self.envio_correo_fields['titulo']}":f"Tienes {len(inboxes)} actividades pendientes {due_time}",
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
            doc = inbox['doc']
            # print('inbox', simplejson.dumps(doc, indent=4))
            name = doc.get('name')
            name = doc.get('name')
            record_json = doc.get('record_json')
            folio = record_json.get('folio')
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

    def search_inboxes(self):
        sup_users = self.supe_users()
        del_user_inbox = {}
        sup_users = [13840,10975]
        user_inbox = self.lkf_api.get_user_inbox(sup_users, threading=True)
        for user_id, inboxes in user_inbox.items():
            self.to_delete = []
            self.send_1_hr_notification = []
            self.send_1_day_notification = []
            print('==================================')
            print('Seaching Inbox of user: ', user_id)
            new_inbox = self.get_inbox_by_status(user_id, inboxes, status='new')
            for inbox in new_inbox:
                self.eval_inbox(inbox)
            self.delete_inboxes(user_id)
            print('TO DELETE: ', len(self.to_delete))
            print('TO send_1_hr_notification: ', len(self.send_1_hr_notification))
            print('TO send_1_day_notification: ', len(self.send_1_day_notification))
            if self.send_1_hr_notification:
                self.send_notification(user_id, self.send_1_hr_notification, due_time="1")
            if self.send_1_day_notification:
                self.send_notification(user_id, self.send_1_day_notification, due_time="24")




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
    user_inbox = service_obj.search_inboxes()

