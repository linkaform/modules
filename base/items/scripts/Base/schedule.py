# -*- coding: utf-8 -*-
import sys, simplejson
from datetime import datetime, timedelta
from linkaform_api import settings, network, utils
from bson import errors, ObjectId

from lkf_addons.addons.base.app import Schedule

from account_settings import *


# USER_CATALOG_ID = '66b3ea91656abce3d1a07e89'
# GROUP_CATALOG_ID = '66b3eb088558cc0bb3a07e96'
# month_dict = {
#     'enero':1,
#     'febrero':2,
#     'marzo':3,
#     'abril':4,
#     'mayo':5,
#     'junio':6,
#     'julio':7,
#     'agosto':8,
#     'septiembre':9,
#     'octubre':10,
#     'noviembre':11,
#     'diciembre':12,
# }

# def get_schedule_config(answers):
#     is_recurrent = answers.get('abcde0001000000000010006')
#     print('is recurrent', is_recurrent)
#     repeat_every = answers.get('abcde0001000000000010007')
#     happens_every = answers.get('abcde0001000000000010009')
#     first_date = answers.get('abcde0001000000000010001')
#     f_date = datetime.strptime(first_date, '%Y-%m-%d %H:%M:%S')

#     minute = answers.get('abcde0001000000000010010', f_date.minute)
#     repeats_every_xminute = answers.get('abcde0001000000000010011')
#     hour = answers.get('abcde0001000000000010012', f_date.hour)
#     repeats_every_xhour = answers.get('abcde0001000000000010013')

#     week_day = answers.get('abcde0001000000000010014', f_date.weekday)
#     week = answers.get('abcde0001000000000010015')
#     day_of_month = answers.get('abcde0001000000000010016', f_date.day)
#     repeats_eveyr_xday = answers.get('abcde0001000000000010017')
#     on_month = answers.get('abcde0001000000000010018', f_date.month)
#     repeats_eveyr_xmonth = answers.get('abcde0001000000000010019')

#     frecuency = {}
#     print('happens_every', happens_every)
#     if is_recurrent == 'cuenta_con_una_recurrencia':

#         if repeat_every == 'configurable':
#             if 'minuto' in happens_every and minute >= 0:
#                 frecuency['every_minute'] = minute
#                 if repeats_every_xminute:
#                     frecuency['every_other_minute'] = repeats_every_xminute

#             if 'hora' in happens_every and hour >= 0:
#                 frecuency['every_hour'] = hour
#                 if repeats_every_xhour:
#                     frecuency['every_other_hour'] = repeats_every_xhour

#             if 'dia_de_la_semana' in happens_every:
#                 frecuency['every_week_day'] = []
#                 if 'domingo' in week_day:
#                     frecuency['every_week_day'].append('sunday')
#                 if 'lunes' in week_day:
#                     frecuency['every_week_day'].append('monday')
#                 if 'martes' in week_day:
#                     frecuency['every_week_day'].append('tuesday')
#                 if 'miercoles' in week_day:
#                     frecuency['every_week_day'].append('wednesday')
#                 if 'jueves' in week_day:
#                     frecuency['every_week_day'].append('thursday')
#                 if 'viernes' in week_day:
#                     frecuency['every_week_day'].append('friday')
#                 if 'sabado' in week_day:
#                     frecuency['every_week_day'].append('saturday')
#                 print('week--------------------', week)
#                 if week:
#                     if week == 'primer_semana_del_mes':
#                         frecuency['week_number'] = 1
#                     if week == 'segunda_semana_del_mes':
#                         frecuency['week_number'] = 2
#                     if week == 'tercera_semana_del_mes':
#                         frecuency['week_number'] = 3
#                     if week == 'cuarta_semana_del_mes':
#                         frecuency['week_number'] = 4
#                     if week == 'quinta_semana_del_mes':
#                         frecuency['week_number'] = 5
#                 print('frecuency', frecuency)
#                 if not frecuency['every_week_day']:
#                     frecuency.pop('every_week_day')
#             if 'dia_del_mes' in happens_every and day_of_month:
#                 frecuency['every_day'] = day_of_month
#                 frecuency['every_other_day'] = repeats_eveyr_xday

#             if 'mes' in happens_every:
#                 if on_month:
#                     frecuency['every_month'] = month_dict[on_month]
#                     print(frecuency)
#                 else:
#                     month = datetime.strptime(first_date, '%Y-%m-%d %H:%M:%S').month
#                     frecuency['every_month'] = month
#                 frecuency['every_other_month'] = repeats_eveyr_xmonth
#         else:
#             if repeat_every == 'hora':
#                 frecuency['hourly'] = True
#             elif repeat_every == 'diario':
#                 frecuency['daily'] = True
#             elif repeat_every == 'semana':
#                 frecuency['weekly'] = True
#             elif repeat_every == 'mes':
#                 frecuency['monthly'] = True
#             elif repeat_every == 'año':
#                 frecuency['yearly'] = True
#             at_beginning = answers.get('abcde0001000000000010008')
#             if at_beginning == 'a_principio_del_periodo':
#                 frecuency['at_beginning'] = True
#     else:
#         frecuency['once'] = True
#     print('frec', frecuency)
#     return frecuency

# def get_answers_map(task, description, first_date, due_date, status, field_map, group_field_map ):
#     answers = {}
#     if task:
#         answers.update({'abcde0001000000000000006':task})
#     if description:
#         answers.update({'abcde0001000000000000007':description})
#     if first_date:
#         answers.update({'fffff0001000000000000001':first_date})
#     if due_date:
#         answers.update({'fffff0001000000000000002':due_date})    
#     if status:
#         answers.update({'abcde0001000000000000020':status.lower().replace(' ', '_')})
#     print('status', status)
#     print('answers', answers)
#     print('group_field_map', group_field_map)
#     group_answers = group_field_map.get('abcde0001000000000000008',[])
#     group_field_ans_map = []
#     field_ans_map = {}
#     for ans in group_answers:
#         question = ans.get('abcde0001000000000000009')
#         print('question', question)
#         try:
#             is_field = ObjectId(question)
#             print('is_field', is_field)
#             field_type = ans.get('abcde0001000000000000010')
#             print('field_type', field_type)
#             if 'contestar_respuesta' in field_type:
#                 print('es un Respuesta')
#                 field_ans_map[question] = ans.get('abcde0001000000000000011')    

#             if 'subir/descargar_imagen' in field_type:
#                 print('es un Imagen')
#                 field_ans_map[question] = ans.get('abcde0001000000000000014')

#             if 'subir/descargar_documento' in field_type:
#                 print('es un documento')
#                 field_ans_map[question] = ans.get('abcde0001000000000000013')

#             if 'ingresar_cantidad' in field_type:
#                 print('es un Cantidad')
#                 field_ans_map[question] = float(ans.get('abcde0001000000000000011'))

#             if 'ingresar_fecha' in field_type:
#                 print('es un Fecha')
#                 field_ans_map[question] = ans.get('abcde0001000000000000012')

#         except errors.InvalidId:
#             is_field = False
#             group_field_ans_map.append(ans)
#     if field_ans_map:
#         answers.update(field_ans_map)
#     if group_field_ans_map:
#         print('group_field_ans_map=',group_field_ans_map)
#         answers.update({'abcde0001000000000000008':group_field_ans_map})
#     print('field_ans_map=',field_ans_map)
#     return answers

# def get_script_map():
#     return {}

# def get_form_fileshare(item_id):
#     shared_users = lkf_api.get_form_users(item_id)
#     user_ids = [user['id'] for user in shared_users if user.get('id')]
#     return user_ids

# def update_users(all_users, new_users):
#     ids = []
#     for x in all_users:
#         if not x.get('user_id'):
#             x['user_id'] = x['id']
#         ids.append(x['user_id'])
#     if type(new_users) == dict:
#         new_users = [new_users,]
#     for x in new_users:
#         if x.get('id') and x.get('id') not in ids:
#             x['user_id'] = x['id']
#             all_users.append(x)
#         elif x.get('user_id') and x.get('user_id') not in ids :
#             all_users.append(x)

#     return all_users

# def convert_usr_id_to_dict(user_ids):
#     user_info = []
#     for user_id in user_ids:
#         user_data = lkf_api.get_user_by_id(user_id)
#         if user_data:
#             user_info.append({
#                 'account_id': user_data.get('parent_info',{}).get('id'), 
#                 'user_id': user_id, 
#                 'name': user_data.get('name'),
#                 'username': user_data.get('email'),
#                 'email': user_data.get('email'), 
#                 'resource_kind': 'user', 
#                 })
#     return user_info

# def get_record_from_db(item_id, folio):
#     query = {
#         'item_id': item_id,
#         'folio': folio,
#         'deleted_at': {'$exists': False}
#     }
#     select_columns = {'folio':1,'user_id':1,'item_id':1,'answers':1,'_id':1,'connection_id':1,'created_at':1,'other_versions':1,'timezone':1}
#     record_found = cr.find(query, select_columns)
#     return record_found.next()

# def delete_cron(item_id=None, folio=None, cron_id=None):
#     if not cron_id:
#         current_record = get_record_from_db( item_id, folio )
#         answers = current_record.get('answers')
#         cron_id = answers.get('abcde0001000000000000000')
#         if not cron_id:
#             msg_error_app = {
#                 "error":{"msg": ["No CronId found on folio {}!!".format(folio)],
#                     "label": "Cron Id",
#                     "error":["No CronId found on folio {}!!".format(folio)]},
#             }
#             return msg_error_app
#     res = lkf_api.delete_cron(cron_id)
#     res.update({'deleted': True})
#     return res

# def calc_date_as_function(first_date, tz_offset, timeframe =None, timeframe_unit=None):
#     #TODO calcular funcion para que airflow calucle el tiempo
#     #ejemplo calc_date('$today', 1 , 'mes')
#     #
#     '''
#     Como por defult se correo en UTC+0, se toma en cuenta que la fecha incio de la tarea.
#     OJO esta no es la fehca cuando se ejecuta procesa si no la fecha compromiso q ve el usuario
#     se adelante al huzo horario, como en se va a correr con un {% today %} se tiene q agrear 5 horas
#     '''
#     time_offset = '1970-01-01 00:00:00'
#     if timeframe and timeframe_unit:
#         time_offset = calc_date(time_offset , timeframe , timeframe_unit)
#     time_offset = calc_date(time_offset , tz_offset , 'minutes')
#     due_epoch = datetime.strptime(time_offset, '%Y-%m-%d %H:%M:%S')
#     seconds = int(due_epoch.strftime('%s'))
#     hours = int(seconds / 3600)
#     first_date = '{% ' + ' $today + $hours + {}'.format(hours) + ' %}'
#     return first_date

# def calc_date(first_date, timeframe, timeframe_unit, operator='+'):
#     '''
#     Recibe una fecha como string y suma o resta segun sea configurado
#     Regresa una fecha

#     '''
#     print('------------------------------------------')
#     print('asi entra trime frame',timeframe )
#     if first_date.find('$') >= 0:
#         first_date = calc_funcint(first_date, return_hour=True)
#     due_date = ''
#     # first_date = '2024-01-01 00:00:00'
#     first_date_dt = datetime.strptime(first_date, '%Y-%m-%d %H:%M:%S')
#     if timeframe < 0 and operator == '+':
#         operator = '-'
#     elif timeframe < 0 and operator == '-':
#         operator = '+'
#     if timeframe_unit in ('segundos', 'seconds', 'sec'):
#         print('asi esta el operador', operator)
#         print('timeframe', timeframe)
#         if operator == '-':
#             due_date = first_date_dt - timedelta(seconds=timeframe)
#         else:
#             due_date = first_date_dt + timedelta(minutes=timeframe)
#     if timeframe_unit in ('minutos', 'minutes', 'min'):
#         if operator == '-':
#             due_date = first_date_dt - timedelta(minutes=timeframe)
#         else:
#             due_date = first_date_dt + timedelta(minutes=timeframe)
#     if timeframe_unit in ('horas', 'hours', 'hr'):
#         if operator == '-':
#             due_date = first_date_dt - timedelta(hours=timeframe)    
#         else:
#             due_date = first_date_dt + timedelta(hours=timeframe)    
#     if timeframe_unit in ('dias', 'days', 'dy'):
#         print('diasssss', operator)
#         print('timeframe', timeframe)
#         if operator == '-':
#             due_date = first_date_dt - timedelta(days=timeframe)
#         else:
#             due_date = first_date_dt + timedelta(days=timeframe)
#     if timeframe_unit in ('semanas', 'weeks', 'wk'):
#         if operator == '-':
#             due_date = first_date_dt - timedelta(weeks=timeframe)
#         else:
#             due_date = first_date_dt + timedelta(weeks=timeframe)
#     if timeframe_unit in ('mes', 'month', 'mth'):
#         if operator == '-':
#             next_month = first_date_dt.month - 1
#             year = first_date_dt.year
#             day = first_date_dt.day
#             if next_month == 0:
#                 next_month = 12
#                 year -= 1
#         else:
#             next_month = first_date_dt.month +1
#             year = first_date_dt.year
#             day = first_date_dt.day
#             if next_month == 13:
#                 next_month = 1
#                 year += 1
#         due_date = '{}-{}-{} {}'.format(year, next_month, day , first_date[-8:])
#         due_date =  datetime.strptime(due_date, '%Y-%m-%d %H:%M:%S')
#         if operator == '-':
#             due_date = due_date + timedelta(days=1)
#         else:
#             due_date = due_date - timedelta(days=1)
#     return due_date.strftime('%Y-%m-%d %H:%M:%S')

# def schedule_task(current_record):
#     '''
#     start_date: es la fecha con la que se va a porgramar la recurrencia del dag
#     ojo si esta fecha aun no pasa, las tareas del dag sencillamente no corren
#     por default el start date es igual a la fecha de la primer ejecucion, a menos de que se 
#     programe con anticipacion.
#     '''
#     response = {}
#     #TODO obtener el huzo horario del usuario y calcular us tzoffset
#     tz_offset = current_record.get('tz_offset', -300) 
#     answers = current_record.get('answers',{})
#     dag_id = answers.get('abcde0001000000000000000')
#     action = answers.get('abcde00010000000a0000001')
#     print(action )
#     print(action )
#     if not answers or  action in ('eliminar', 'delete'):
#         if dag_id:
#             return delete_cron(cron_id = dag_id)
#         else:
#             return delete_cron(current_record.get('item_id'), current_record.get('folio'))
#     task_type = 'create_and_assign'
#     if action in ('pausar', 'pause','pausa'):
#         print('pauaaaaaaaaaaaaaaaaaaaaaaaaaa')
#         if not dag_id:
#             msg_error_app = {
#                 "error":{"msg": ["Cron ID is needed, only existing Crons can be paused!"], "label": "Cron Id", "error":["Cron ID is needed, only existing Crons can be paused!!!"]},
#             }
#             raise Exception(simplejson.dumps(msg_error_app))
#         body = {
#             'dag_id': dag_id,
#             'is_paused': True
#         }
#         response = lkf_api.update_cron(body)
#         if response.get('status_code') == 200:
#             response['is_paused'] = True
#         return response
#     elif action in ('corriendo', 'running','programar','program') and dag_id:
#         print('va por acaaaa................')
#         body = {
#             'dag_id': dag_id,
#             'is_pause': False
#         }
#         response = lkf_api.update_cron(body)
#         if response.get('satus_code') == 200:
#             response['is_paused'] = False

#     first_date = answers.get('abcde0001000000000010001')
#     #por default se corre en UTC+0
#     start_date = first_date
#     anticipacion = answers.get('abcde0002000000000010001')
#     timeframe_ant = answers.get('abcde0002000000000010004')
#     timeframe_unit_ant = answers.get('abcde0002000000000010005','horas')
#     if anticipacion == 'si':
#         if timeframe_ant:
#             print('first_date=',first_date)
#             print('timeframe_ant=',timeframe_ant)
#             print('timeframe_unit_ant=',timeframe_unit_ant)
#             print('tz_offset=',tz_offset)
#             start_date = calc_date(first_date, timeframe_ant, timeframe_unit_ant, operator='-')
#             print('start date 1111', start_date)
#             start_date = calc_date(start_date, tz_offset, 'minutes')
#     print('start date ', start_date)
#     # print('start date ', start_datestop)
#     end_date = answers.get('abcde0001000000000010099')
#     timeframe = answers.get('abcde0001000000000010004')
#     timeframe_unit = answers.get('abcde0001000000000010005',1)
#     #
#     due_date = calc_date_as_function(first_date, tz_offset, timeframe, timeframe_unit)
#     first_date = calc_date_as_function(first_date, tz_offset)


#     task_name = answers.get('abcde0001000000000000001')

#     task_st = answers.get('abcde0001000000000000006')
#     duration = answers.get('abcde0001000000000000016',1) * 3600
#     description = answers.get('abcde0001000000000000007')
#     status = answers.get('abcde0001000000000000020')

#     #cambia cada forma
#     field_map = {'abcded001000000000000001': answers.get('abcded001000000000000001')}
#     group_field_map = {'abcde0001000000000000008': answers.get('abcde0001000000000000008')}

#     asigne_to = answers.get('abcde0001000000000020001',[])
#     assigned_users = answers.get('abcde0001000000000020002')
#     custom_cron = False


#     # if repeat_every in ('custom', 'configurable'):
#     #     custom_cron = answers.get('abcde0001000000000010020')
#     #     if not custom_cron:
#     #         error_msg = 'Si se indica una configuracion custom debe de poner un cron custom'
#     # else:
#     schedule_config = get_schedule_config(answers)
#     if not schedule_config:
#         error_msg = 'No se encontro configuracion'
#     body = {}

#     item_id = answers.get('66b3e9a363db61a8c7f62096',{}).get('ccccc0000000000000000000')[0]
#     item_type = answers.get('66b3e9a363db61a8c7f62096',{}).get('ccccc0000000000000000002')
#     if not item_type or not item_id:
#         msg_error_app = {
#             "error":{"msg": ["Error al obtener el tipo de recurso (item)"], "label": "Cron Id", "error":["Error al obtener el tipo de recurso (item)"]},
#         }
#         raise Exception(simplejson.dumps(msg_error_app))
#     if type(item_type) == list:
#         item_type = item_type[0]
#     item_type = item_type.lower()
#     if item_type == 'script':
#         task_type = 'LKFRunScript'
#     if not item_id:
#         error_msg = 'Es requerido tener una forma seleccionada'
    
#     if dag_id:
#         body['id'] = dag_id
#     body['subscription_id'] = item_id
#     body['subscription_type'] = item_type
#     body['name'] = task_name
#     body['description'] = description
#     body['default_args'] = {
#         "email":["josepato@linkaform.com","roman.lezama@linkaform.com"],
#         "retries":1,
#         "email_on_failure" : True,
#         "retry_delay" : "timedelta(seconds=30)"
#     }
#     body['params'] = {'api_key':config['API_KEY']}
#     #TODO calcular el first date , para que arrance la recurrencia tomando en cuenta
#     # el tiempo de anticipacion
#     body['dag_params'] = {
#         "concurrency":3,
#         "catchup": False,
#         "duration": duration,
#         "start_date":"datetime({} ,{}, {}, {}, {})".format(
#             int(start_date[:4]),
#             int(start_date[5:7]),
#             int(start_date[8:10]),
#             int(start_date[11:13]),
#             int(start_date[14:16]),
#             int(start_date[17:19]),
#         )
#     }

#     if custom_cron:
#         body['dag_params'].update({'schedule_interval':custom_cron})
#     else:
#         body['dag_params'].update({'schedule_config':schedule_config})

#     if end_date:
#         print('hace el update????????? end date')
#         body['dag_params'].update({
#             "end_date":"datetime({} ,{}, {}, {}, {})".format(
#             int(end_date[:4]),
#             int(end_date[5:7]),
#             int(end_date[8:10]),
#             int(end_date[11:13]),
#             int(end_date[14:16]),
#             int(end_date[17:19]),
#             )
#             })

#     body['tasks'] = [{
#         "name":"LKF Login",
#         "operator_lib":"lkf_operator",
#         "operator":"LKFLogin",
#         "downstream_task_id":[]
#     }]
#     downstream_task_id = 1
#     if task_type == 'create_and_assign':
#         task = {
#             "name":task_name,
#             "operator_lib":"lkf_operator",
#             "operator":"CreateAndAssignTask",
#             "params":{
#                 "form_id":item_id,
#                 "answers":get_answers_map(task_st, description, first_date, due_date, status, field_map, group_field_map),
#             }
#         }
#         #if 'todos_los_usuarios_que_tengan_el_formulario_ compartido' in asigne_to:
#         body['tasks'].append(task)
#         downstream_task_id += 1
#         body['tasks'][0]['downstream_task_id'].append(downstream_task_id)
#         all_user_ids = []
#         if 'todos_los_usuarios_que_tengan_el_formulario_compartido' in  asigne_to:
#             fileshare_user_ids = get_form_fileshare(item_id)
#             print('fileshare_user_ids', fileshare_user_ids)
#             all_user_ids += convert_usr_id_to_dict(fileshare_user_ids)
#         for gset in assigned_users:
#             print('gset', gset)
#             if gset.get('abcde0001000000000020003') == 'grupo':
#                 udata = gset.get(GROUP_CATALOG_ID,{})
#                 print('udata', udata)
#                 group_id = udata.get('639b65dfaf316bacfc551ba2')[0]
#                 # grp_set = set.get(GROUP_CATALOG_ID)
#                 group_users = lkf_api.get_group_users(group_id)
#                 # guser_id = [user['id'] for user in group_users if user.get('id')]
#                 # user_idsG = update_users(user_ids, guser_id)
#                 all_user_ids += update_users(all_user_ids, group_users)
#             elif gset.get('abcde0001000000000020003') == 'usuario':
#                 udata = gset.get(USER_CATALOG_ID,{})
#                 data = {
#                     "name":udata.get('638a9a7767c332f5d459fc81'),
#                     "email":udata.get('638a9a7767c332f5d459fc82',[])[0],
#                     "username":udata.get('638a9a7767c332f5d459fc82',[])[0],
#                     "user_id":udata.get('638a9a99616398d2e392a9f5',[])[0],
#                     "account_id":9804,
#                     "resource_kind":"user"
#                 }
#                 all_user_ids += update_users(all_user_ids, data)
#         if all_user_ids:
#             body['assign'] = {'assign_users':[]}

#         for assige_usr in all_user_ids:
#             print('a ver como va a qui', assige_usr)
#             # user_data = {
#             #     "resource_kind":"user",
#             #     "email":"user_email",
#             #     "username":"usernameuser",
#             #     "user_id":assige_usr,
#             #     "account_id":9804}
#             # this_task = deepcopy(task)
#             # print('un user......', this_task)
#             body['assign']['assign_users'].append(assige_usr)
#             # this_task['params']['assinge_user_id'] = assige_usr.get('user_id')
#             # body['tasks'].append(this_task)
#             # downstream_task_id += 1
#             # print('tasks',simplejson.dumps(body['tasks'], indent=4))
#             # body['tasks'][0]['downstream_task_id'].append(downstream_task_id)
#             # task['params'].update({'assinge_user_id':assige_usr})
#             # th_body = deepcopy(body)
#             # th_body['dag_params'].update({'dag_id_suffix': str(assige_usr)})
#             # th_body['tasks'].append(task)
#             # print('th_body=',th_body)
        
#         #print('body=', body['dag_params'])
#         response.update(subscribe_cron(body))
#         # print('si nos regresa el res....', response)
#     elif task_type == 'LKFRunScript':
#         task = {
#             "name":task_name,
#             "operator_lib":"lkf_operator",
#             "operator":task_type,
#             "description":task_st,
#             "summary":description,
#             "params":{
#                 "script_id":item_id,
#             }
#         }
#         #TODO place the script parameters answers
#         task["params"].update(get_script_map())
#         body['tasks'].append(task)
#         downstream_task_id += 1
#         body['tasks'][0]['downstream_task_id'].append(downstream_task_id)
#         response.update(subscribe_cron(body))
#     return response

# def subscribe_cron(body):
#     # print('sub=',body)
#     print('subscribe=',simplejson.dumps(body, indent=4))
#     subscribe = lkf_api.subscribe_cron(body)
#     print('subscribe=',subscribe)
#     return subscribe
#     #todo borrar regla
#     #hacer coleccion para que el dag id sea el object id
#     #la idea que el dag_name_id se el objectid es para que se pueda editar de
#     #manera sencilla porque si el nombre se tiene que editar y el nombre
#     #compuesto por parametros, los parametros no pueden cambiar
#     #hay que conectar airflow bob con mongo

# def lkf_date(date_str):
#     global current_record
#     tz_offset = current_record.get('tz_offset', -300) 
#     lkf_date = datetime.strptime(date_str[:19], '%Y-%m-%dT%H:%M:%S') + timedelta(minutes=tz_offset)
#     lkf_date = lkf_date.strftime('%Y-%m-%d %H:%M:%S')
#     return lkf_date

# def get_dag_dates(data):
#     res = {}
#     dag_info = data.get('dag_info',{})
#     next_run = dag_info.get('next_dagrun')
#     create_after = dag_info.get('next_dagrun_create_after')
#     if next_run and create_after:
#         res ={
#             'abcde000100000000000f000':lkf_date(next_run),
#             'abcde000100000000000f001':lkf_date(create_after),
#         }
#     return res    


class Schedule(Schedule):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):

        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)

        self.mf.update({
            'pais_localidad':'631fccdd844ed53c7d989718',
            'localidad':'631fc1e48d9fe191da0c3daf',
            })


        # self.LOCALIDADES = self.lkm.catalog_id('localidades')
        # self.LOCALIDADES_ID = self.LOCALIDADES.get('id')
        # self.LOCALIDADES_OBJ_ID = self.LOCALIDADES.get('obj_id')

    def get_answers_map(self, task, description, first_date, due_date, status, field_map, group_field_map ):
        ans = super().get_answers_map(task, description, first_date, due_date, status, field_map, group_field_map )
        localidad = self.answers.get(self.LOCALIDADES_OBJ_ID)
        if localidad:
            ans.update({self.LOCALIDADES_OBJ_ID:localidad})
        return ans




if __name__ == "__main__":
    # print(sys.argv)
    schedule_obj = Schedule(settings, sys_argv=sys.argv, use_api=True)
    schedule_obj.console_run()
    lkf_api = schedule_obj.lkf_api
    res = schedule_obj.schedule_task()
    data = res.get('data')
    print('res', res)
    if res.get('status_code') == 0:
        print('Ningun cambio')
    elif res.get('status_code') == 200:
        if res.get('deleted'):
            schedule_obj.answers['abcde00010000000a0000000'] = 'eliminado'
        else:
            schedule_obj.answers['abcde0001000000000000000'] = data.get('dag_id')
            schedule_obj.answers.update(schedule_obj.get_dag_dates(data))
            if res.get('is_paused') == True:
                schedule_obj.answers['abcde00010000000a0000000'] = 'pausado'
            else:
                schedule_obj.answers['abcde00010000000a0000000'] = 'corriendo'

        sys.stdout.write(simplejson.dumps({
            'status': 101,
            'replace_ans': schedule_obj.answers,
        }))
    else:
        print('res===0',res)

        msg_error_app = "Something went wrong!!!"
        if res.get('json',{}).get('error') or res.get('status_code') == 400:
            if res.get('json',{}).get('error'):
                msg_error_app = res['json']['error']
            else:
                msg_error_app = res['json'].get('message','Something went wrong!!!')
        else:
            msg_error_app = {
                "error":{"msg": [msg_error_app], "label": "Cron Id", "error":[msg_error_app]},
            }
        raise Exception(simplejson.dumps(msg_error_app))