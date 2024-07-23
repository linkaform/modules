# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from app import Accesos

class Accesos(Accesos):
    pass
if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    data = acceso_obj.data.get('data',{})
    option = data.get("option",'')

    data_notes = data.get("data_notes",{
        'note_status':'cerrado',
        'note':'Vienen a recoger un paquete ',
        'note_booth':'Caseta Vigilancia Norte 3',
        'note_guard':'Jacinto Sánchez Hil',
        'note_guard_close':'Lucia Perez N',
        'note_pic':[
            #{'file_url':'','file_name':''}
        ],
        'note_file':[
            #{'file_url':'','file_name':''}
        ],
        'note_comments':[
            'Primer Comentario',
            'Segundo Comentario',
        ],
    })
    data_update = data.get("data_update",{
        'note_status':'cerrado',
        'note':'Vienen a recoger un paquete ',
        'note_booth':'Caseta Vigilancia Av 16',
        'note_guard':'Jacinto Sánchez Hil',
        'note_guard_close':'Lucia Perez N',
    })
    area = data.get("area",'Caseta Vigilancia Norte 3')
    folio = data.get("folio",'588-10')
    #-FUNCTIONS
    #option = 'new_notes';
    #option = 'get_notes';
    #option = 'update_note';
    #option = 'delete_note';
    print('option', option)
    if option == 'new_notes':
        response = acceso_obj.create_note(data_notes)
    elif option == 'get_notes':
        response = acceso_obj.get_list_notes(area)
    elif option == 'update_note':
        response = acceso_obj.update_notes(data_update, folio)
    elif option == 'delete_note':
        response = acceso_obj.delete_notes(folio)
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})
