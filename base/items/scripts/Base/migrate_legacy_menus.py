# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from base_utils import Base

class Base(Base):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)


if __name__ == "__main__":
    script_obj = Base(settings, sys_argv=sys.argv)
    script_obj.console_run()
    data = script_obj.data.get('data', {})

    user_ids = data.get('user_ids')
    dry_run = bool(data.get('dry_run'))

    response = script_obj.migrate_legacy_menus(user_ids=user_ids, dry_run=dry_run)
    script_obj.HttpResponse({"data": response})
