# Copyright 2021 The sqlalchemy-bigquery Authors
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import functools
import re

from google.api_core import client_info
import google.auth
from google.cloud import bigquery
from google.oauth2 import service_account
import sqlalchemy
import base64
import json
import logging

USER_AGENT_TEMPLATE = "sqlalchemy/{}"
SCOPES = (
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
)


def google_client_info():
    user_agent = USER_AGENT_TEMPLATE.format(sqlalchemy.__version__)
    return client_info.ClientInfo(user_agent=user_agent)

def verify_args(email,username):
    if not email.endswith("geotab.com"):
        return False
    return bool(re.match("^[A-Za-z0-9_-]*$", username))

logger = logging.getLogger()

# Create a new google client on query run, database added, dataset added
# username and email fields are added for impersonation from parse_url (attributes are part of URL object)
def create_bigquery_client(
    credentials_info=None,
    credentials_path=None,
    credentials_base64=None,
    default_query_job_config=None,
    location=None,
    project_id=None,
    username=None,
    email=None,
):
   
    default_project = None
    
    if credentials_base64:
        credentials_info = json.loads(base64.b64decode(credentials_base64))

    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        credentials = credentials.with_scopes(SCOPES)
        default_project = credentials.project_id
    elif credentials_info:
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info
        )
        credentials = credentials.with_scopes(SCOPES)
        default_project = credentials.project_id
    else:
        credentials, default_project = google.auth.default(scopes=SCOPES)

    if project_id is None:
        project_id = default_project
    
#     if email is not None and username is not None and not verify_args(email, username):
#         logger.critical("INVALID USERNAME OR EMAIL: {} {}".format(username, email))
    
    if email is not None:
        logger.debug("Impersonated email: {}".format(email))
        credentials = credentials.with_subject(email)    

#     if username is not None:
#         logger.debug("username: {}".format(username))


#     logger.debug("client_info: {}".format(google_client_info()))
#     logger.debug("projectid: {}".format(project_id))
#     logger.debug("credentials info: {}".format(credentials))
#     logger.debug("location {}".format(location))
#     logger.debug("queryjobconfig {}".format(default_query_job_config))

    return bigquery.Client(
        client_info=google_client_info(),
        project=project_id,
        credentials=credentials,
        location=location,
        default_query_job_config=default_query_job_config,
    )


def substitute_re_method(r, flags=0, repl=None):
    if repl is None:
        return lambda f: substitute_re_method(r, flags, f)

    r = re.compile(r, flags)

    @functools.wraps(repl)
    def sub(self, s, *args, **kw):
        def repl_(m):
            return repl(self, m, *args, **kw)

        return r.sub(repl_, s)

    return sub


def substitute_string_re_method(r, *, repl, flags=0):
    r = re.compile(r, flags)
    return lambda self, s: r.sub(repl, s)
