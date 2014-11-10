#!/usr/bin/python

import socket
import time
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo

# This script can be run from the CM host, or remotely.
# Just make sure that the CM host has been added into the cluster so stuff can be provisioned on it

# hosts = [ ]

cm_host = 'ip-10-136-86-133'
api = ApiResource(cm_host, username='admin', password='admin')

cluster = api.get_cluster('cloudera-pe-test')
manager = api.get_cloudera_manager()

### Management Services ###
# If using the embedded postgresql database, the database passwords can be found in /etc/cloudera-scm-server/db.mgmt.properties.
# The values change every time the cloudera-scm-server-db process is restarted.
# TBD will CM have to be reconfigured each time?
# Prep work before calling the Cloudera provisioning script.
# firehostdbpassword=`grep com.cloudera.cmf.ACTIVITYMONITOR.db.password /etc/cloudera-scm-server/db.mgmt.properties | awk -F'=' '{print $2}'`
# navigatordbpassword=`grep com.cloudera.cmf.NAVIGATOR.db.password /etc/cloudera-scm-server/db.mgmt.properties | awk -F'=' '{print $2}'`
# headlampdbpassword=`grep com.cloudera.cmf.REPORTSMANAGER.db.password /etc/cloudera-scm-server/db.mgmt.properties | awk -F'=' '{print $2}'`

mgmt_service_name = 'MGMT'
mgmt_service_config = {'zookeeper_datadir_autocreate': 'true'}
mgmt_role_config = {'quorumPort': 2888}
amon_role_name = 'ACTIVITYMONITOR'
amon_role_config = {
    'firehose_database_host': cm_host + ':7432',
    'firehose_database_user': 'amon',
    'firehose_database_password': 'aI3geMV2Wk',
    'firehose_database_type': 'postgresql',
    'firehose_database_name': 'amon',
    'firehose_heapsize': '215964392',
    }
apub_role_name = 'ALERTPUBLISHER'
apub_role_config = {}
eserv_role_name = 'EVENTSERVER'
eserv_role_config = {'event_server_heapsize': '215964392'}
hmon_role_name = 'HOSTMONITOR'
hmon_role_config = {}
smon_role_name = 'SERVICEMONITOR'
smon_role_config = {}
nav_role_name = 'NAVIGATOR'
nav_role_config = {
    'navigator_database_host': cm_host + ':7432',
    'navigator_database_user': 'nav',
    'navigator_database_password': 'q05fjl5jiZ',
    'navigator_database_type': 'postgresql',
    'navigator_database_name': 'nav',
    'navigator_heapsize': '215964392',
    }
navms_role_name = 'NAVIGATORMETADATASERVER'
navms_role_config = {}
rman_role_name = 'REPORTMANAGER'
rman_role_config = {
    'headlamp_database_host': cm_host + ':7432',
    'headlamp_database_user': 'rman',
    'headlamp_database_password': 'dAHIrIM3xv',
    'headlamp_database_type': 'postgresql',
    'headlamp_database_name': 'rman',
    'headlamp_heapsize': '215964392',
    }

mgmt = manager.create_mgmt_service(ApiServiceSetupInfo())

# create roles. Note that host id may be different from host name (especially in CM 5). Look it it up in /api/v5/hosts

mgmt.create_role(amon_role_name + '-1', 'ACTIVITYMONITOR', cm_host)
mgmt.create_role(apub_role_name + '-1', 'ALERTPUBLISHER', cm_host)
mgmt.create_role(eserv_role_name + '-1', 'EVENTSERVER', cm_host)
mgmt.create_role(hmon_role_name + '-1', 'HOSTMONITOR', cm_host)
mgmt.create_role(smon_role_name + '-1', 'SERVICEMONITOR', cm_host)

#mgmt.create_role(nav_role_name + "-1", "NAVIGATOR", cm_host)
#mgmt.create_role(navms_role_name + "-1", "NAVIGATORMETADATASERVER", cm_host)
mgmt.create_role(rman_role_name + "-1", "REPORTSMANAGER", cm_host)

# now configure each role

for group in mgmt.get_all_role_config_groups():
    if group.roleType == 'ACTIVITYMONITOR':
        group.update_config(amon_role_config)
    elif group.roleType == 'ALERTPUBLISHER':
        group.update_config(apub_role_config)
    elif group.roleType == 'EVENTSERVER':
        group.update_config(eserv_role_config)
    elif group.roleType == 'HOSTMONITOR':
        group.update_config(hmon_role_config)
    elif group.roleType == 'SERVICEMONITOR':
        group.update_config(smon_role_config)

#    elif group.roleType == "NAVIGATOR":
#        group.update_config(nav_role_config)
#    elif group.roleType == "NAVIGATORMETADATASERVER":
#        group.update_config(navms_role_config)
    elif group.roleType == "REPORTSMANAGER":
        group.update_config(rman_role_config)

# now start the management service

mgmt.start().wait()
