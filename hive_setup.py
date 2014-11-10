#!/usr/bin/env python

import socket
import time
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo

cm_host = 'ip-10-136-86-133'
api = ApiResource(cm_host, username='admin', password='admin')

cluster = api.get_cluster('cloudera-pe-test')

### Hive ###
hive_service_name = "HIVE"
hive_service_config = {
  'hive_metastore_database_host': cm_host,
  'hive_metastore_database_name': 'metastore',
  'hive_metastore_database_password': 'hive',
  'hive_metastore_database_port': 3306,
  'hive_metastore_database_type': 'mysql',
  'mapreduce_yarn_service': 'YARN',
  'zookeeper_service': 'zookeeper01',
  'mapreduce_yarn_service': 'YARN',
}
hive_hms_host = "ip-10-11-167-80"
hive_hms_config = {
  'hive_metastore_java_heapsize': 85306784,
}
hive_hs2_host = "ip-10-11-167-80"
hive_hs2_config = { }
hive_whc_host = "ip-10-11-167-80"
hive_whc_config = { }
hive_gw_hosts = [ ]
hive_gw_hosts.append("ip-10-11-167-80")
hive_gw_hosts.append("ip-10-136-86-133")
hive_gw_hosts.append("ip-10-153-224-197")
hive_gw_hosts.append("ip-10-169-69-118")
hive_gw_hosts.append("ip-10-37-166-245")
hive_gw_config = { }


hive_service = cluster.create_service(hive_service_name, "HIVE")
hive_service.update_config(hive_service_config)
   
hms = hive_service.get_role_config_group("{0}-HIVEMETASTORE-BASE".format(hive_service_name))
hms.update_config(hive_hms_config)
hive_service.create_role("{0}-hms".format(hive_service_name), "HIVEMETASTORE", hive_hms_host)
   
hs2 = hive_service.get_role_config_group("{0}-HIVESERVER2-BASE".format(hive_service_name))
hs2.update_config(hive_hs2_config)
hive_service.create_role("{0}-hs2".format(hive_service_name), "HIVESERVER2", hive_hs2_host)
   
whc = hive_service.get_role_config_group("{0}-WEBHCAT-BASE".format(hive_service_name))
whc.update_config(hive_whc_config)
hive_service.create_role("{0}-whc".format(hive_service_name), "WEBHCAT", hive_whc_host)
   
gw = hive_service.get_role_config_group("{0}-GATEWAY-BASE".format(hive_service_name))
gw.update_config(hive_gw_config)
   
gateway = 0
for host in hive_gw_hosts:
   gateway += 1
   hive_service.create_role("{0}-gw-".format(hive_service_name) + str(gateway), "GATEWAY", host)

hive_service.start().wait()
