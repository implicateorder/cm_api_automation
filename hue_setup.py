#!/usr/bin/env python

import socket
import time
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo

cm_host = 'ip-10-136-86-133'
api = ApiResource(cm_host, username='admin', password='admin')

cluster = api.get_cluster('cloudera-pe-test')

### HUE ###
hue_service_name = "HUE"
hue_service_config = {
  'hive_service': 'HIVE',
  'hbase_service': 'HBASE',
  'impala_service': 'IMPALA',
  'hue_webhdfs': "hdfs01" + "-" + "NAMENODE-MASTER",
  'hue_hbase_thrift': "HBASE" + "-" + "HBASETHRIFTSERVER",
}
hue_server_host = "ip-10-136-86-133"
hue_server_config = { 
}
hue_ktr_host = "ip-10-136-86-133"
hue_ktr_config = { }

hue_service = cluster.create_service(hue_service_name, "HUE")
hue_service.update_config(hue_service_config)

hue_server = hue_service.get_role_config_group("{0}-HUE_SERVER-BASE".format(hue_service_name))
hue_server.update_config(hue_server_config)
hue_service.create_role("{0}-server".format(hue_service_name), "HUE_SERVER", hue_server_host)

time.sleep(10)
hue_service.start().wait()
