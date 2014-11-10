#!/usr/bin/env python

import socket
import time
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo

cm_host = 'ip-10-136-86-133'
api = ApiResource(cm_host, username='admin', password='admin')

cluster = api.get_cluster('cloudera-pe-test')

### HBase ###
hbase_service_name = "HBASE"
hbase_service_config = {
  'hdfs_service': 'hdfs01',
  'zookeeper_service': 'zookeeper01',
}
hbase_hm_host = "ip-10-136-86-133"
hbase_hm_config = { }
hbase_rs_hosts = [ ]
hbase_rs_hosts.append("ip-10-153-224-197")
hbase_rs_hosts.append("ip-10-169-69-118")
hbase_rs_config = {
  'hbase_hregion_memstore_flush_size': 1024000000,
  'hbase_regionserver_handler_count': 10,
  'hbase_regionserver_java_heapsize': 2048000000,
  'hbase_regionserver_java_opts': '',
}
hbase_thriftserver_service_name = "HBASETHRIFTSERVER"
hbase_thriftserver_host = "ip-10-136-86-133"
hbase_thriftserver_config = { }
hbase_gw_hosts = hbase_rs_hosts
hbase_gw_hosts.append("ip-10-136-86-133")
hbase_gw_hosts.append("ip-10-37-166-245")
hbase_gw_hosts.append("ip-10-11-167-80")
hbase_gw_config = { }


hbase_service = cluster.create_service(hbase_service_name, "HBASE")
hbase_service.update_config(hbase_service_config)
   
hm = hbase_service.get_role_config_group("{0}-MASTER-BASE".format(hbase_service_name))
hm.update_config(hbase_hm_config)
hbase_service.create_role("{0}-hm".format(hbase_service_name), "MASTER", hbase_hm_host)

rs = hbase_service.get_role_config_group("{0}-REGIONSERVER-BASE".format(hbase_service_name))
rs.update_config(hbase_rs_config)

ts = hbase_service.get_role_config_group("{0}-HBASETHRIFTSERVER-BASE".format(hbase_service_name))
ts.update_config(hbase_thriftserver_config)
ts_name_pattern = "{0}-" + hbase_thriftserver_service_name
hbase_service.create_role(ts_name_pattern.format(hbase_service_name), "HBASETHRIFTSERVER", hbase_thriftserver_host)

gw = hbase_service.get_role_config_group("{0}-GATEWAY-BASE".format(hbase_service_name))
gw.update_config(hbase_gw_config)

regionserver = 0
for host in hbase_rs_hosts:
   regionserver += 1
   hbase_service.create_role("{0}-rs-".format(hbase_service_name) + str(regionserver), "REGIONSERVER", host)

gateway = 0
for host in hbase_gw_hosts:
   gateway += 1
   hbase_service.create_role("{0}-gw-".format(hbase_service_name) + str(gateway), "GATEWAY", host)

hbase_service.create_hbase_root()

hbase_service.start().wait()
