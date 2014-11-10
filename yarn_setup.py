#!/usr/bin/env python

import socket
import time
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo

cm_host = 'ip-10-136-86-133'
api = ApiResource(cm_host, username='admin', password='admin')

cluster = api.get_cluster('cloudera-pe-test')

### YARN ###
yarn_service_name = "YARN"
yarn_service_config = {
  'hdfs_service': "hdfs01",
}
yarn_rm_host = "ip-10-11-167-80"
yarn_rm_config = { }
yarn_jhs_host = "ip-10-11-167-80"
yarn_jhs_config = { }
yarn_nm_hosts = [ ]
yarn_nm_hosts.append("ip-10-153-224-197")
yarn_nm_hosts.append("ip-10-169-69-118")
yarn_nm_hosts.append("ip-10-37-166-245")

yarn_nm_config = {
  #'yarn_nodemanager_local_dirs': '/data01/hadoop/yarn/nm',
  'yarn_nodemanager_local_dirs': '/dfs' + '/yarn/nm',
}
yarn_gw_hosts = [ ]
yarn_gw_hosts.append("ip-10-11-167-80")
yarn_gw_hosts.append("ip-10-153-224-197")
yarn_gw_hosts.append("ip-10-169-69-118")
yarn_gw_hosts.append("ip-10-37-166-245")

yarn_gw_config = {
  'mapred_submit_replication': min(3, len(yarn_gw_hosts))
}

yarn_service = cluster.create_service(yarn_service_name, "YARN")
yarn_service.update_config(yarn_service_config)
      
rm = yarn_service.get_role_config_group("{0}-RESOURCEMANAGER-BASE".format(yarn_service_name))
rm.update_config(yarn_rm_config)
yarn_service.create_role("{0}-rm".format(yarn_service_name), "RESOURCEMANAGER", yarn_rm_host)
      
jhs = yarn_service.get_role_config_group("{0}-JOBHISTORY-BASE".format(yarn_service_name))
jhs.update_config(yarn_jhs_config)
yarn_service.create_role("{0}-jhs".format(yarn_service_name), "JOBHISTORY", yarn_jhs_host)
   
nm = yarn_service.get_role_config_group("{0}-NODEMANAGER-BASE".format(yarn_service_name))
nm.update_config(yarn_nm_config)
   
nodemanager = 0
for host in yarn_nm_hosts:
   nodemanager += 1
   yarn_service.create_role("{0}-nm-".format(yarn_service_name) + str(nodemanager), "NODEMANAGER", host)

gw = yarn_service.get_role_config_group("{0}-GATEWAY-BASE".format(yarn_service_name))
gw.update_config(yarn_gw_config)
   
gateway = 0
for host in yarn_gw_hosts:
   gateway += 1
   yarn_service.create_role("{0}-gw-".format(yarn_service_name) + str(gateway), "GATEWAY", host)

yarn_service.start().wait()
