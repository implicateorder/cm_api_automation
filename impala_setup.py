#!/usr/bin/env python

import socket
import time
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo

cm_host = 'ip-10-136-86-133'
api = ApiResource(cm_host, username='admin', password='admin')

cluster = api.get_cluster('cloudera-pe-test')

### Impala ###
impala_service_name = "IMPALA"
impala_service_config = {
  'hdfs_service': 'hdfs01',
  'hbase_service': 'HBASE',
  'hive_service': 'HIVE',
}
impala_ss_host = "ip-10-11-167-80"
impala_ss_config = { }
impala_cs_host = "ip-10-11-167-80"
impala_cs_config = { }
impala_id_hosts = ["ip-10-11-167-80","ip-10-136-86-133","ip-10-153-224-197","ip-10-169-69-118","ip-10-37-166-245"]
impala_id_config = { }

impala_service = cluster.create_service(impala_service_name, "IMPALA")
impala_service.update_config(impala_service_config)

ss = impala_service.get_role_config_group("{0}-STATESTORE-BASE".format(impala_service_name))
ss.update_config(impala_ss_config)
impala_service.create_role("{0}-ss".format(impala_service_name), "STATESTORE", impala_ss_host)

cs = impala_service.get_role_config_group("{0}-CATALOGSERVER-BASE".format(impala_service_name))
cs.update_config(impala_cs_config)
impala_service.create_role("{0}-cs".format(impala_service_name), "CATALOGSERVER", impala_cs_host)

id = impala_service.get_role_config_group("{0}-IMPALAD-BASE".format(impala_service_name))
id.update_config(impala_id_config)

impalad = 0
for host in impala_id_hosts:
   impalad += 1
   impala_service.create_role("{0}-id-".format(impala_service_name) + str(impalad), "IMPALAD", host)

time.sleep(10)

impala_service.start().wait()
