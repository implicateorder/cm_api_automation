#!/usr/bin/env python

import socket
import time
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo

cm_host = 'ip-10-136-86-133'
api = ApiResource(cm_host, username='admin', password='admin')

cluster = api.get_cluster('cloudera-pe-test')

### Spark ###
spark_service_name = "SPARK"
spark_service_config = {
  'hdfs_service': 'hdfs01',
}
spark_master_host = "ip-10-11-167-80";
spark_master_config = {
#   'master_max_heapsize': 67108864,
}
spark_worker_hosts = [ ]
spark_worker_hosts.append("ip-10-136-86-133")
spark_worker_hosts.append("ip-10-153-224-197")
spark_worker_hosts.append("ip-10-169-69-118")
spark_worker_hosts.append("ip-10-37-166-245")

spark_worker_config = {
#   'executor_total_max_heapsize': 67108864,
#   'worker_max_heapsize': 67108864,
}
spark_gw_hosts = spark_worker_hosts
spark_gw_hosts.append("ip-10-11-167-80")
spark_gw_config = { }

spark_service = cluster.create_service(spark_service_name, "SPARK")
spark_service.update_config(spark_service_config)
   
sm = spark_service.get_role_config_group("{0}-SPARK_MASTER-BASE".format(spark_service_name))
sm.update_config(spark_master_config)
spark_service.create_role("{0}-sm".format(spark_service_name), "SPARK_MASTER", spark_master_host)

sw = spark_service.get_role_config_group("{0}-SPARK_WORKER-BASE".format(spark_service_name))
sw.update_config(spark_worker_config)

worker = 0
for host in spark_worker_hosts:
   worker += 1
   spark_service.create_role("{0}-sw-".format(spark_service_name) + str(worker), "SPARK_WORKER", host)

gw = spark_service.get_role_config_group("{0}-GATEWAY-BASE".format(spark_service_name))
gw.update_config(spark_gw_config)

gateway = 0
for host in spark_gw_hosts:
   gateway += 1
   spark_service.create_role("{0}-gw-".format(spark_service_name) + str(gateway), "GATEWAY", host)

spark_service.start().wait()
