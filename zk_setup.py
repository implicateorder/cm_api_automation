#!/usr/bin/env python
import socket
import time
from cm_api.api_client import ApiResource

# 

#hosts = [ ]
cm_host = "cloudera-pe-cm01"
api = ApiResource(cm_host, username="admin", password="admin")

cluster = api.get_cluster("cloudera-pe-test")

# Instantiate and start Zookeeper

zk = cluster.create_service("zookeeper01", "ZOOKEEPER")
zk_service_conf = { 
	'zookeeper_datadir_autocreate': 'true'
}

zk_role_conf = {
    'quorumPort': 2888,
    'electionPort': 3888,
    'dataLogDir': '/var/lib/zookeeper',
    'dataDir': '/var/lib/zookeeper',
    'maxClientCnxns': '1024'
}

zk.update_config(zk_service_conf)

zk_id = 1
zk_role_conf['serverId'] = zk_id
role = zk.create_role("zookeeper01" + "-" + str(zk_id), "SERVER", "ip-10-11-167-80")
role.update_config(zk_role_conf)
zk.init_zookeeper()
