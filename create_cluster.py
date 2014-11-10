#!/usr/bin/env python
import socket
import time
from cm_api.api_client import ApiResource
#initialize

hosts = [ ]
cm_host = "cloudera-pe-cm01"
api = ApiResource(cm_host, username="admin", password="admin")

# Distribute the CDH parcel

parcel_repo = 'http://archive.cloudera.com/cdh5/parcels/5.2.0'
#parcel_repo = 'http://archive.cloudera.com/cdh5/parcels/5.1.3/'
cm_config = api.get_cloudera_manager().get_config(view='full')
repo_config = cm_config['REMOTE_PARCEL_REPO_URLS']
value = repo_config.value or repo_config.default
value += ',' + parcel_repo
api.get_cloudera_manager().update_config({'REMOTE_PARCEL_REPO_URLS': value})
time.sleep(10)

# create cluster, add the hosts
cluster = api.create_cluster("cloudera-pe-test", "CDH5")
#api.create_host("master", "ip-10-238-154-140", "10.238.154.140")
#api.create_host("w01", "ip-10-143-183-98", "10.143.183.98")
#api.create_host("w02", "ip-10-140-38-88", "10.140.38.88")
#api.create_host("w03", "ip-10-140-28-243", "10.140.28.243")
#hosts.append("master")
#hosts.append("w01")
#hosts.append("w02")
#hosts.append("w03")
hosts.append("ip-10-11-167-80")
hosts.append("ip-10-153-224-197")
hosts.append("ip-10-37-166-245")
hosts.append("ip-10-169-69-118")
cluster.add_hosts(hosts)

# Downloads and distributes parcels

# Had to recreate the cluster object as follows. For some reason doing a cluster.get_parcel was
# failing while the cluster object was api.create_cluster() 

cluster = api.get_cluster("cloudera-pe-test")
#parcel = cluster.get_parcel("CDH", "5.2.0-1.cdh5.2.0.p0.36")
parcel = cluster.get_parcel("CDH", "5.2.0-1.cdh5.2.0.p0.36")
parcel.start_download();
while True:
    parcel = cluster.get_parcel("CDH", "5.2.0-1.cdh5.2.0.p0.36")
    if parcel.stage != "DOWNLOADED":
    	print "Downloading : %s / %s" % ( parcel.state.progress, parcel.state.totalProgress)
    else:
	break

parcel.start_distribution()
while True:
    parcel = cluster.get_parcel("CDH", "5.2.0-1.cdh5.2.0.p0.36")
    if parcel.stage != "DISTRIBUTED":
        print "Distributing: %s / %s" % ( parcel.state.progress, parcel.state.totalProgress)
    else:
	break
parcel.activate()
while True:
    parcel = cluster.get_parcel("CDH", "5.2.0-1.cdh5.2.0.p0.36")
    if parcel.stage != "ACTIVATED":
        print "Activating: %s / %s" % ( parcel.state.progress, parcel.state.totalProgress)
    else:
	break

cluster.stop().wait()
cluster.start().wait()


# create hdfs service and configure
hdfs = cluster.create_service("hdfs01", "HDFS")
nn_group = hdfs.get_role_config_group("hdfs01-NAMENODE-BASE")
snn_group = hdfs.get_role_config_group("hdfs01-SECONDARYNAMENODE-BASE")
dn_group = hdfs.get_role_config_group("hdfs01-DATANODE-BASE")
# specify required HDFS configuration
hdfs_config = {
   'dfs_replication': 1,
}
nn_config = {
   'dfs_name_dir_list': '/dfs/nn',
   'dfs_namenode_handler_count': 30,
}
snn_config = {
   'fs_checkpoint_dir_list': '/dfs/snn',
}
dn_config = {
   'dfs_data_dir_list': '/dfs/dn1,/dfs/dn2,/dfs/dn3',
   'dfs_datanode_failed_volumes_tolerated': 1,
}
nn_group.update_config(nn_config)
snn_group.update_config(snn_config)
dn_group.update_config(dn_config)
hdfs.update_config(hdfs_config)
hdfs.create_role("NAMENODE-MASTER", "NAMENODE", "ip-10-11-167-80")
hdfs.create_role("SECONDARYNAMENODE-MASTER", "SECONDARYNAMENODE", "ip-10-11-167-80")
hdfs.create_role("DATANODE-WORKER1", "DATANODE", "ip-10-153-224-197")
hdfs.create_role("DATANODE-WORKER2", "DATANODE", "ip-10-37-166-245")
hdfs.create_role("DATANODE-WORKER3", "DATANODE", "ip-10-169-69-118")
hdfs.format_hdfs("NAMENODE-MASTER")
time.sleep(20)
hdfs.start()
