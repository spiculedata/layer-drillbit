import yaml
import json
import urllib.request as request
from charms.reactive import (
    when,
    when_not,
    set_state,
    remove_state
)
from subprocess import (
    check_call,
    CalledProcessError,
    call,
    check_output,
    Popen
)
from charmhelpers.core import hookenv
from charmhelpers.core.host import (
    adduser,
    chownr,
    mkdir
)
from charmhelpers.core.hookenv import (
    status_set,
    log,
    resource_get
)
from charms.reactive.helpers import data_changed
from psutil import virtual_memory
import shutil
from charms.layer import snap
import os
import charmhelpers
from time import sleep

@when_not('drillbit.installed')
def install_drillbit():
    """
         Install a drillbit on a node
         This will download Apache Drill from the configuration url and extract it into /opt/drill/
    """
    status_set('maintenance', 'Installing Apache Drill')
    snap.install('apache-drill-spicule', channel="edge", devmode=True)
    hookenv.open_port(8047)
    mysql = resource_get("mysql-jar")
    pgsql = resource_get("pgsql-jar")
    mkdir('/var/snap/apache-drill/common/jars/3rdparty/')
    shutil.copy(mysql, "/var/snap/apache-drill/common/jars/3rdparty/")
    shutil.copy(pgsql, "/var/snap/apache-drill/common/jars/3rdparty/")
    set_state('drillbit.installed')
    status_set('waiting', 'Apache Drill Installed, Awaiting Configuration')


@when('zookeeper.joined')
@when_not('zookeeper.ready')
def wait_for_zookeeper(zookeeper):
    """
         We always run in Distributed mode, so wait for Zookeeper to become available.
    """
    hookenv.status_set('waiting', 'Waiting for Zookeeper to become available')

@when_not('zookeeper.joined')
def wait_for_zkjoin():
    """
        Wait for Zookeeper
    """
    status_set('waiting', 'Waiting for Zookeeper to become joined')


@when('zookeeper.ready')
@when_not('drillbit.configured')
def configure(zookeeper):
    """
        Configure Zookeeper for the first time.
        This will set memory limits. By default we use a % model for memory calculations.
        This allows us to automatically scale the drillbit depending on where it is installed.
    """
    status_set('maintenance', 'Configuring Apache Drill')
    direct = configure_direct_memory()
    heap = configure_heap()
    write_zk_file(zookeeper)
    write_memory_file(direct,heap)
    start_drill()
    hookenv.open_port('8047')
    set_state('drillbit.configured')
    status_set('active', 'Apache Drill up and running')

@when('drillbit.configured', 'zookeeper.ready')
def configure_zookeepers(zookeeper):
    """
        Once ZK has been related and java is available we endlessly
        run this hook to keep the ZK config up to date and RAM settings correct.
        If the ZK information changes, this will update the configs and restart Drill.
    """
    zks = zookeeper.zookeepers()
    if data_changed('available.zookeepers', zks):
        status_set('maintenance', 'Zookeeper configuration changed. Updating Apache Drill.')
        write_zk_file(zookeeper)
        restart_drill()
    if data_changed('direct.memory', hookenv.config('drill_max_direct_memory')) or data_changed('drill.heap', hookenv.config('drill_heap')):
        status_set('maintenance', 'Memory settings changed. Updating Apache Drill.')
        direct = configure_direct_memory()
        heap = configure_heap()
        write_memory_file(direct,heap)
        restart_drill()

def calculate_ram(percent):
    """
        Calculate the % of RAM rounded to the lower GB to ensure it doesn't blow the memory limits.
    """
    mem = virtual_memory()
    gb = mem.total/1073741824
    return int((int(gb)/100)*int(percent))

def add_zookeeper(host, port):
    """
        Return a ZK hostline for the config.
    """
    return host+':'+port+','

def simple_template(zk):
    """
        Return a drill exec line for Drill configuration. This creates an entry in ZK.
    """
    return 'drill.exec: { cluster-id: "'+hookenv.config()['cluster_id']+'", zk.connect: "'+zk+'"}'

def start_drill():
    """
        Run the Drill start script.
    """
    try:
        log("Checking drill status")
        check_call('apache-drill-spicule.status-drill-distributed', shell=True)
    except CalledProcessError:
        log("Starting Drill.........")
        check_call('apache-drill-spicule.start-drill-distributed', shell=True)
        status_set('active', 'Apache Drill up and running.')
        set_state('drillbit.running')

def restart_drill():
    """
         Run the drill stop script.
    """
    remove_state('drillbit.running')
    check_call('apache-drill-spicule.stop-drill-distributed', shell=True)
    check_call('apache-drill-spicule.start-drill-distributed', shell=True)
    check_responsive(10)
    status_set('active', 'Apache Drill up and running.')
    set_state('drillbit.running')

def check_responsive(lc):
    
    for _ in range(lc):
        try:        
            req = request.Request('http://localhost:8047')
            resp = request.urlopen(req)
            break
        except Exception:
            sleep(5)
            log("ping failed, retrying")
    else:
        raise
    
def stop_drill():
    """
        Stop drill.
    """
    check_call('apache-drill-spicule.stop-drill-distributed', shell=True)
    status_set('active', 'Apache Drill Stopped.')
    remove_state('drillbit.running')

@when('drillbit.running')
@when('mongodb.database.available')
def configure_mongodb(mongo):
    """
        Configure MongoDB when a relation is added.
    """
    n = ''
    for conv in mongo.conversations():
        log(conv.units)
        n = next(s for s in conv.units if s)
    if n:
        n = n.split('/', 1)[0]
    t = {"name":"juju_mongo_"+n, "config": {"type": "mongo","connection": "mongodb://"+mongo.connection_string()+"/","enabled": True}}
    params = json.dumps(t).encode('utf8')
    req = request.Request('http://localhost:8047/storage/juju_mongo_'+n+'.json', data=params,headers={'content-type': 'application/json'})
    request.urlopen(req)

@when('drillbit.running')
@when('hdfs.joined')
@when_not('drill.hdfs.configured')
def configure_hdfs(client):
    """
        Configure HDFS when a relation is added.
    """
    n = ''
    for conv in client.conversations():
        log(conv.units)
        n = next(s for s in conv.units if s)
    if n:
        n = n.split('/', 1)[0]
    nn = list(client.hosts_map().keys())[list(client.hosts_map().values()).index('namenode-0')]
    port = str(client.port())
    str1 = hookenv.config()['hdfs_formats'][1:-2]
    print(str1)
    fmt = yaml.load(str1)
    log(fmt)
    t = {"name":"juju_hdfs_"+n, "config": {
        "type": "file",
        "enabled": True,
        "connection": "hdfs://"+nn+':'+port,
        "workspaces": {
            "root": {
                "location": hookenv.config()['hdfs_path'],
                "writable": hookenv.config()['hdfs_writeable'],
                "defaultInputFormat": None
            },
        },
        "formats": fmt
    }}
    params = json.dumps(t).encode('utf8')
    req = request.Request('http://localhost:8047/storage/juju_hdfs_'+n+'.json', data=params,headers={'content-type': 'application/json'})
    request.urlopen(req)
    set_state('drill.hdfs.configured')

@when('drillbit.running')
@when('mysql.available')
@when_not('drill.mysql.configured')
def configure_mysql(mysql):
    """
        Configure MySQL when a relation is added.
    """
    log("configuring mysql server"+ mysql.host())
    port2 = str(mysql.port())
    t = {"name":"juju_mysql_"+mysql.host(), "config": {"type": "jdbc","driver": "com.mysql.jdbc.Driver", "url": "jdbc:mysql://"+mysql.host()+":"+port2,"username": mysql.user(), "password":mysql.password(), "enabled": True}}
    params = json.dumps(t).encode('utf8')
    req = request.Request('http://localhost:8047/storage/juju_mysql_'+mysql.database()+'.json', data=params,headers={'content-type': 'application/json'})
    request.urlopen(req)
    set_state('drill.mysql.configured')

@when('drillbit.running')
@when('pgsql.master.available')
@when_not('drill.psql.configured')
def configure_pgsql(psql):
    """
        Configure Postgres when a relation is added.
    """
    n=0
    log("configuring psql server"+ psql.master.host+psql.master.port)
    t = {"name":"juju_psql_"+psql.master.host, "config": {"type": "jdbc","driver": "org.postgresql.Driver", "url": "jdbc:postgresql://"+psql.master.host+":"+str(psql.master.port)+"/"+psql.master.dbname,"username": psql.master.user, "password":psql.master.password, "enabled": True}}
    params = json.dumps(t).encode('utf8')
    req = request.Request('http://localhost:8047/storage/juju_psql_'+psql.master.host+'.json', data=params,headers={'content-type': 'application/json'})
    request.urlopen(req)
    set_state('drill.psql.configured')

@when('drillbit.running')
@when('hbase.ready')
@when_not('drill.hbase.configured')
def configure(hbase):
    n = ''
    p = ''
    for unit in hbase.servers():
        n += unit['host']+","
        p = unit['master_port']

    t = {"name":"juju_hbase_"+n, "config": {"type": "hbase", "size.calculator.enabled": False, "config": { "hbase.zookeeper.quorum": n, "hbase.zookeeper.property.clientport": p}, "enabled": True}}
    params = json.dumps(t).encode('utf8')
    req = request.Request('http://localhost:8047/storage/juju_hbase_'+n+'.json', data=params,headers={'content-type': 'application/json'})
    request.urlopen(req)
    set_state('drill.hbase.configured')

def configure_direct_memory():
    """
        Configure Drill direct memory variable. We figure out if its a % or actual value(in GB).
    """
    if '%' in hookenv.config()['drill_max_direct_memory']:
        direct = calculate_ram(hookenv.config()['drill_max_direct_memory'][:-1])
        if direct < 2:
            direct = str(2)+'G'
        else:
            direct = str(direct)+'G'
        return direct
    elif '%' not in hookenv.config()['drill_max_direct_memory'] and 'G' not in hookenv.config()['drill_max_direct_memory']:
        direct = hookenv.config()['drill_max_direct_memory']+'G'
        return direct
    else:
        return hookenv.config()['drill_max_direct_memory']

def configure_heap():
    """
         Configure the heap. We figure out of its a % or an actual value and configure appropriately.
    """
    if '%' in hookenv.config()['drill_heap']:
        heap = calculate_ram(hookenv.config()['drill_heap'][:-1])
        if heap < 1:
            heap = str(1)+'G'
        else:
            heap = str(heap)+'G'
        return heap
    elif '%' not in hookenv.config()['drill_heap'] and 'G' not in hookenv.config()['drill_heap']:
        heap = hookenv.config()['drill_heap']+'G'
        return heap
    else:
        return hookenv.config()['drill_heap']

def write_memory_file(direct, heap):
    """
         Write the RAM variables to disk.
    """
    t2 = 'DRILL_MAX_DIRECT_MEMORY="'+direct+'"\nDRILL_HEAP="'+heap+'"\nexport DRILL_JAVA_OPTS="-Xms$DRILL_HEAP -Xmx$DRILL_HEAP -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G -Ddrill.exec.enable-epoll=true"\nexport SERVER_GC_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseG1GC "'
    text_file = open("/var/snap/apache-drill-spicule/current/drill/conf/drill-env.sh", "w+")
    text_file.write(t2)
    text_file.close()

def write_zk_file(zookeeper):
    """
        Write the ZK details to disk.
    """
    zklist = ''
    for zk_unit in zookeeper.zookeepers():
        zklist += add_zookeeper(zk_unit['host'], zk_unit['port'])
    zklist = zklist[:-1]
    t = simple_template(zklist)
    if not os.path.exists("/var/snap/apache-drill-spicule/current/drill/conf"):
        os.makedirs("/var/snap/apache-drill-spicule/current/drill/conf")
    text_file = open("/var/snap/apache-drill-spicule/current/drill/conf/drill-override.conf", "w+")
    text_file.write(t)
    text_file.close()

@when('zookeeper.ready')
@when('jdbc.connection.requested')
def provide_connection(zookeeper, jdbc):
      zklist = []
      for zk_unit in jdbc.zookeepers():
        zklist.append(add_zookeeper(zk_unit['host'], zk_unit['port']))
      zoos = ",".join(zklist)
      url = "jdbc:drill:zk="+zoos
      url = url[:-1]
      url = url+'/drill/drill-cluster'
      sn = charmhelpers.core.hookenv.service_name()
      zookeeper.provide_connection(
          service=sn,
          host=sn,
          url=url,
          user=None,
          password=None,
          driver="org.apache.drill.jdbc.Driver",
          extended=None
      )
