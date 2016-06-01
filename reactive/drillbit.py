import json
import urllib.request
from charms.reactive import when, when_not, set_state
from subprocess import check_call, CalledProcessError, call, check_output, Popen
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from charmhelpers.core import hookenv
from charmhelpers.core.host import adduser, chownr, mkdir
from charmhelpers.core.hookenv import status_set, log
from charms.reactive.helpers import data_changed
from psutil import virtual_memory


@when_not('drillbit.installed')
def install_drillbit():
    au = ArchiveUrlFetchHandler()
    au.download(hookenv.config()['drill_url'], '/opt/drill.tar.gz')
    mkdir('/opt/drill/')
    check_call(['tar', 'xvfz', '/opt/drill.tar.gz', '-C', '/opt/drill', '--strip-components=1'])
    set_state('drillbit.installed')

@when('zookeeper.joined')
@when_not('zookeeper.ready')
def wait_for_zookeeper(zookeeper):
    hookenv.status_set('waiting', 'Waiting for Zookeeper to become available')

@when_not('zookeeper.joined')
def wait_for_zkjoin():
    hookenv.status_set('waiting', 'Waiting for Zookeeper to become joined')

@when_not('java.ready')
def wait_for_java():
    hookenv.status_set('waiting', 'Waiting for Java')

@when('zookeeper.ready')
@when_not('drillbit.configured')
@when('java.ready')
def configure(java, zookeeper):
    direct = hookenv.config()['drill_max_direct_memory']
    heap = hookenv.config()['drill_heap']
    if '%' in hookenv.config()['drill_max_direct_memory']:
        direct = calculate_ram(hookenv.config()['drill_max_direct_memory'][:-1])
    if '%' in hookenv.config()['drill_heap']:
        heap = calculate_ram(hookenv.config()['drill_heap'][:-1])

    if '%' not in hookenv.config()['drill_max_direct_memory'] and 'G' not in hookenv.config()['drill_max_direct_memory']:
        direct = hookenv.config()['drill_max_direct_memory']+'G'

    if '%' not in hookenv.config()['drill_heap'] and 'G' not in hookenv.config()['drill_heap']:
        heap = hookenv.config()['drill_heap']+'G'

    zklist = ''
    for zk_unit in zookeeper.zookeepers():
        zklist += add_zookeeper(zk_unit['host'], zk_unit['port'])
    zklist = zklist[:-1]
    t = simple_template(zklist)
    text_file = open("/opt/drill/conf/drill-override.conf", "w")
    text_file.write(t)
    text_file.close()
    t2 = 'DRILL_MAX_DIRECT_MEMORY="'+direct+'"\nDRILL_HEAP="'+heap+'"\nexport DRILL_JAVA_OPTS="-Xms$DRILL_HEAP -Xmx$DRILL_HEAP -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G -Ddrill.exec.enable-epoll=true"\nexport SERVER_GC_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseG1GC "'
    text_file = open("/opt/drill/conf/drill-env.sh", "w")
    text_file.write(t2)
    text_file.close()
    start_drill()
    hookenv.open_port('8047')
    set_state('drillbit.configured')
    hookenv.status_set('active', 'Drill up and running')

@when('drillbit.configured', 'zookeeper.ready', 'java.ready')
def configure_zookeepers(zookeeper, java):
    zks = zookeeper.zookeepers()
    if data_changed('available.zookeepers', zks):
        zklist = ''
        for zk_unit in zookeeper.zookeepers():
            zklist += add_zookeeper(zk_unit['host'], zk_unit['port'])
        zklist = zklist[:-1]
        t = simple_template(zklist)
        text_file = open("/opt/drill/conf/drill-override.conf", "w")
        text_file.write(t)
        text_file.close()
        restart_drill()
    if data_changed('direct.memory', hookenv.config('drill_max_direct_memory')) or data_changed('drill.heap', hookenv.config('drill_heap')):
        direct = hookenv.config()['drill_max_direct_memory']
        heap = hookenv.config()['drill_heap']
        if '%' in hookenv.config()['drill_max_direct_memory']:
             direct = calculate_ram(hookenv.config()['drill_max_direct_memory'][:-1])
        if '%' in hookenv.config()['drill_heap']:
             heap = calculate_ram(hookenv.config()['drill_heap'][:-1])
        if '%' not in hookenv.config()['drill_max_direct_memory'] and 'G' not in hookenv.config()['drill_max_direct_memory']:
            direct = hookenv.config()['drill_max_direct_memory']+'G'
        if '%' not in hookenv.config()['drill_heap'] and 'G' not in hookenv.config()['drill_heap']:
            heap = hookenv.config()['drill_heap']+'G'
        t2 = 'DRILL_MAX_DIRECT_MEMORY="'+direct+'"\nDRILL_HEAP="'+heap+'"\nexport DRILL_JAVA_OPTS="-Xms$DRILL_HEAP -Xmx$DRILL_HEAP -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G -Ddrill.exec.enable-epoll=true"\nexport SERVER_GC_OPTS="-XX:+CMSClassUnloadingEnabled -XX:+UseG1GC "'
        text_file = open("/opt/drill/conf/drill-env.sh", "w")
        text_file.write(t2)
        text_file.close()
        restart_drill()

def calculate_ram(percent):
    mem = virtual_memory()
    gb = mem.total/1073741824
    return str(int((int(gb)/100)*int(percent)))+'G'

def add_zookeeper(host, port):
    log('Adding host:port:'+host+':'+port)
    return host+':'+port+','

def simple_template(zk):
    return 'drill.exec: { cluster-id: "'+hookenv.config()['cluster_id']+'", zk.connect: "'+zk+'"}'

def start_drill():
    try:
        log("Checking drill status")
        check_call('./drillbit.sh status', cwd="/opt/drill/bin/", shell=True)
    except CalledProcessError:
        log("Starting Drill.........")
        check_call('./drillbit.sh start', cwd="/opt/drill/bin/", shell=True)

def restart_drill():
    check_call('./drillbit.sh restart', cwd="/opt/drill/bin/", shell=True)

def stop_drill():
    check_call('./drillbit.sh stop', cwd="/opt/drill/bin/", shell=True)

@when('mongodb.database.available')
def configure_mongodb(mongo):
    n = ''
    for conv in mongo.conversations():
        log(conv.units)
        n = next(s for s in conv.units if s)
    if n:
        n = n.split('/', 1)[0]
    t = {"name":"juju_mongo_"+n, "config": {"type": "mongo","connection": "mongodb://"+mongo.connection_string()+"/","enabled": True}}
    params = json.dumps(t).encode('utf8')
    req = urllib.request.Request('http://localhost:8047/storage/juju_mongo_'+n+'.json', data=params,headers={'content-type': 'application/json'})
    urllib.request.urlopen(req)

@when('hdfs.joined')
@when_not('drill.hdfs.configured')
def configure_hdfs(client):
    n = ''
    for conv in client.conversations():
        log(conv.units)
        n = next(s for s in conv.units if s)
    if n:
        n = n.split('/', 1)[0]
    nn = list(client.hosts_map().keys())[list(client.hosts_map().values()).index('namenode-0')]
    port = str(client.port())
    t = {"name":"juju_hdfs_"+n, "config": {
        "type": "file",
        "enabled": True,
        "connection": "hdfs://"+nn+':'+port,
        "workspaces": {
            "root": {
                "location": hookenv.config()['hdfs_path'],
                "writable": True,
                "defaultInputFormat": None
            },
        },
        "formats": {
            "psv": {
                "type": "text",
                "extensions": [
                    "tbl"
                ],
                "delimiter": "|"
            },
            "csv": {
                "type": "text",
                "extensions": [
                    "csv"
                ],
                "delimiter": ","
            },
            "tsv": {
                "type": "text",
                "extensions": [
                    "tsv"
                ],
                "delimiter": "\t"
            },
            "parquet": {
                "type": "parquet"
            },
            "json": {
                "type": "json",
                "extensions": [
                    "json"
                ]
            },
            "avro": {
                "type": "avro"
            },
            "sequencefile": {
                "type": "sequencefile",
                "extensions": [
                    "seq"
                ]
            },
            "csvh": {
                "type": "text",
                "extensions": [
                    "csvh"
                ],
                "extractHeader": True,
                "delimiter": ","
            }
        }
    }}
    params = json.dumps(t).encode('utf8')
    req = urllib.request.Request('http://localhost:8047/storage/juju_hdfs_'+n+'.json', data=params,headers={'content-type': 'application/json'})
    urllib.request.urlopen(req)
    set_state('drill.hdfs.configured')
