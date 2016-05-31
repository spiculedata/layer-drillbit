import json
import urllib.request
from charms.reactive import when, when_not, set_state
from subprocess import check_call, CalledProcessError, call, check_output, Popen
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from charmhelpers.core import hookenv
from charmhelpers.core.host import adduser, chownr, mkdir
from charmhelpers.core.hookenv import status_set, log
from charms.reactive.helpers import data_changed



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
    zklist = ''
    for zk_unit in zookeeper.zookeepers():
        zklist += add_zookeeper(zk_unit['host'], zk_unit['port'])
    zklist = zklist[:-1]
    t = simple_template(zklist)
    text_file = open("/opt/drill/conf/drill-override.conf", "w")
    text_file.write(t)
    text_file.close()
    start_drill()
    hookenv.open_port('8047')
    set_state('drillbit.configured')
    hookenv.status_set('active', 'Drill up and running')

@when('drillbit.configured', 'zookeeper.ready')
def configure_zookeepers(zookeeper):
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
    req = urllib.request.Request('http://localhost:8047/storage/juju-mongo.json', data=params,headers={'content-type': 'application/json'})
    response = urllib.request.urlopen(req)


