import os
from charms.reactive import when, when_not, set_state
from subprocess import check_call, CalledProcessError, call, check_output, Popen
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from charmhelpers.core import hookenv
from charmhelpers.core.host import adduser, chownr, mkdir
from charmhelpers.core.hookenv import status_set, log



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
def configure(zookeeper):
    zklist = ''
    for zk_unit in zookeeper.zookeepers():
        zklist += add_zookeeper(zk_unit['host'], zk_unit['port'])
    zklist = zklist[:-1]
    log('Template:' + zklist)
    t = simple_template(zklist)
    text_file = open("/opt/drill/conf/drill-override.conf", "w")
    text_file.write(t)
    text_file.close()

    set_state('drillbit.configured')
    hookenv.status_set('active', 'Drill up and running')

def add_zookeeper(host, port):
    log('Adding host:port:'+host+':'+port)
    return '"'+host+':'+port+'",'

def simple_template(zk):
    return 'drill.exec: { cluster-id: "'+hookenv.config()['cluster_id']+'", zk.connect: '+zk+'}'
