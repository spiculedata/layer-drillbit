from charms.reactive import when, when_not, set_state
from subprocess import check_call, CalledProcessError, call, check_output, Popen
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from charmhelpers.core import hookenv


@when_not('drillbit.installed')
def install_drillbit():
    au = ArchiveUrlFetchHandler()
    au.install(hookenv.config()['drill_url'], '/opt/')
    check_output(['tar', 'xvfz', "/opt/apache-drill-1.6.0.tar.gz"])
    set_state('drillbit.installed')
