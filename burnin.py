#!/usr/bin/python
import os
import re
import ast
import ssl
import sys
import json
import time
import shlex
import base64
import inspect
import argparse
import datetime
import subprocess
#import MySQLdb as SQL
import multiprocessing.pool
from subprocess import Popen
from urllib import urlencode
from random import shuffle, sample, choice
from multiprocessing import Process, Pool, TimeoutError
from urllib2 import Request, urlopen, URLError, build_opener, HTTPHandler, HTTPError


# All of the config options are here.
now = datetime.datetime.now();
LOG_FILE = 'burnin.{}-{}-{}_{}{}.log'.format(now.year, now.month, now.day, now.hour%12,'pm' if now.hour/12 else 'am')
# DATA_FILE = 'workload.dat'
API_TARGET = 'http://127.0.0.1'
SSH_OPTIONS="-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=30"

DATA_FILE="burninIOPSResults.data"
BATCHES_OUTPUT_FILE="batches.data"
CONFIG_FILE="burnin.pyconf"

PYTHON_TEST_RESULTS_FILE='burnin_test_results.pydat'
JSON_TEST_RESULTS_FILE  ='burnin_test_results.json'

FAILURE_LIMIT=25
FAILURES=0

ONAPP_ROOT = '/onapp'
ONAPP_CONF_DIR="{}/interface/config".format(ONAPP_ROOT);
ONAPP_CONF_FILE="{}/on_app.yml".format(ONAPP_CONF_DIR);
DB_CONF_FILE="{}/database.yml".format(ONAPP_CONF_DIR);

DEFAULTS = {
    'delay' : 45, # Seconds to delay between batches.
    'vms_per_hypervisor' : 10, # Virtual machines per hypervisor to build
    'resizetarget' : 2, # Amount to increase disk size by in EditDisk
    'maxdisksize' : 20, # Maximum disk size
    'wlduration' : 3, # Workload Duration in minutes
    'interval' : 10, # Seconds to pause between performing dd commands
    'writes' : 0, # Percentage(sort of..) to write to disk instead of read
    'dd_bs' : '10M', # dd command bs value
    'dd_count' : 10 ## dd command count value
    }



HVZONE = 0
## These classes were stolen from the internet,
## However they're for non-daemonizing the processes
## Because occaisionally one needs to spawn some children for a second.
##### I actually may have fixed this, so it may not be necessary anymore but I'm afraid to change it.
class NoDaemonProcess(Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)

class NoDaePool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess

## This function also stolen
def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def is_ip(s):
    a = s.split('.')
    if len(a) != 4:
        return False
    for x in a:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True

# functions that I've made, and are used around.
def avg(l,round_to=False):
    if type(l) is not list: raise TypeError('List required')
    try: sum(l)
    except TypeError: raise TypeError('List contains non-number value')
    try:
        if round_to: return round(sum(l)/len(l), round_to)
        else: return sum(l)/len(l)
    except ZeroDivisionError: return False;

def errorCheck(err, contFlag=True):
    global testVMs;
    global FAILURES;
    logger('Error: Action: {}, Data: {}.'.format(err.func, err.data))
    if err.func == 'dbcall': return True;
    FAILURES += 1;
    if batchsize and contFlag and err.func != 'createvm':
        if FAILURES < FAILURE_LIMIT:
            print('Action {} failed with data {}, attempting to continue.'.format(err.func, err.data));
            return True;
        else:
            print('Action {} failed with data {}. Over {} failures have been encountered, not continuing.')
            return False;
    if err.func == 'destroyvm':
        print('Unable to destroy vm {}.'.format(err.data))
        return True;
    else:
        noyes = raw_input('Action: {} found error:\n\n{}\n\nDelete virtual machines? (y/n): '.format(err.func, err.data))
        while noyes not in ['y','Y','n','N']: raw_input('Invalid input. (y/n)?:');
        if noyes in ['y', 'Y']:
            runJobs([ ['unlockvm',  {'id':vm['id'] }] for vm in testVMs ]);
            runJobs([ ['destroyvm', {'id':vm['id'] }] for vm in testVMs ]);
            sys.exit();
        if noyes in ['n', 'N']:
            print('Leaving virtual machines alone.');
            if contFlag:
                noyesb = raw_input('Continue other tests? (y/[n]): ') or 'n';
                if noyesb in ['y', 'Y']:
                    return True;
            print('Exiting.');
            logger('Program terminated.');
            sys.exit();

def logger(s):
    l = open(LOG_FILE, "a");
    text = '[{}] - {}\n'.format(str(datetime.datetime.now()),s)
    l.write(text)
    l.flush();
    l.close();
    # if VERBOSE: print text.rstrip();

def runCmd(cmd, shell=False, shlexy=True):
    if shlexy and type(cmd) is str:
        cmd = shlex.split(cmd)
    stdout, stderr = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate();
    if stderr and 'Warning: Permanently added' not in stderr:
        logger("Command {} failed, stderr: {}".format(cmd, stderr.strip()))
        return False;
    return stdout.strip();

# try to import MySQLdb, attempt to install if not or raise an error.
try:
    import MySQLdb as SQL
except ImportError:
    print "MySQL not detected, attempting to install automatically..."
    runCmd(['yum','-q','-y','install','MySQL-python'])
    try:
        import MySQLdb as SQL
        print "Imported MySQL properly."
    except:
        print "Couldn't install/import MySQL. Please run `sudo yum -y install MySQL-python`."
        raise



### There are too many arguments here.
####################################
arp = argparse.ArgumentParser(prog='burnin', description='Burn-in script for OnApp');
garp= arp.add_mutually_exclusive_group();
garp.add_argument("-v", "--verbose", help="Verbose output/logging", action="store_true");
garp.add_argument("-q", "--quiet", help="Quiet output", action="store_true");
arp.add_argument("-w", "--workers", metavar='N', help="Number of worker processes for starting jobs. Default: 8", default=8);
arp.add_argument("-d", "--defaults", help="Automatically do all tests and use default values without prompting.", action="store_true");
arp.add_argument("-n", "--nonetwork", help="Do NOT use VM network. Disables the Workload job. Use if cannot connect to virtual machines.", action="store_true")
# arp.add_argument("-c", "--contmode", help="Continue Mode; Workloads will be started again if they are detected as stopped.", action="store_true");
arp.add_argument("-y", "--yes", help="Answer yes to most questions(not custom dd params or error passing)", action="store_true");
arp.add_argument("-b", "--batch", metavar='N', help="Alter batch weight, default is 1.25*(# VMs). This causes between 62-100 percent of VMs being used per batch", default=False)
arp.add_argument("-p", "--pretest", metavar='N', help="Number of minutes to run a burnin pretest, which will run disk workloads on all VMs", default=False);
arp.add_argument("-z", "--zzzz", help="Continue working with -EVERY- virtual machine on the cloud right now. Dev use mainly. Implies -k", action="store_true", default=False)
arp.add_argument("-k", "--keep", help="Keep VMs; Do not delete VMs used for testing at end of test. -z/--zzzz option implies this.", action="store_true", default=False)
arp.add_argument("-g", "--generate", metavar='F', help="Generate output from batch data, in the case of previous failure but you still want the data in proper format.", default=False)
arp.add_argument("-t", "--token", metavar='T', help="Token for submitting to the architecture portal.", default=False)
arp.add_argument("-u", "--user", metavar='U', help="User ID which has API key and permissions. Default is 1 (admin).", default=1)
arp.add_argument("-i", "--iops", metavar='H', help="Just pull IOPS and process them from the past H hours.", default=False)
arp.add_argument("-m", "--minutes", metavar='M', help="Number of minutes to run the test for.", default=720)
arp.add_argument("--clean", help="Delete all VMs used in test from previous failure.", action="store_true")
args = arp.parse_args();
VERBOSE=args.verbose;
quiet=args.quiet;
workers=int(args.workers);
defaults=args.defaults;
using_vm_network=not args.nonetwork;
CONTINUE_MODE=True;  #Previously could change it but I think it's best forced on.
batchsize=args.batch;
autoyes=args.yes;
use_existing_virtual_machines=args.zzzz;
DELETE_VMS=not args.keep;
if use_existing_virtual_machines: DELETE_VMS=False;
run_pre_burnin_test=args.pretest;
SEND_RESULTS = args.token
USER_ID=args.user;
just_iops_duration=args.iops;
ONLY_GENERATE_OUTPUT=args.generate;
DURATION_MINUTES=args.minutes
CLEAN_TEST_VMS=args.clean;
####################################

def pullDBConfig(f):
    confDict = {};
    conf = open(f).read().split('\n');
    curLabel = False;
    for line in conf:
        if ':' not in line: continue;
        if line.startswith('  '):
            tmp = line.strip().split(':');
            confDict[curLabel][tmp[0].strip()] = tmp[1].strip();
        else:
            tmp = line.strip().split(':');
            if tmp[1] == '':
                curLabel = tmp[0].strip()
                confDict[curLabel] = {};
            else: confDict[tmp[0].strip()] = tmp[1].strip();
    logger("Gathered database configuration.");
    return confDict;

DB_CONFIG = pullDBConfig(DB_CONF_FILE);


def pullOAConfig(f):
    confDict = {}
    conf = open(f).read().split('\n');
    for line in conf:
        if ':' not in line: continue;
        tmp = line.strip().split(':');
        if tmp[1].strip() == '' : continue;
        confDict[tmp[0].strip()] = tmp[1].strip().strip('"').strip("'");
    logger("Gathered OnApp configuration.");
    return confDict;

ONAPP_CONFIG= pullOAConfig(ONAPP_CONF_FILE);

def dbConn(conf=None):
    if conf is None:
        conf = DB_CONFIG[DB_CONFIG['onapp_daemon']];
    return SQL.connect(host=conf['host'], user=conf['username'], passwd=conf['password'], db=conf['database'])

def pullAPIKey():
    db = dbConn();
    cur = db.cursor();
    cur.execute("SELECT api_key FROM users WHERE id={}".format(USER_ID));
    res = cur.fetchone()[0];
    cur.close();
    db.close();
    if res == None:
        print('!! API Key is not in database, please ensure the admin user as an API key generated !!');
        sys.exit();
    logger("Pulled API key from database.");
    return res

def pullAPIEmail():
    db = dbConn();
    cur = db.cursor();
    cur.execute("SELECT email FROM users WHERE id={}".format(USER_ID));
    res = cur.fetchone()[0];
    cur.close();
    db.close();
    if res == None:
        print('!! Admin email was not able to be pulled. !!');
        sys.exit();
    logger("Pulled API Email from database.");
    return res

API_AUTH = base64.encodestring("{}:{}".format(pullAPIEmail(), pullAPIKey())).replace('\n', '');
testVMs = [];

# sConfigs = [ line for line in open('/onapp/interface/config/on_app.yml').read().split('\n') if 'simultaneous_' in line ]
# sConfigs = { s.split(':')[0]:s.split(':')[1].lstrip() for s in sConfigs }
# simultaneous_backups_per_hypervisor
# simultaneous_transactions
# simultaneous_storage_resync_transactions
# simultaneous_backups_per_datastore
# simultaneous_backups
# simultaneous_backups_per_backup_server
# simultaneous_migrations_per_hypervisor


def __runJob__(j):
    return j.run();

def __runTimedJob__(j):
    return j.timedRun();

def runAll(j):
    jobData = [ job.run() for job in j ];
    return jobData;

def runParallel(j, timed=False):
    if [job.getAction() for job in j if job.getAction() is not 'VMStatus']:
        if len(j) > 1: logger('Starting parallel jobs: {}'.format([job.getAction() for job in j]))
        if VERBOSE: print "Starting parallel jobs: {}".format([job.getAction() for job in j])
    poolHandler = NoDaePool(workers)
    try:
        if timed: jobData = poolHandler.map(__runTimedJob__, j)
        else:     jobData = poolHandler.map(__runJob__, j)
    except:
        poolHandler.close();
        raise;
    if len(j) > 1: logger('Finished parallel jobs. Returned data: {}'.format(jobData))
    poolHandler.close();
    return jobData

def runStaggeredJobs(j, delay, tout=3600):
    if VERBOSE: logger('Starting Staggered Jobs with {}s delay: {}'.format(delay, j))
    poolHandler = NoDaePool(workers);
    jobData = [];
    handlers = [];
    for job in j:
        if timed: handlers.append(poolHandler.map_async(__runJob__, [job]));
        else:     handlers.append(poolHandler.map_async(__runTimedJob__, [job]));
        time.sleep(delay)
    if VERBOSE: logger('Mapped jobs, looking for results.');
    for n, h in enumerate(handlers):
        try:
            jobData.append(h.get(timeout=tout)[0]);
        except TimeoutError:
            if not quiet: print('Job {} timed out after {} seconds, skipping.'.format(j[n], tout));
            pass;
        except:
            poolHandler.close();
            raise;
        time.sleep(1);
    poolHandler.close();
    #checkJobOutput(j, jobData)
    if VERBOSE: logger('Finished staggered jobs.');
    return jobData;

def filterAPIOutput(data):
    if type(data) is not list:
        return data
    max_keys = 0
    for entry in data:
        ek = entry.keys()
        if len(ek) > max_keys: max_keys = len(ek);
    if max_keys == 1:
        return [ e[data[0].keys()[0]] for e in data ]
    return data;


def dictifyStr(s):
    d = {};
    if type(s) == str: stmp = shlex.split(s);
    else: return s;
    for ii in stmp:
        val = ii.split('=');
        d[val[0]] = val[1];
    return d;

dictify = dictifyStr;

def ListHVs(data):
    url = '/settings/hypervisors.json'
    r = apiCall(url)
    return r;

def ListVMBackups(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/backups.json'.format(data['vm_id']);
    r = apiCall(url)
    return r;

def ListNormalBackups(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/backups/images.json'.format(data['vm_id']);
    r = apiCall(url)
    return r;

def ListIncrementalBackups(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/backups/files.json'.format(data['vm_id']);
    r = apiCall(url)
    return r;

def ListVMDiskBackups(data):
    reqKeys = [ 'vm_id', 'disk_id' ]
    checkKeys(data, reqKeys)
    url = '/virtual_machines/{}/disks/{}/backups.json'.format(data['vm_id'], data['disk_id'])
    r = apiCall(url)
    return r;

def CreateIncrementalBackup(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/backups.json'.format(data['vm_id'])
    if 'note' in data.keys():
        r = apiCall(url, data={"backup:":{"note":data['note']}}, method='POST')
    else:
        r = apiCall(url, method='POST')
    if len(r) == 1: return r;
    elif type(r) is dict and len(r.keys()) == 1: return r['backup']
    else: return r[0];

def CreateDiskBackup(data):
    checkKeys(data, ['disk_id'])
    url = '/settings/disks/{}/backups.json'.format(data['disk_id'])
    if 'note' in data.keys():
        r = apiCall(url, data={"backup:":{"note":data['note']}}, method='POST')
    else:
        r = apiCall(url, method='POST')
    return r;

def DeleteBackup(data):
    checkKeys(data, ['backup_id'])
    url = '/backups/{}.json'.format(data['backup_id'])
    r = apiCall(url, method='DELETE')
    return r;

def RestoreBackup(data):
    checkKeys(data, ['backup_id'])
    url = '/backups/{}/restore.json'.format(data['backup_id'])
    r = apiCall(url, method='POST')
    return r;

def DetailBackup(data):
    checkKeys(data, ['backup_id'])
    url = '/backups/{}.json'.format(data['backup_id'])
    r = apiCall(url)
    return r;

def ListVMs(data=None, short=True):
    url = '/virtual_machines/per_page/all.json'
    if short: url += '?short'
    r = apiCall(url)
    return r

def ListShortVMs(data):
    r = ListVMs(data, short=True);
    return r;

def DetailVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}.json'.format(data['vm_id'])
    r = apiCall(url);
    return r

def AllVMStatuses(data):
    url = '/virtual_machines/status.json'
    r = apiCall(url);
    return r

def VMStatus(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/status.json'.format(data['vm_id'])
    r = apiCall(url);
    return r

def CreateVM(data):
    reqKeys = [
      'memory', 'cpus', 'cpu_shares',
      'hostname', 'label', 'primary_disk_size',
      'required_virtual_machine_build',
      'required_ip_address_assignment',
      'template_id']
    checkKeys(data, reqKeys)
    url = '/virtual_machines.json'
    r = apiCall(url, data={"virtual_machine":data}, method='POST')
    return r

def BuildVM(data):
    reqKeys = [ 'vm_id', 'template_id' ]
    checkKeys(data, reqKeys)
    url = '/virtual_machines/{}/build.json'.format(data['vm_id'])
    datanoid = dict(data)
    del datanoid['vm_id'];
    r = apiCall(url, data={"virtual_machine":datanoid}, method='POST')
    return r;

def EditVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}.json'.format(data['vm_id'])
    datanoid = dict(data)
    del datanoid['vm_id'];
    r = apiCall(url, data={"virtual_machine":datanoid}, method='POST')
    return r;


def UnlockVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/unlock.json'.format(data['vm_id'])
    r = apiCall(url, method='POST')
    return r;

def MigrateVM(data):
    reqKeys = [ 'vm_id', 'destination' ]
    checkKeys(data, reqKeys)
    url = '/virtual_machines/{}/migration.json'.format(data['vm_id'])
    datanoid = dict(data)
    del datanoid['vm_id']
    r = apiCall(url, data={"virtual_machine":datanoid}, method='POST')
    return r;

def DeleteVM(data):
    checkKeys(data, ['vm_id'])
    dk = data.keys()
    url = '/virtual_machines/{}.json'.format(data['vm_id'])
    if 'convert_last_backup' in dk or 'destroy_all_backups' in dk:
        url += '?'
        if 'convert_last_backup' in dk:
            url += 'convert_last_backup={}'.format(data['convert_last_backup'])
            if 'destroy_all_backups' in dk:
                url += '&'
        if 'destroy_all_backups' in dk:
            url += 'destroy_all_backups={}'.format(data['destroy_all_backups'])
    r = apiCall(url, method='DELETE')
    return r;

def StartVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/startup.json'.format(data['vm_id'])
    if 'recovery' in data.keys() and data['recovery'] == True:
        r = apiCall(url, data={"mode":"recovery"}, method='POST')
    else:
        r = apiCall(url, method='POST')
    return r;

def RebootVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/reboot.json'.format(data['vm_id'])
    r = apiCall(url, method='POST')
    return r;

def ShutdownVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/shutdown.json'.format(data['vm_id'])
    r = apiCall(url, method='POST')
    return r;

def StopVM(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/stop.json'.format(data['vm_id'])
    r = apiCall(url, method='POST')
    return r;

def ListVMDisks(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/disks.json'.format(data['vm_id'])
    r = apiCall(url)
    return r;

def DetailVMDisk(data):
    checkKeys(data, ['vm_id'])
    url = '/virtual_machines/{}/disks/{}.json'.format(data['vm_id'], data['disk_id'])
    r = apiCall(url);
    return r;

def EditDisk(data):
    checkKeys(data, ['disk_id'])
    url = '/settings/disks/{}.json'.format(data['disk_id'])
    datanoid = dict(data)
    del datanoid['disk_id']
    r = apiCall(url, data={"disk":datanoid}, method='PUT')
    return r;

def GetDiskIOPS(data):
    checkKeys(data, ['disk_id'])
    url = '/settings/disks/{}/usage.json'.format(data['disk_id'])
    r = apiCall(url)
    return r;

def HealthcheckVM(data):
    d={}
    if type(data) is dict:
        ks = data.keys();
        if "ip_address" in ks:
            d['ip_address'] = data['ip_address']
        if "identifier" in ks:
            d['identifier'] = data['identifier']
    elif type(data) is str:
        if is_ip(data):
            d = {'ip_address', data}
        else:
            d = {'identifier', data}
    d["cmd"] = "loadavg"
    loadavg = runOnVM(d)
    if not loadavg:
        return False
    else:
        return True

def ListHVsInZone(data):
    checkKeys(data, ['hv_zone_id'])
    url = '/settings/hypervisor_zones/{}/hypervisors.json'.format(data['hv_zone_id'])
    r = apiCall(url, method='GET')
    return r;

def runOnVM(data, raiseErrors=False):
    ks = data.keys();
    if "cmd" not in ks:
        raise KeyError('Function runOnVM requires cmd key in data')
    if "ip_address" not in ks and "identifier" in ks:
        vm_ip_addr = dsql("SELECT ip_address FROM virtual_machines WHERE identifier={}".format(data['identifier']))
    elif "ip_address" in ks:
        vm_ip_addr = data['ip_address']
    cur_cmd_full = 'ssh {} root@{} "{}"'.format(SSH_OPTIONS, vm_ip_addr, data['cmd'])
    cur_cmd = ['su', 'onapp', cur_cmd_full]
    p = Popen(cur_cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE);
    stdo, stde = p.communicate();
    if raiseErrors and len(stde): raise OnappException(cur_cmd, 'runOnVM', stde)
    elif len(stde): return stde
    return stdo;

def getTemplate():
    if use_existing_virtual_machines: return 0
    if VERBOSE: print('Pulling templates list:');
    templateStoreData = apiCall('/template_store.json')
    avail_templates = {}
    for store in templateStoreData:
        for template in store['relations']:
            if template['image_template']['cdn']: continue;
            if template['image_template']['operating_system_distro'] == 'lbva': continue;
            avail_templates[template['template_id']] = template['image_template']['manager_id']
    if len(avail_templates) == 1:
        k = avail_templates.keys()
        print('Only one template found: {}'.format(avail_templates[k[0]]))
        print('Selecting this template automatically.');
        return k[0];
    for tid in avail_templates.keys():
        print('{:>3}. {}'.format(tid, avail_templates[tid]))
    tm = raw_input('Provide template ID to use: ');
    while not tm.isdigit() and tm not in avail_templates.keys(): tm = raw_input('Not an ID. Provide ID: ');
    t = avail_templates[int(tm)]
    return int(tm);

def dRunQuery(q, unlist=True):
    db = dbConn();
    cur = db.cursor();
    cur.execute(q)
    res = cur.fetchall();
    cur.close();
    db.close();
    if len(res) == 1 and unlist:
        if len(res[0]) == 1: return res[0][0];
        else: return res[0]
    return res;

dsql = dRunQuery;

# def dRunPrettyQuery(fields, table, conditions):
#     if type(fields) == str: fields = [fields];
#     query = 'SELECT {} FROM {} WHERE {}'.format(','.join(fields), table, conditions);
#     res = dsql(query, False);
#     output = [];
#     for n, r in enumerate(res):
#         o = {}
#         for nn, fld in enumerate(fields):
#             o[fld] = res[n][nn];
#         output.append(o)
#     return output;


def dRunPrettyQuery(q, unlist=True):
    if VERBOSE: logger("Running pretty query:{}".format(' '.join(q.split())))
    db = dbConn();
    cur = db.cursor();
    cur.execute(q)
    res = cur.fetchall();
    num_fields = len(cur.description)
    field_names = [i[0] for i in cur.description]
    cur.close();
    db.close();
    if num_fields == 1 and len(res) == 1 and unlist:
        return res[0][0]
    if num_fields == 1 and len(res) == 1 and not unlist:
        return {field_names[0] : res[0][0]}
    output = [];
    for n, r in enumerate(res):
        o = {}
        for nn, fld in enumerate(field_names):
            if type(res[n][nn]) is datetime.datetime:
                o[fld] = str(res[n][nn])
            else:
                o[fld] = res[n][nn];
        output.append(o)
    if len(output) == 1 and unlist: return output[0]
    if len(output) == 0: return False;
    return output;

dpsql = dRunPrettyQuery;

def dGetDiskID(disk):
    #{'datastore': 'k7blx48pwsvmya', 'uuid': 'l61uni90j7zgxw', 'size': '5'}
    res = dsql('SELECT id FROM disks WHERE identifier=\'{}\''.format(disk['uuid']))
    if res == None:
        print('Disk {} was not found in database!'.format(str(disk)))
        raise OnappException('dbcall', 'getDiskID, {}'.format(uuid or disk_id))
    return res;

def dGetDiskSize(uuid=None, disk_id=None):
    res = None;
    if uuid: res = dsql('SELECT disk_size FROM disks WHERE identifier=\'{}\''.format(uuid))
    if disk_id: res = dsql('SELECT disk_size FROM disks WHERE id=\'{}\''.format(disk_id))
    if res == None:
        print('Could not retrieve size for disk {}'.format(uuid or disk_id))
        raise OnappException('dbcall', 'getDiskSize, {}'.format(uuid or disk_id))
    return res;

def dGetVMFromBackupID(bkpid):
    ttypeid = dsql('SELECT target_type, target_id FROM backups WHERE id={}'.format(bkpid))
    if ttypeid == None:
        print('Getting backup {} data failed.'.format(bkpid))
        raise OnappException('dbcall', 'getVMFromBackupID1, {}'.format(bkpid))
    if ttypeid[0] == 'Disk':
        tid = dsql('SELECT virtual_machine_id FROM disks WHERE id={}'.format(ttypeid[1]))
    else:
        tid = dsql('SELECT identifier FROM virtual_machines WHERE id={}'.format(ttypeid[1]))
    if tid == None:
        return "FAIL";
        print('Getting VM ID from Backup ID {} failed.'.format(bkpid))
        raise OnappException('dbcall', 'getVMFromBackupID2, {}'.format(bkpid))
    return tid;

def dListHVZones():
    zones = []
    res = dpsql("SELECT id, label FROM packs WHERE type='HypervisorGroup'", unlist=False)
    if res == None:
        print('Could not list HV Zones.')
        raise OnappException('dbcall', 'listHVZones')
    return res;

def dListHVsFromZone(zone):
    res = dpsql("SELECT id, label, ip_address, mac FROM hypervisors \
            WHERE hypervisor_group_id={}".format(zone), unlist=False)
    if res == None:
        print('Could not list HVs from zone {}'.format(zone))
        raise OnappException('dbcall', 'listHVsFromZone')
    return res;

# def dDetailVM(vm):
#     req = dsql(''.format(vm));

class Job(object):
    def __init__(self, action, data=None, **kwdata):
        self.action = action;
        if not data: self.data = {};
        else: self.data = data;
        self.data.update(kwdata)

    def __repr__(self):
        if self.action == 'batchRunnerJob':
            return "<Onapp.Job batch-controlled action:{}>".format(self.data['func'])
        return "<Onapp.Job action:{}>".format(self.action);

    def __str__(self):
        if self.action == 'batchRunnerJob':
            return 'Onapp Job object, batch-controlled action: {}|data: {}'.format(self.action, self.data)
        return 'Onapp Job object, action: {}|data: {}'.format(self.action, self.data);

    def addData(self, **kwdata):
        for key, value in kwdata.iteritems():
            self.data[key] = value;

    def delData(self, key):
        del self.data[key]

    def clearData(self):
        self.data = {};

    def getAction(self):
        if self.action == 'batchRunnerJob':
            return self.data['func'];
        else:
            return self.action;

    def run(self):
        global FAILURES;
        if callable(self.action):
            if self.action in [ runAll , runParallel, runStaggeredJobs ]: raise ValueError('{} is not valid for a Job action.'.format(self.action))
            data = self.action(self.data)
        elif self.action not in globals().keys():
            raise OnappException('{}.run{}'.format(self.action), self.data, 'Function Onapp.{} does not exist.'.format(self.action))
        if type(self.action) is str:
            if self.action in ['runAll' , 'runParallel' , 'runStaggeredJobs' ]:
                raise ValueError('{} is not valid for a Job action.'.format(self.action))
            try:
                data = globals()[self.action](self.data);
            except HTTPError: raise;
            except OnappException as err:
                print "Error in job {} : {}".format(self.action, sys.exc_info()[0]);
                # error here about failures not being defined
                FAILURES += 1
                if FAILURES >= FAILURE_LIMIT: raise OnappException('{}.run{}'.format(self.action), self.data, "Failure limit has been reached.")
                return False;
            except:
                e = sys.exc_info()
                print "!!!!! ERROR OCCURRED INSIDE JOB {} : {} line {} !!!!!\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!".format(self.action, e, e[2].tb_lineno)
                print str(self.data)
                FAILURES += 1
                if FAILURES >= FAILURE_LIMIT: raise OnappException('runJob', sys.exc_info()[0], 'Internal errors above {}'.format(FAILURE_LIMIT))
                return False;
        if type(data) is dict and len(data.keys()) == 1:
            return data.values()[0];
        else:
            return data;

    def timedRun(self):
        global FAILURES;
        beginTime = datetime.now();
        if callable(self.action):
            if self.action in [ runAll , runParallel, runStaggeredJobs ]: raise ValueError('{} is not valid for a Job action.'.format(self.action))
            data = self.action(self.data)
        elif self.action not in globals().keys():
            raise OnappException('{}.run'.format(self.action), self.data, 'Function Onapp.{} does not exist.'.format(self.action))
        if type(self.action) is str:
            if self.action in ['runAll' , 'runParallel' , 'runStaggeredJobs' ]:
                raise ValueError('{} is not valid for a Job action.'.format(self.action))
            try:
                data = globals()[self.action](self.data);
            except OnappException as err:
                print "Error in job {}"
                if not CONTINUE_MODE: raise
                FAILURES += 1
                if FAILURES >= FAILURE_LIMIT: raise OnappException('{}.timedRun{}'.format(self.action), self.data, "Failure limit has been reached.")
            except:
                print "!!!!! ERROR OCCURRED INSIDE JOB {} : {} !!!!!\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!".format(self.action, sys.exc_info())
                raise
        timeTook = datetime.now() - beginTime;
        if type(data) is dict and len(data.keys()) == 1:
            return (data.values()[0], timeTook);
        else:
            return (data, timeTook);


def checkKeys(data, reqKeys):
    dk = data.keys();
    caller = inspect.stack()[1][3];
    for k in reqKeys:
        if k not in dk: raise KeyError('{} requires data key {}'.format(caller, k))

def apiCall(r, data=None, method='GET', target=API_TARGET, auth=API_AUTH):
    req = Request("{}{}".format(target, r), json.dumps(data))
    if auth: req.add_header("Authorization", "Basic {}".format(auth))
    req.add_header("Accept", "application/json")
    req.add_header("Content-type", "application/json")
    if method: req.get_method = lambda: method;
    try:
        if target.startswith('https://'):
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            ssl_context.load_default_certs();
            response = urlopen(req, context=ssl_context)
        else:
            response = urlopen(req)
        status = response.getcode();
    except HTTPError as err:
        caller = inspect.stack()[1][3];
        print caller,"called erroneous API request: {}{}, error: {}".format(target, r, err)
        status = response.getcode()
        # if r.endswith('status.json'): raise;
        # else: return False;
        raise;
    try:
        status;
    except NameError:
        status = response.getcode();
    if VERBOSE and 'status.json' not in r: logger('API Call executed - {}{}, Status code: {}'.format(API_TARGET, r, status));
    apiResponse = response.read().replace('null', 'None').replace('true', 'True').replace('false', 'False')
    if apiResponse != '':
        pyResponse = ast.literal_eval(apiResponse)
        retData = filterAPIOutput(pyResponse)
    else:
        retData = False;
    if status in [200, 201, 204]:
        return retData;
    else:
        raise OnappException('apiCall', "{}: Unknown HTTP Status code".format(status), caller)

def storageAPICall(target, r, data=None, method=None):
    req = Request("http://{}:8080{}".format(target, r), data)
    if method: req.get_method = lambda: method;
    response = urlopen(req)
    status = response.getcode()
    caller = inspect.stack()[1][3];
    # print 'API Call executed - {}{}, Status code: {}'.format(target, r, status);
    return ast.literal_eval(response.read().replace('null', 'None').replace('true', 'True').replace('false', 'False'));

stapi = storageAPICall
#
#
# def aGetStorageNodeStats(data):
#     # Filter types:
#     # 0 - number of read IOs processed
#     # 1 - number of read IOs merged with in-queue IO
#     # 2 - number of sectors read
#     # 3 - total wait time for read requests
#     # 4 - number of write IOs processed
#     # 5 - number of write IOs merged with in-queue IO
#     # 6 - number of sectors written
#     # 7 - total wait time for write requests
#     # 8 - number of IOs currently in flight
#     # 9 - total time this block device has been active
#     # 10 - total wait time for all requests
#     call = '/storage/{}/nodes/{}/io_stats.json?filter[start]={}&filter[finish]={}&filter[type]={}&filter[local]=1'.format(
#         HVZONE,data['node'],data['startTime'].strftime('%Y-%m-%d+%H:%M'),
#         data['endTime'].strftime('%Y-%m-%d+%H:%M'), data['filterType']).replace(' ', '%20');
#     storageStats = apiCall(call);
#     stats = [ (s['node_io_stat']['created_at'],s['node_io_stat']['value']) for s in storageStats ]
#     return storageStats;
#
def GetVMIOPS(vm, period=None):
    disks = Job('ListVMDisks', vm_id=vm['id']).run()
    vmiops = {}
    if 'period' in vm.keys():
        period = vm['period']
    for disk in disks:
        if period:
            vmiops[disk['id']] = apiCall( '/settings/disks/{}/usage.json?period[startdate]={}&period[enddate]={}'.format( \
                disk['id'], period['start'].strftime('%Y-%m-%d+%H:%M'),  \
                period['end'].strftime('%Y-%m-%d+%H:%M')).replace(' ', '%20'));
        else:
            vmiops[disk['id']] = apiCall( '/settings/disks/{}/usage.json'.format(disk['id']) );
    iopsData = {};
    for diskid, iops in vmiops.iteritems():
        iopsData[diskid] = {'reads':[],'writes':[],'dataread':[],'datawrite':[],'stattimes':[]}
        for d in iops:
            iopsData[diskid]['reads'].append(d['reads_completed']);
            iopsData[diskid]['writes'].append(d['writes_completed']);
            iopsData[diskid]['dataread'].append(d['data_read']);
            iopsData[diskid]['datawrite'].append(d['data_written']);
            iopsData[diskid]['stattimes'].append(str(datetime.datetime.strptime(d['stat_time'], '%Y-%m-%dT%X.%fZ')))
    return vm['id'], iopsData;

def DeployWorkloadFile(file_path='/tmp'):
    file='{}/workload.sh'.format(file_path)
    data = """#!/usr/bin/env sh

    RATIO=$1
    SLEEP=$2
    VIRT_TYPE=$3
    OS_TYPE=$4
    DD_BS=$5
    DD_COUNT=$6

    if [ -s /tmp/dd.log ] ; then
      echo '' > /tmp/dd.log
    fi

    if [ $VIRT_TYPE = "kvm" ]; then
      if [ $OS_TYPE = "freebsd" ]; then
        DISK=/dev/vtbd0s1a
      else
        DISK=/dev/vda1
      fi
    elif [ $VIRT_TYPE = "xen" ]; then
      DISK=/dev/xvda1
    else
      echo "Error: Unknown virtualization type"
    fi

    while true; do
      VALUE=$((RANDOM%101))
      if test $VALUE -ge $RATIO; then

        if [ $OS_TYPE = "linux" ]; then
          FLAGS="iflag=direct"
        fi
        # echo -n "READ " #>>/tmp/dd.log
        OUT=$(dd if=$DISK of=/dev/null bs=$DD_BS count=$DD_COUNT $FLAGS 2>&1 | grep copied) #>> /tmp/dd.log
        echo "READ ${OUT}"
      else

        if [ $OS_TYPE = "linux" ]; then
          FLAGS="oflag=direct"
        fi
        # echo -n "WRITE " #>>/tmp/dd.log
        OUT=$(dd if=/dev/zero of=/root/workload.bin bs=$DD_BS count=$DD_COUNT $FLAGS 2>&1 | grep copied) #>> /tmp/dd.log
        echo "WRITE ${OUT}"
      fi

      if test $? -ne 0; then
        echo "Test failed"# >> /tmp/dd.log
        exit 0
      fi
      sync
      sleep $SLEEP
    done
"""
    f = open(file, "w")
    f.write(data)
    f.flush();
    f.close();

def CopyWorkloadFile(vm, file='/tmp/workload.sh'):
    if type(vm) is str:
        if is_ip(data):
            VM_IP=vm
        else:
            raise ValueError('copyWorkloadFile was given string that is NOT an IP address.\nProvide either IP or VM Dictionary')
    elif type(vm) is dict:
        VM_IP = vm['ip_addresses'][0]['ip_address']['address']
    else:
        raise TypeError('copyWorkloadFile requires either IP addres string or VM Dictionary')
    copy_cmd = ['su','onapp','-c','scp {} {} root@{}:/tmp'.format(SSH_OPTIONS, file, VM_IP)]
    cmdOut = runCmd(copy_cmd)
    check_cmd = ['su','onapp','-c','ssh {} root@{} "du /tmp/workload.sh" 2>/dev/null'.format(SSH_OPTIONS, VM_IP)]
    cmdOut = runCmd(check_cmd)
    if not cmdOut:
        cmdOut = runCmd(copy_cmd)
        cmdOut = runCmd(check_cmd)
        if not cmdOut:
            raise OnappException(VM_IP, 'copyWorkloadFile', 'Unable to detect file on remote virtual machine.')
        else:
            return True;
    else:
        return True;

def StartVMWorkload(data):
    if type(data) is not dict:
        raise TypeError('startVMWorkload requires dictionary of workload values and VM.')
    checkKeys(data, ['virtual_machine', 'ratio', 'interval', 'dd_bs', 'dd_count'])
    VM_IP = data['virtual_machine']['ip_addresses'][0]['ip_address']['address']
    if CheckVMWorkload(data['virtual_machine']): return True
    VIRT_TYPE = dsql("SELECT hypervisor_type FROM hypervisors WHERE id={}".format(data['virtual_machine']['hypervisor_id']))
    OS_TYPE = dsql("SELECT operating_system FROM virtual_machines WHERE id={}".format(data['virtual_machine']['id']))
    cmd = ['su','onapp','-c','ssh {} root@{} "nohup sh /tmp/workload.sh {} {} {} {} {} {} >> /tmp/dd.log 2>&1 &"' \
        .format(SSH_OPTIONS, VM_IP, data['ratio'], data['interval'], VIRT_TYPE, OS_TYPE, data['dd_bs'], data['dd_count'])]
    out = runCmd(cmd);
    time.sleep(2);
    return CheckVMWorkload(data['virtual_machine'])

def CheckVMWorkload(data):
    if type(data) is not dict:
        raise TypeError('CheckVMWorkload requires dictionary of virtual machine')
    VM_IP = data['ip_addresses'][0]['ip_address']['address']
    cmd = ['su','onapp','-c','ssh {} root@{} "pgrep -f /tmp/workload.sh"'.format(SSH_OPTIONS, VM_IP)]
    pid = runCmd(cmd)
    if pid:
        return True
    else:
        return False;

def StopVMWorkload(data):
    if type(data) is str:
        if is_ip(data):
            VM_IP=data
        else:
            raise ValueError('stopVMWorkload was given string that is NOT an IP address.\nProvide either IP or VM Dictionary')
    elif type(data) is dict:
        VM_IP = data['ip_addresses'][0]['ip_address']['address']
    else:
        raise TypeError('stopVMWorkload requires either IP addres string or VM Dictionary')
    cmd = ['su','onapp','-c','ssh {} root@{} "pkill -f /tmp/workload.sh"'.format(SSH_OPTIONS, VM_IP)]
    out = runCmd(cmd)
    time.sleep(1)
    return CheckVMWorkload(data)

def GetVMWorkloadLog(data):
    if type(data) is not dict: raise TypeError('GetVMWorkloadLog requires virtual machine dictionary')
    VM_IP = data['ip_addresses'][0]['ip_address']['address']
    VM_ID = data['id']
    cmd = ['su','onapp','-c','ssh {} root@{} "cat /tmp/dd.log" 2>/dev/null || echo "FAIL"'.format(SSH_OPTIONS, VM_IP)]
    log = runCmd(cmd)
    if log.strip() == 'FAIL':
        # raise OnappException(VM_IP, 'GetVMWorkloadLog', 'Failed to find the log on the target server.')
        return {'vm_id':VM_ID, 'data':False}
    data = {'reads':[], 'writes':[]}
    for line in log.split('\n'):
        speed, unit = line.split(',')[2].split()
        speed = float(speed)
        if unit == 'GB/s':
            speed = speed*1024.0
        elif unit != 'MB/s':
            raise TypeError('unimplemented rate, speed: {}, rate: {}'.format(speed, rate))
        if line[0:4] == 'READ':
            data['reads'].append(speed)
        elif line[0:5] == 'WRITE':
            data['writes'].append(speed)
        else:
            raise TypeError('neither read nor write.')
    return {'vm_id': VM_ID, 'data':data};

def SingleWorkload(data):
    checkKeys(data, ['virtual_machine', 'ratio', 'interval', 'dd_bs', 'dd_count', 'duration'])
    job = Job('StartVMWorkload', data).run()
    if job is False:
        checks = 0
        while job is False and checks < 10:
            time.sleep(5)
            job = Job('StartVMWorkload', data).run()
            checks+=1
            if checks == 10:
                raise OnappException('SingleWorkload', 'Workload failed to start 10 times on virtual machine id {}'.format(data['virtual_machine']['id']))
            print "Trying again ({})...".format(checks)
    dur = datetime.timedelta(minutes=int(data['duration']))
    wData = data.copy()
    wData['duration'] = dur
    watchVMWorkload(wData)
    job = Job('StopVMWorkload', data['virtual_machine']).run()
    job = Job('GetVMWorkloadLog', data['virtual_machine']).run()
    if job: return job;
    else: raise OnappException('SingleWorkload', 'Uknown workload failure, log was not present.')

def BatchWorkload(vms, duration, params):
    print "Starting workload for VMs: {}".format([vm['id'] for vm in vms])
    jobs = [ Job('StartVMWorkload', virtual_machine=vm, **params) for vm in vms ]
    wl_status = runParallel(jobs)
    if False in wl_status:
        print "Workloads may not have all started, trying one more time."
        wl_status = runParallel(jobs)
        if False in wl_status:
            print "One or more workloads did not start. Stopping all and ending."
            runParallel([Job('StopVMWorkload', vm) for vm in vms])
            return False
        else:
            print "All started the second time."
    else:
        print "All started the first time."
    print "Waiting for workload duration."
    wData = params.copy()
    wData['duration'] = duration
    jobs = [ Job('watchVMWorkload', virtual_machine=vm, duration=duration, **params) for vm in vms ]
    print "Watching workloads"
    wl_watchers = runParallel(jobs)
    print "Duration complete! Stopping workloads."
    jobs = [ Job('StopVMWorkload', vm) for vm in vms ]
    wl_statss = runParallel(jobs)
    if True in wl_status:
        print "Not all workloads stopped, trying again."
        wl_status = runParallel(jobs)
        if True in wl_status:
            print "One or more workloads couldn't be stopped. Please stop the virtual machines."
        else:
            print "All stopped the second time."
    else:
        print "All stopped the first time."
    print "Gathering workload logs."
    rData = { vm['id'] : Job('GetVMWorkloadLog', vm).run() for vm in vms }
    return rData;

def stallUntilOnline(vms, timeout=300, bTime=None):
    if type(vms) is dict:
        vm_ids = {vms['id']:False}
    elif type(vms) is int or type(vms) is long:
        vm_ids = {vms:False};
    elif type(vms) is list:
        if len(vms) == 0: return True;
        if type(vms[0]) is int:
            vm_ids = { vm : False for vm in vms }
        elif type(vms[0]) is dict:
            vm_ids = { vm['id'] : False for vm in vms }
    else:
        raise OnappException('Unexpected data for virtual machine ID(s)')
    beginTime = datetime.datetime.now()
    logger('Waiting for VMs to come online: {}'.format(','.join([str(v) for v in vm_ids.keys()])))
    while False in vm_ids.values():
        if datetime.datetime.now() - beginTime > datetime.timedelta(seconds=timeout):
            raise OnappException('Timed out waiting for VMs to come online, still offline:', [t for t in vm_ids.keys() if not vm_ids[t]])
        jobs = [ Job('VMStatus', {'vm_id':vm}) for vm in vm_ids.keys() if not vm_ids[vm] ]
        if len(jobs) > 1: jobData = runParallel(jobs)
        else: jobData = runAll(jobs)
        for j in jobData:
            if j['booted'] and j['built'] and not j['locked']:
                if using_vm_network:
                    VM_IP = dsql("SELECT INET_NTOA(ip.address) FROM virtual_machines AS vm \
                        JOIN networking_network_interfaces AS nif ON nif.virtual_machine_id = vm.id \
                        JOIN networking_ip_address_joins AS nipj ON nipj.network_interface_id = nif.id \
                        JOIN networking_ip_addresses AS ip ON ip.id = nipj.ip_address_id \
                        WHERE vm.id={}".format(j['id']))
                    cmd = ['su','onapp','-c','ssh {} root@{} "uptime"'.format(SSH_OPTIONS, VM_IP)]
                    status = runCmd(cmd)
                    if status is False: continue;
                vm_ids[j['id']] = True;
        time.sleep(2)
    logger('VMs {} have come online.'.format(','.join([str(v) for v in vm_ids.keys()])))
    if VERBOSE:
        if type(vms) is int or type(vms) is long:
            print "VM", vms, "has come online."
        else:
            print "VMs", ','.join([str(v) for v in vm_ids.keys()]), "have come online."

def stallUntilOffline(vms, timeout=3600, bTime=None):
    if type(vms) is dict:
        vm_ids = {vms['id']:False}
    elif type(vms) is int or type(vms) is long:
        vm_ids = {vms:False};
    elif type(vms) is list:
        if len(vms) == 0: return True;
        if type(vms[0]) is int:
            vm_ids = { vm : False for vm in vms }
        elif type(vms[0]) is dict:
            vm_ids = { vm['id'] : False for vm in vms }
    else:
        raise OnappException('Unexpected data for virtual machine ID(s)')
    beginTime = datetime.datetime.now()
    logger('Waiting for VMs to go offline: {}'.format(','.join([str(v) for v in vm_ids.keys()])))
    while False in vm_ids.values():
        if datetime.datetime.now() - beginTime > datetime.timedelta(seconds=timeout):
            raise OnappException('Timed out waiting for VMs to go offline:', [t[0] for t in vm_ids.keys() if not vm_ids[t]])
        jobs = [ Job('VMStatus', {'vm_id':vm}) for vm in vm_ids.keys() if not vm_ids[vm] ]
        if len(jobs) > 1: jobData = runParallel(jobs)
        else: jobData = runAll(jobs)
        for j in jobData:
            if j is False:
                vm_ids[j['id']] = True
            elif not j['booted'] and not j['locked']:
                vm_ids[j['id']] = True;
        time.sleep(1)
    time.sleep(1)
    logger('VMs {} have gone offline.'.format(','.join([str(v) for v in vm_ids.keys()])))
    if VERBOSE:
        if type(vms) is int or type(vms) is long:
            print "VM", vms, "has gone offline."
        else:
            print "VMs", ','.join([str(v) for v in vm_ids.keys()]), "have gone offline."

def stallUntilDeleted(vms, timeout=3600, bTime=None):
    if type(vms) is dict:
        vm_ids = {vms['id']:False}
    elif type(vms) is int or type(vms) is long:
        vm_ids = {vms:False};
    elif type(vms) is list:
        if len(vms) == 0: return True;
        if type(vms[0]) is int:
            vm_ids = { vm : False for vm in vms }
        elif type(vms[0]) is dict:
            vm_ids = { vm['id'] : False for vm in vms }
    else:
        raise OnappException('Unexpected data for virtual machine ID(s)')
    beginTime = datetime.datetime.now()
    logger('Waiting for VMs to disappear: {}'.format(','.join([str(v) for v in vm_ids.keys()])))
    while False in vm_ids.values():
        if datetime.datetime.now() - beginTime > datetime.timedelta(seconds=timeout):
            raise OnappException('Timed out waiting for VMs to delete:', [t[0] for t in vm_ids.keys() if not vm_ids[t]])
        jobs = [ Job('VMStatus', {'vm_id':vm}) for vm in vm_ids.keys() if not vm_ids[vm] ]
        for j in jobs:
            try:
                tmp = j.run()
            except HTTPError:
                vm_ids[j.data['vm_id']] = True
            if tmp is False:
                vm_ids[j.data['vm_id']] = True
        time.sleep(1);
    logger('VMs {} have been deleted.'.format(','.join([str(v) for v in vm_ids.keys()])))
    if VERBOSE:
        if type(vms) is int or type(vms) is long:
            print "VM", vms, "has been destroyed."
        else:
            print "VMs", ','.join([str(v) for v in vm_ids.keys()]), "have been destroyed."

def stallUntilBackupBuilt(backup, timeout=3600):
    beginTime = datetime.datetime.now()
    logger('Waiting for backup ID {} to be built.'.format(backup['id']))
    try:
        backupStatus = Job('DetailBackup', backup_id=backup['id']).run()
    except HTTPError as err:
        if err.code == 404:
            logger('Backup {} appears to have failed.'.format(backup['id']))
            raise OnappException('Backup ID {} failed since it disappeared.'.format(backup['id']), 'stallUntilBackupBuilt')
        else:
            raise
    if backupStatus['built']: return True
    else: time.sleep(5)
    while not backupStatus['built']:
        if datetime.datetime.now() - beginTime > datetime.timedelta(seconds=timeout):
            raise OnappException('Timed out waiting for backup to build', [t[0] for t in vm_ids.keys() if not vm_ids[t]])
        try:
            backupStatus = Job('DetailBackup', backup_id=backup['id']).run()
        except HTTPError as err:
            if err.code == 404:
                logger('Backup {} appears to have failed.'.format(backup['id']))
                raise OnappException('Backup ID {} failed since it disappeared.'.format(backup['id']), 'stallUntilBackupBuilt')
            else:
                raise
        time.sleep(2)
    time.sleep(1)
    logger('Backup ID {} has been built.'.format(backup['id']))
    if VERBOSE:
        print 'Backup ID {} has been built.'.format(backup['id'])

def getAllStorageNodeStats(start, end):
    nodes = aGetStorageNodes();
    statsData = {};
    for node in nodes:
        statsData[node] = {}
        data = {'node':node, 'startTime':start, 'endTime':end, filterType:0}
        statsData[node]['reads'] = aGetStorageNodeStats(data)
        data['filterType'] = 4
        statsData[node]['writes'] = aGetStorageNodeStats(data)
    return statsData;

def batchRunnerJob(data):
    beginTime = datetime.datetime.now();
    params = data.copy()
    del params['func']
    if VERBOSE:
        print "Starting Job", data['func']
    try:
        output = Job(data['func'], params).run();#output = globals()[data['func']](params)
    except HTTPError as err:
        print "HTTP ERROR", err
        raise
    vmdeets = Job('DetailVM', vm_id=data['vm_id']).run()
    vmdisks = Job('ListVMDisks', vm_id=data['vm_id']).run()
    if data['func'] == 'MigrateVM':
        try:
            stallUntilOnline(vmdeets)
        except OnappException as err:
            print "Waiting for virtual machine {} to come online failed, trying to start once more.".format(vmdeets['id'])
            tmp = Job('UnlockVM', vm_id=vmdeets['id']).run()
            tmp = Job('StartVM', vm_id=vmdeets['id']).run()
            try:
                stallUntilOnline(vmdeets)
            except OnappException as err:
                print "!!!!! A virtual machine has failed to start thrice! Please Investigate Control Server !!!!!!!"
                raise;
        new_output = {'origin':vmdeets['hypervisor_id'], 'destination':params['destination']}
        return {'output':new_output, 'vm':vmdeets, 'time':datetime.datetime.now() - beginTime}
    if data['func'] == 'EditDisk':
        time.sleep(10)
        try:
            stallUntilOnline(vmdeets)
        except OnappException as err:
            print "Waiting for virtual machine {} to come online failed, trying to start once more.".format(vmdeets['id'])
            tmp = Job('UnlockVM', vm_id=vmdeets['id']).run()
            tmp = Job('StartVM', vm_id=vmdeets['id']).run()
            try:
                stallUntilOnline(vmdeets)
            except OnappException as err:
                print "!!!!! A virtual machine has failed to start thrice! Please Investigate Control Server !!!!!!!"
                raise;
        new_output = {'start_size':vmdisks[0]['disk_size'], \
        'increase': int(data['disk_size']) - int(vmdisks[0]['disk_size'])}
        return {'vm':vmdeets, 'time':datetime.datetime.now() - beginTime, 'output':new_output}
    if data['func'] == 'RestoreBackup':
        try:
            stallUntilOnline(vmdeets)
        except OnappException as err:
            print "Waiting for virtual machine {} to come online failed, trying to start once more.".format(vmdeets['id'])
            tmp = Job('UnlockVM', vm_id=vmdeets['id']).run()
            tmp = Job('StartVM', vm_id=vmdeets['id']).run()
            try:
                stallUntilOnline(vmdeets)
            except OnappException as err:
                print "!!!!! A virtual machine has failed to start thrice! Please Investigate Control Server !!!!!!!"
                raise;
        new_output = {'backup_id':data['backup_id']}
        return {'vm':vmdeets, 'time':datetime.datetime.now() - beginTime, 'output':new_output}
    if data['func'] == 'CreateIncrementalBackup':
        print output
        stallUntilBackupBuilt(output)
        return {'vm':vmdeets, 'time':datetime.datetime.now() - beginTime}

def generateJobsBatch(tvms, count, defData={}):
    vms = tvms[:]
    shuffle(vms);
    # Job name, weight.
    jobsList = [ \
        ('MigrateVM',1), \
        ('EditDisk',2), \
        ('CreateBackup',1), \
        ('RestoreBackup',2), \
        ('stopStartVM',1) \
    ]
    if using_vm_network: jobsList.append(('SingleWorkload',2))
    # ('CreateVM',3), ('DestroyVM',2)];
    jobs = [];
    weight=0;
    if VERBOSE: print 'Generating new batch.'
    while weight < count and len(vms) > 0:
        j = choice(jobsList);
        if VERBOSE: print 'Job: {}'.format(j[0])
        while j[1] > int(count - weight):
            j = choice(jobsList);
            if VERBOSE: print 'Reselecting: {}'.format(j[0])
        weight += j[1]
        if VERBOSE: print 'Weight: {}'.format(weight)
        curvm = choice(vms)
        if VERBOSE: print 'VM: {}'.format(curvm['id'])
        if j[0] == 'MigrateVM':
            lowest_hv_id = dsql("SELECT hv.id FROM hypervisors hv \
            JOIN virtual_machines vm ON vm.hypervisor_id = hv.id \
            WHERE vm.deleted_at IS NULL \
            AND hv.id <> {} \
            AND hv.hypervisor_group_id = {} \
            GROUP BY hv.id \
            ORDER BY count(vm.id) ASC \
            LIMIT 1".format(dsql("SELECT hypervisor_id FROM virtual_machines WHERE id={}".format(curvm['id'])), HVZONE))
            if not lowest_hv_id:
                hv_ids = [ i[0] for i in dsql("SELECT id FROM hypervisors WHERE hypervisor_group_id={}".format(HVZONE)) ]
                try:
                    hv_ids.remove(curvm['hypervisor_id'])
                except ValueError:
                    print "Somehow VMs hyperivsor ID is not in the zone. picking randomly......expect a 422"
                try:
                    lowest_hv_id = choice(hv_ids)
                except IndexError:
                    print "There are no IDs in the hypervisor zone that I'm testing?"
                    raise IndexError("generateJobsBatch->MigrateVM->hv_ids is empty.")
            jobs.append(Job('batchRunnerJob' , func='MigrateVM', destination=lowest_hv_id, vm_id=str(curvm['id'])))
        if j[0] == 'EditDisk':
            dInfo = dpsql("SELECT id, disk_size FROM disks WHERE virtual_machine_id={} AND disks.primary=1".format(curvm['id']))
            resizeTarget = dInfo['disk_size'] + int(defData['resizetarget'])
            if resizeTarget > defData['maxdisksize']:
                weight -= j[1]
                continue;
            jobs.append(Job('batchRunnerJob', func='EditDisk', disk_id=str(dInfo['id']), disk_size=resizeTarget, vm_id=curvm['id']))
        if j[0] == 'RestoreBackup':
            # bkpids = filter(lambda a: a != 'NONE', runJobs( [ [ 'listbkp' , { 'id' : vm['id'] } ] for vm in vms ] ))
            bkpids = Job('ListVMBackups', vm_id=str(curvm['id'])).run();
            if len(bkpids) == 0:
                j = ('CreateBackup',1);
                weight -= 1;
            else:
                curbkp = choice(bkpids)
                jobs.append(Job('batchRunnerJob', func='RestoreBackup', backup_id=str(curbkp['id']),vm_id=curvm['id']))
        if j[0] == 'CreateBackup':
            jobs.append(Job('batchRunnerJob', func='CreateIncrementalBackup', vm_id=str(curvm['id'])))
        if j[0] == 'SingleWorkload':
            cjob = Job('SingleWorkload', id=str(curvm['id']), duration=defData['wlduration'] if 'wlduration' in defData.keys() else 10 )
            if 'ddparams' in defData.keys():
                d=defData['ddparams']
                cjob.addData(interval=d['interval'], ratio=d['writes'], dd_bs=d['dd_bs'], dd_count=d['dd_count'], virtual_machine=curvm)
            else:
                cjob.addData(interval=DEFAULTS['interval'], ratio=DEFAULTS['writes'], \
                    dd_bs=DEFAULTS['dd_bs'], dd_count=DEFAULTS['dd_count'], virtual_machine=curvm)
            jobs.append( cjob )
        if j[0] == 'stopStartVM':
            jobs.append(Job('stopStartVM',curvm))
        # if j[0] == 'CreateVM':
        #
        # if j[0] == 'DestroyVM':
        vms.remove(curvm)
    return jobs;

def stopStartVM(vm):
    beginTime = datetime.datetime.now()
    rData = {}
    tmp = Job('StopVM', vm_id=vm['id']).run()
    stallUntilOffline(vm)
    rData['stoptime'] = datetime.datetime.now() - beginTime
    time.sleep(10)
    beginTime = datetime.datetime.now()
    tmp = Job('StartVM', vm_id=vm['id']).run()
    try:
        stallUntilOnline(vm)
    except OnappException as err:
        print "Waiting for virtual machine to come online failed, trying to start once more."
        tmp = Job('UnlockVM', vm_id=vm['id']).run()
        tmp = Job('StartVM', vm_id=vm['id']).run()
        try:
            stallUntilOnline(vm)
        except OnappException as err:
            print "!!!!! A virtual machine has failed to start! Please Investigate Control Server !!!!!!!"
            raise;
    rData['starttime'] = datetime.datetime.now() - beginTime
    rData['vm'] = Job('DetailVM', vm_id=vm['id']).run()
    return rData;

def getHVZone():
    if VERBOSE: print('Pulling HV Zone list:');
    hvZones = dListHVZones();
    if len(hvZones) == 1:
        print(' Found one zone with ID {}, Label {}'.format(hvZones[0]['id'], hvZones[0]['label']))
        return hvZones[0]['id']
    for z in hvZones:
        print(' Zone {}: {}'.format(z['id'], z['label']));
    zid = int(raw_input('Provide zone ID: '));
    while zid not in [z['id'] for z in hvZones]:
        zid = int(raw_input('Do not see that zone, provide valid zone: '))
    return zid;

def getHVs():
    if VERBOSE: print('Pulling hypervisor list:');
    hvList = { hv['id'] : hv for hv in [ h['hypervisor'] for h in Job('ListHVs').run()]}; # tListHV outputs a series of dict lists id, mac, ip, something, type, label
    if len(hvList) == 1:
        print('Only one hypervisor: {} @ {}'.format(i['label'], ['ip_address']));
        print('Selecting this hypervisor automatically');
        return hvList[0];
    for i in hvList:
        print('  {}. {} @ {}'.format(i['id'], i['label'], i['ip_address']));
    t = raw_input('List of hypervisors to test, space separated?: ').split();
    rList = [ hvList[int(i)] for i in hvsToTest]
    return rList;

def getLegacyTemplate():
    if VERBOSE: print('Pulling templates list:');
    templateList = tAvailTemplates('linux'); # tAvailTemplates outputs a list of template names.
    if len(templateList) == 1:
        print('Only one template found: {}'.format(templateList[0]));
        print('Selecting this template automatically.');
        return templateList[0]
    for i, j in enumerate(templateList):
        print('{:>3}. {}'.format(i, j));
    tm = raw_input('Provide template number to use: ');
    while not tm.isdigit(): tm = raw_input('Not a number. Please provide number.');
    t = templateList[int(tm)];
    return t;

def getTemplate():
    if VERBOSE: print('Pulling templates list:');
    templateStoreData = apiCall('/template_store.json')
    avail_templates = {}
    for store in templateStoreData:
        for template in store['relations']:
            if template['image_template']['cdn']: continue;
            if template['image_template']['operating_system_distro'] == 'lbva': continue;
            if template['image_template']['state'] == 'active':
                avail_templates[template['template_id']] = template['image_template']['manager_id']
    if len(avail_templates) == 1:
        k = avail_templates.keys()
        print('Only one template found: {}'.format(avail_templates[k[0]]))
        print('Selecting this template automatically.');
        return k[0];
    for tid in avail_templates.keys():
        print('{:>3}. {}'.format(tid, avail_templates[tid]))
    tm = raw_input('Provide template ID to use: ');
    while not tm.isdigit() and tm not in avail_templates.keys(): tm = raw_input('Not an ID. Provide ID: ');
    t = avail_templates[int(tm)]
    return int(tm);

def getDatastore(hv_zone):
    ds_ids = dsql("SELECT data_store_group_id as id FROM data_stores WHERE id in (SELECT data_store_id \
    FROM data_store_joins WHERE target_join_type='HypervisorGroup' AND target_join_id={})".format(hv_zone))
    if not ds_ids:
        raise OnappException(None, 'getDatastore', 'Could not find any datastores attached to hypervisor zone ID {}'.format(hv_zone))
    if type(ds_ids) == long:
        print "Found one datastore zone attached to this zone."
        return ds_ids;
    else:
        ds_ids = [d[0] for d in ds_ids]
        print "Multiple datastore zones for this hypervisor zone."
        labels = dpsql("SELECT id, label FROM packs WHERE type='DataStoreGroup' AND id IN ({})".format(','.join([d['id'] for d in ds_ids])))
        for ds in labels:
            print ' {}. {}'.format(labels['id'], labels['label'])
        chosen_id = raw_input("Provide ID of data store zone to use: ")
        while chosen_id not in ds_ids:
            chosen_id = raw_input("Invalid. Please provide ID of data store zone to use: ")
        return chosen_id

def createWorkerVMs(count, hvs, templ, datast, jd): # should be named createWorkerVeez
    global testVMs
    jobs = [];
    for i in xrange(int(count)):
        for hv in hvs:
            jd_tmp = {'hypervisor_id':hv['id'], 'template_id':templ, \
            'data_store_group_primary_id':datast, 'data_store_group_swap_id':datast, \
            'hostname':'burninTesting{}on{}'.format(i,re.sub(r'[^a-zA-Z0-9]', '', hv['label'])), \
            'label':'burnin{}at{}'.format(i,hv['label']), \
            }
            tmp = jd_tmp.copy()
            jd_tmp.update(jd);
            if not quiet: print jd_tmp
            # jobs.append({'action':'createvm', 'data':jd_tmp})
            jobs.append(Job('CreateVM', jd_tmp))
    print('Creating worker VMs.')
    logger('Starting worker {} VMs with {} template on the {} datastore zone on hvs: {}'.format(count,templ,datast,str(hvs)))
    vms = runAll(jobs)
    if False in vms:
        print "!!!! Virtual machines failed to create or got invalid API response, please investigate."
        raise OnappException(jd_tmp, 'createWorkerVMs', "Invalid API Response for CreateVMs")
    print 'Sleeping for a 3 minutes before checking VM status...allowing 20 minutes for VM creation after'
    time.sleep(180)
    stallUntilOnline(vms, timeout=1200)
    print('Storing VMs created in testVMs.')
    testVMs = vms;
    print str(testVMs)
    # checkJobOutput(jobs, vms);
    return vms

def watchVMWorkload(data):
    checkKeys(data, ['virtual_machine', 'ratio', 'interval', 'dd_bs', 'dd_count', 'duration'])
    beginTime = datetime.datetime.now()
    proc = {}
    job = Job('CheckVMWorkload', data['virtual_machine'])
    if type(data['duration']) is datetime.timedelta:
        duration = data['duration'];
    else:
        duration = datetime.timedelta(seconds=duration)
    while datetime.datetime.now() - beginTime < duration:
        if job.run() is not True:
            elapsed = datetime.datetime.now() - beginTime
            if CONTINUE_MODE:
                print "VM {} no longer running after {} seconds, attempting to restart workload.".format(data['virtual_machine']['id'], elapsed.seconds)
                jobOutput = Job('StartVMWorkload', data).run()
                if not jobOutput:
                    print "Failed to restart workload on VM {}".format(data['virtual_machine']['id'])
                    return False;
            else:
                print "VM {} no longer running after {} seconds, stopping monitoring.".format(data['virtual_machine']['id'], elapsed.seconds)
                return False
        time.sleep(3)
    print "Workload duration completed."
    return True;


def gatherIOPSData(vms, period=0):
    tvms = vms[:]
    if period != 0:
        for n, vm in enumerate(tvms):
            tvms[n]['period']=period;
    logger("Generating hourly stats before pulling IOPS data.")
    if generateHourlyStats():
        logger("Stats are generated.")
    else:
        print "!!!!! Hourly Stats failed to generate. The last section may be blank. !!!!!"
    jobs = [ Job('GetVMIOPS', vm) for vm in tvms ]
    logger("Starting to pull VM IOPS")
    retData = runParallel(jobs)
    jobData = { t[0] : t[1] for t in retData };
    logger("Writing IOPS data to file {}".format(DATA_FILE))
    f = open(DATA_FILE, "a")
    f.write(str(jobData));
    f.write('\n');
    f.flush();
    f.close();
    return jobData;

def recoverBatchData():
    if not os.path.isfile(BATCHES_OUTPUT_FILE):
        return [];
    if os.stat(BATCHES_OUTPUT_FILE).st_size < 8:
        return [];
    bFile = open(BATCHES_OUTPUT_FILE, 'r')
    try:
        bContents = [ ast.literal_eval(l) for l in bFile.readlines() ]
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        print 'Unable to recover previous batch data.'
        return [];
    print 'Recovered previous batch data, {} batches total recovered.'.format(len(bContents))
    return bContents;

def runBatchesTest(batchSize, restartParams=False):
    global testVMs
    print('Running in batch mode.');
    logger('Batch mode enabled.');
    hvs = Job('ListHVsInZone', hv_zone_id=HVZONE).run()
    hvs = [ hv for hv in hvs if hv['online'] ]
    if restartParams:
        if restartParams['defData']['vm_params'] != 0:
            t = restartParams['defData']['vm_params']['template_id']
            ds = restartParams['defData']['datastore_zone_id']
        else:
            t = 0
            ds = 0
    else:
        t = getTemplate();
        ds = getDatastore(HVZONE);
    defData = {'resizetarget':DEFAULTS['resizetarget'], \
        'maxdisksize': DEFAULTS['maxdisksize'], \
        'wlduration': DEFAULTS['wlduration']};
    ##### Gather information
    ##### Generate a batch, process entire thing.
    ##### Gather data about each job
    duration=datetime.timedelta(minutes=int(DURATION_MINUTES))
    if restartParams:
        defData = restartParams['defData'];
        print "Continuing from previous configuration file."
        # duration = int(raw_input("How many minutes longer should this test be ran? (Default 30): ") or 30)
        # duration = datetime.timedelta(minutes=int(duration));
        if defaults: delay = DEFAULTS['delay']
        else: delay = int(raw_input('Seconds to delay between batches? (Default: 45): ') or DEFAULTS['delay']);
    elif defaults:
        logger('Using default values for everything.');
        delay = DEFAULTS['delay'];
        # duration = datetime.timedelta(minutes=720);
        nvms = DEFAULTS['vms_per_hypervisor'];
    else:
        if not use_existing_virtual_machines:
            nvms = raw_input('How many virtual machines per hypervisor? (Default 10): ') or DEFAULTS['vms_per_hypervisor'];
        delay = raw_input('Seconds to delay between batches? (Default 45): ') or DEFAULTS['delay'];
        # duration = raw_input('Duration of test in minutes? (Default 60): ') or 60;
        # duration = datetime.timedelta(minutes=int(duration));
        if autoyes: noyes = 'y';
        else:
            noyes = raw_input('Change default values for disk resizing and workload duration? (y/n): ');
            while noyes not in ['n', 'N', 'y', 'Y']:
                noyes = raw_input("Invalid. (y/n): ")
        if noyes in [ 'y' , 'Y' ]:
            defData['resizetarget'] = raw_input('How much to increase size for disks in GB(Default 2): ') or DEFAULTS['resizetarget'];
            defData['maxdisksize'] = raw_input('Maximum disk size in GB(default 20): ') or DEFAULTS['maxdisksize'];
            defData['wlduration'] = raw_input('Workload duration in minutes(Default 3): ') or DEFAULTS['wlduration'];
        if defaults:
            noyes = 'n'
        else:
            noyes = raw_input('Customize dd parameters? (y/[n]): ') or 'n';
            while noyes not in ['n', 'N', 'y', 'Y']:
                noyes = raw_input('Invalid. (y/n): ')
        workParams = {'interval':DEFAULTS['interval'], 'writes':DEFAULTS['writes'], \
            'dd_bs':DEFAULTS['dd_bs'], 'dd_count':DEFAULTS['dd_count']};
        if noyes in ['y', 'Y']:
            workParams = {};
            workParams['interval'] = raw_input('Interval(Seconds to sleep between workloads): '
                if not quiet else 'Interval :') or DEFAULTS['interval'];
            workParams['writes'] = raw_input('Write %(Percentage of time writing, else reading): '
                if not quiet else 'Write%: ') or DEFAULTS['writes'];
            workParams['dd_bs'] = raw_input('Block size(Block read/write size, dd\'s bs param): '
                if not quiet else 'BlockSize: ') or DEFAULTS['dd_bs'];
            workParams['dd_count'] = raw_input('Count(Block count for dd): '
                if not quiet else 'Count: ') or DEFAULTS['dd_count'];
            logger('Custom dd parameters: {}'.format(workParams));
        defData['ddparams']=workParams;
        logger('Custom data: {}'.format(str(defData)));
    DeployWorkloadFile()
    if use_existing_virtual_machines:
        if restartParams: print "Using VMs on cloud rather than config file."
        else: print "Skipping VM creation..."
        testVMs = vms = ListVMs()
        #testVMs = vms
        defData['vm_params'] = 0
    elif restartParams:
        print "Using VMs from config file."
        testVMs = vms = restartParams['testVMs']
    else:
        ask = raw_input('Customize VM resources (y/n)? ')
        while ask not in ['n','N','y','Y']:
            ask = raw_input('Invalid. (y/n)? ')
        templ_data = dpsql("SELECT * FROM templates WHERE id={}".format(t))
        if ask in ['y', 'Y']:
            jd = {'required_virtual_machine_build': 1, \
                  'required_ip_address_assignment': 1 }
            jd['memory'] = raw_input('Memory(MB): ') or templ_data['min_memory_size']
            jd['cpus'] = raw_input('CPU Cores: ') or 1
            jd['cpu_shares'] = raw_input('CPU Shares: ') or 1
            jd['primary_disk_size'] = raw_input('Primary disk size: ') or templ_data['min_disk_size']
        else:

            jd = { 'memory' : templ_data['min_memory_size'], \
                   'cpus'   : 1, \
                   'cpu_shares': 1, \
                   'primary_disk_size': templ_data['min_disk_size'], \
                   'required_virtual_machine_build': 1, \
                   'required_ip_address_assignment': 'true' }
        defData['vm_params'] = jd
        defData['vm_params']['template_id'] = t
        defData['datastore_zone_id'] = ds
        vms = createWorkerVMs(nvms, hvs, t, ds, jd);
    for vm in vms:
        stat = Job('VMStatus', vm_id=vm['id']).run()
        if stat['booted'] == False:
            Job('StartVM', vm_id=vm['id']).run()
    try:
        stallUntilOnline(vms)
    except OnappException as err:
        print "Waiting for virtual machines {} to come online failed, trying to start once more.".format([vm['id'] for vm in vms])
        tmpJobs = [ Job('UnlockVM', vm_id=vm['id']) for vm in vms ]
        runParallel(tmpJobs);
        tmpJobs = [ Job('StartVM', vm_id=vms['id']) for vm in vms ]
        runParallel(tmpJobs);
        try:
            stallUntilOnline(vms)
        except OnappException as err:
            print "!!!!! A virtual machine has failed to start thrice! Please Investigate Control Server !!!!!!!"
            raise;
    if using_vm_network: runParallel([Job('CopyWorkloadFile', vm) for vm in vms])
    if run_pre_burnin_test: BatchWorkload(vms, defData['ddparams'], run_pre_burnin_test)
    beginTime = datetime.datetime.now();
    batchNum = 0;
    allData = []
    if restartParams:
        allData = recoverBatchData();
    output_file = open(BATCHES_OUTPUT_FILE, 'a')
    if batchSize is 0:
        batchSize = int(float(len(vms))*1.25)
    if not restartParams: writeConfigFile(CONFIG_FILE, defData)
    elif not not restartParams['latest']: batchNum = restartParams['latest'][0]
    else: batchNum = 0;
    DS_IDENTIFIER = False;
    if ds != 0: DS_IDENTIFER = dsql('SELECT identifier FROM data_stores WHERE id={}'.format(ds))
    while datetime.datetime.now() - beginTime < duration:
        batchNum+=1;
        if VERBOSE: print 'Ensuring VM\'s are online.'
        vm_statuses = runParallel( [ Job('VMStatus', vm_id=vm['id']) for vm in vms ] )
        for stat in vm_statuses:
            if stat['booted'] == False:
                logger('Starting VM ID {}'.format(stat['id']))
                Job('StartVM', vm_id=stat['id']).run();
        if DS_IDENTIFIER:
            if VERBOSE: print "Ensuring there are no degraded disks."
            for hv in hvs:
                degraded_disks = stapi(hv['ip_address'], '/is/Datastore/{}'.format(DS_IDENTIFIER), json.dumps({'state':2}), method='PUT')['degraded_vdisks']
                if degraded_disks != '':
                    print "Degraded disks have been found. Refusing to continue. Please investigate disks: {}".format(degraded_disks)
                    print "Once disk issues have been resolved, restart script."
                    print "If issues cannot be resolved and VMs must be deleted, rerun script with the --clean flag."
                    sys.exit()
        # for vm in vms:
        #     stat = Job('VMStatus', vm_id=vm['id']).run()
        #     if stat['booted'] == False:
        #         logger('Starting VM ID {0}'.format(vm['id']))
        #         Job('StartVM', vm_id=vm['id']).run()
        try:
            stallUntilOnline(vms)
        except OnappException as err:
            print "Waiting for virtual machines {} to come online failed, trying to start once more.".format([vm['id'] for vm in vms])
            tmpJobs = [ Job('UnlockVM', vm_id=vm['id']) for vm in vms ]
            runParallel(tmpJobs);
            tmpJobs = [ Job('StartVM', vm_id=vms['id']) for vm in vms ]
            runParallel(tmpJobs);
            try:
                stallUntilOnline(vms)
            except OnappException as err:
                print "!!!!! A virtual machine has failed to start thrice! Please Investigate Control Server !!!!!!!"
                raise;
        jobs = generateJobsBatch(vms, batchSize, defData);
        print "Batch ID {}, # of Jobs: {}".format(batchNum, len(jobs))
        if VERBOSE:
            for j in jobs: print j
            print '--------------------------------------------'
        jobData = runParallel(jobs);
        output_file.write(str((batchNum,datetime.datetime.now(),[tmpjobs.getAction() for tmpjobs in jobs],jobData)))
        output_file.write('\n')
        output_file.flush()
        allData.append((batchNum,datetime.datetime.now(),[tmpjobs.getAction() for tmpjobs in jobs],jobData));
        logger('Batch {} completed, data: {}'.format(batchNum, jobData))
        if VERBOSE:
            print('Batch {} completed, data: {}'.format(batchNum, jobData))
        else: print('Batch {} completed.'.format(batchNum));
        ########### TODO check for degraded disks ##############
        # diskstatus = stapi(HV_IP, '/is/Datastore/{}'.format(ID_DS_IDENTIFIER), json.dumps({'state':2}), method='PUT' )
        # checkJobOutput(jobs, jobData);
        time.sleep(float(delay));
    output_file.close();
    # print('\n\nDisplaying all data from batches:\n');
    # print allData
    # print('\n\nData also in ./batches.output.\n');
    #processIOPSOutput(gatherIOPSData(testVMs))
    unlockJobs = False
    destroyJobs = False
    if not use_existing_virtual_machines:
        print('Time completed. Cleaning up virtual machines.');
        unlockJobs = [ Job('UnlockVM', vm_id=vm['id']) for vm in testVMs ]
        destroyJobs= [ Job('DeleteVM', vm_id=vm['id'], destroy_all_backups=1) for vm in testVMs ]
    else:
        print('Not destroying VMs since they were not created by this run.')
    return unlockJobs, destroyJobs, allData, defData

def cpuCheck(target=False):
    cpu_model_cmd="grep model\ name /proc/cpuinfo -m1 | cut -d':' -f2 | sed -r -e 's/^ //;s/ $//'"
    cpu_speed_cmd="grep cpu\ MHz /proc/cpuinfo -m1 | cut -d':' -f2 | cut -d'.' -f1 | tr -d ' '"
    cpu_cores_cmd="grep -c ^processor /proc/cpuinfo"
    if target is False:
        rData = { \
        'model':runCmd("grep model\ name /proc/cpuinfo -m1").split(':')[1].strip(), \
        'speed':runCmd("grep cpu\ MHz /proc/cpuinfo -m1").split(':')[1].strip(), \
        'cores':runCmd("grep -c ^processor /proc/cpuinfo") }
        return rData;
    rData = {};
    rData['model'] = runCmd(['su', 'onapp', '-c', 'ssh {} -p{} root@{} "{}"'.format(SSH_OPTIONS, ONAPP_CONFIG['ssh_port'], target, cpu_model_cmd)])
    rData['speed'] = runCmd(['su', 'onapp', '-c', 'ssh {} -p{} root@{} "{}"'.format(SSH_OPTIONS, ONAPP_CONFIG['ssh_port'], target, cpu_speed_cmd)])
    rData['cores'] = runCmd(['su', 'onapp', '-c', 'ssh {} -p{} root@{} "{}"'.format(SSH_OPTIONS, ONAPP_CONFIG['ssh_port'], target, cpu_cores_cmd)])
    return rData;

def motherboardCheck(target=False):
    base_cmd = "dmidecode -s baseboard-{}"
    if target is False:
        return { \
        'manufacturer' : runCmd(base_cmd.format('manufacturer')) ,
        'product-name' : runCmd(base_cmd.format('product-name')) ,
        'version' : runCmd(base_cmd.format('version')) }
    rData = {};
    rData['manufacturer'] = runCmd(['su', 'onapp', '-c', 'ssh -p{} root@{} "{}"'.format(ONAPP_CONFIG['ssh_port'], target, base_cmd.format('manufacturer'))])
    rData['product-name'] = runCmd(['su', 'onapp', '-c', 'ssh -p{} root@{} "{}"'.format(ONAPP_CONFIG['ssh_port'], target, base_cmd.format('product-name'))])
    rData['version'] = runCmd(['su', 'onapp', '-c', 'ssh -p{} root@{} "{}"'.format(ONAPP_CONFIG['ssh_port'], target, base_cmd.format('version'))])
    return rData;

def chassisCheck(target=False):
    base_cmd = "dmidecode -s chassis-{}"
    if target is False:
        return { \
        'manufacturer' : runCmd(base_cmd.format('manufacturer')) ,
        'type' : runCmd(base_cmd.format('type')) ,
        'version' : runCmd(base_cmd.format('version')) }
    rData = {}
    rData['manufacturer'] = runCmd(['su', 'onapp', '-c', 'ssh -p{} root@{} "{}"'.format(ONAPP_CONFIG['ssh_port'], target, base_cmd.format('manufacturer'))])
    rData['type'] = runCmd(['su', 'onapp', '-c', 'ssh -p{} root@{} "{}"'.format(ONAPP_CONFIG['ssh_port'], target, base_cmd.format('type'))])
    rData['version'] = runCmd(['su', 'onapp', '-c', 'ssh -p{} root@{} "{}"'.format(ONAPP_CONFIG['ssh_port'], target, base_cmd.format('version'))])
    return rData;

def diskHWCheck(target=False):
    #list_disks_cmd = "lsblk -n -d -e 1,7,11 -oNAME"
    list_disks_cmd = "lsblk -dn -oNAME -I8,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135"
    udev_cmd = "bash -c 'eval $(udevadm info --export --query=property --path=/sys/class/block/{}) && echo $ID_VENDOR - $ID_MODEL'"
    disk_data = {}
    if target is False:
        disks = runCmd(list_disks_cmd, shlexy=False, shell=True).split('\n')
        for d in disks:
            disk_data[d] = runCmd(udev_cmd.format(d), shlexy=False, shell=True)
        return disk_data;
    disks = runCmd(['su', 'onapp', '-c', 'ssh -p{} root@{} "{}"'.format(ONAPP_CONFIG['ssh_port'], target, list_disks_cmd)]).split('\n')
    udev_cmd = "bash -c 'eval \$(udevadm info --export --query=property --path=/sys/class/block/{}) && echo \$ID_VENDOR - \$ID_MODEL'"
    for d in disks:
        disk_data[d] = runCmd(['su', 'onapp', '-c', 'ssh -p{} root@{} "{}"'.format(ONAPP_CONFIG['ssh_port'], target, udev_cmd.format(d))])
    return disk_data;

def interfaceCheck(target=False):
    iface_cmd = "find /sys/class/net -type l -not -lname '*virtual*' -printf '/sys/class/net/%f\n'"
    udev_cmd = "bash -c 'eval $(udevadm info --export --query=property --path=`readlink -f {}`) && echo $ID_MODEL_FROM_DATABASE'"
    iface_data = {}
    if target is False:
        iface_list = runCmd(iface_cmd).split('\n')
        for iface in iface_list:
            iface_data[iface.split('/')[-1]] = runCmd(udev_cmd.format(iface))
        return iface_data;
    iface_list = runCmd(['su', 'onapp', '-c', 'ssh -p{} root@{} "{}"'.format(ONAPP_CONFIG['ssh_port'], target, iface_cmd)]).split('\n')
    udev_cmd = "bash -c 'eval \$(udevadm info --export --query=property --path=`readlink -f {}`) && echo \$ID_MODEL_FROM_DATABASE'"
    for iface in iface_list:
        iface_data[iface.split('/')[-1]] = runCmd(['su', 'onapp', '-c', 'ssh -p{} root@{} "{}"'.format(ONAPP_CONFIG['ssh_port'], target, udev_cmd.format(iface))])
    return iface_data;

def generateHourlyStats():
    cmd = ['su', 'onapp', '-c', 'cd /onapp/interface && RAILS_ENV=production rake vm:generate_hourly_stats']
    stdout, stderr = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate();
    stderr = [ x for x in stderr.rstrip('\n').split('\n') if 'WARNING: OnApp configuration key' not in x ]
    if stderr:
        logger("Generating hourly stats failed, stderr: {}".format('\n'.join(stderr).strip()))
        return False
    return True;

# gotta change this so the
    # jobsList = [('MigrateVM',1), ('EditDisk',2), ('CreateBackup',1), \
    # ('RestoreBackup',2), ('SingleWorkload',2), ('stopStartVM',1)]
def newProcessOutput(content):
    batches = {}
    for c in content:
        batchNum = c[0];
        batches[batchNum] = {}
        batches[batchNum]['time_started'] = str(c[1])
        batches[batchNum]['tests'] = {}
        for ii in xrange(len(c[2])):
            curAction = c[2][ii]
            curData = c[3][ii]
            if curData is False: continue;
            if curAction == 'SingleWorkload':
                batches[batchNum]['tests'][ii] = { \
                        'virtual_machine_id':curData['vm_id'], \
                        'workdata':curData['data']}
            if curAction == 'stopStartVM':
                batches[batchNum]['tests'][ii] = {'time':{ \
                        'stop': float('{}.{}'.format(curData['stoptime'].seconds,  curData['stoptime'].microseconds)),    \
                        'start':float('{}.{}'.format(curData['starttime'].seconds, curData['starttime'].microseconds)) }, \
                        'virtual_machine_id':curData['vm']['id'] }
            if curAction == 'MigrateVM':
                batches[batchNum]['tests'][ii] = { \
                        'time':float('{}.{}'.format(curData['time'].seconds, curData['time'].microseconds)), \
                        'virtual_machine_id':curData['vm']['id'], \
                        'origin':curData['output']['origin'], 'destination':curData['output']['destination'] }
            if curAction == 'EditDisk':
                batches[batchNum]['tests'][ii] = { \
                        'time':float('{}.{}'.format(curData['time'].seconds, curData['time'].microseconds)), \
                        'virtual_machine_id':curData['vm']['id'], \
                        'start_size':curData['output']['start_size'], 'increase': curData['output']['increase']}
            if curAction == 'CreateIncrementalBackup':
                batches[batchNum]['tests'][ii] = { \
                        'time':float('{}.{}'.format(curData['time'].seconds, curData['time'].microseconds)), \
                        'virtual_machine_id':curData['vm']['id'] }
            if curAction == 'RestoreBackup':
                batches[batchNum]['tests'][ii] = { \
                        'time':float('{}.{}'.format(curData['time'].seconds, curData['time'].microseconds)), \
                        'virtual_machine_id':curData['vm']['id'], 'backup_id':curData['output']['backup_id'] }
            batches[batchNum]['tests'][ii]['name'] = curAction;
    return batches


def newProcessIOPSData(content):
    iops_by_time_by_vm = {}
    iops_by_time = {}
    overall_totals = { 'dataread':0.0, 'datawrite':0.0, 'reads':0.0, 'writes':0.0 }
    per_vm_totals = {}
    per_vm_averages = {}
    for vm in content.keys():
        iops_by_time_by_vm[vm] = {}
        per_vm_totals[vm] = { 'dataread':0.0, 'datawrite':0.0, 'reads':0.0, 'writes':0.0 }
        per_vm_averages[vm] = {}
        for disk in content[vm].keys():
            iops_by_time_by_vm[vm][disk] = {}
            numStats = len(content[vm][disk]['stattimes'])
            for ii in xrange(len(content[vm][disk]['stattimes'])):
                c = content[vm][disk]['stattimes'][ii]
                cont = content[vm][disk]
                per_vm_totals[vm]['dataread']  += cont['dataread'] [ii]
                per_vm_totals[vm]['datawrite'] += cont['datawrite'][ii]
                per_vm_totals[vm]['reads']     += cont['reads']    [ii]
                per_vm_totals[vm]['writes']    += cont['writes']   [ii]
                overall_totals['dataread']  += cont['dataread'] [ii]
                overall_totals['datawrite'] += cont['datawrite'][ii]
                overall_totals['reads']     += cont['reads']    [ii]
                overall_totals['writes']    += cont['writes']   [ii]
                iops_by_time_by_vm[vm][disk][c] = { \
                    'dataread' : cont['dataread'] [ii], \
                    'datawrite': cont['datawrite'][ii], \
                    'reads'    : cont['reads']    [ii], \
                    'writes'   : cont['writes']   [ii] }
        if numStats == 0:
            per_vm_averages[vm] = {'dataread':0, 'datawrite':0,'reads':0,'writes':0}
        else:
            per_vm_averages[vm]['dataread']  = per_vm_totals[vm]['dataread']  / numStats
            per_vm_averages[vm]['datawrite'] = per_vm_totals[vm]['datawrite'] / numStats
            per_vm_averages[vm]['reads']     = per_vm_totals[vm]['reads']     / numStats
            per_vm_averages[vm]['writes']    = per_vm_totals[vm]['writes']    / numStats
    if len(per_vm_averages) == 0: return False;
    all_averages = { 'full': {   'dataread'  : avg([ per_vm_averages[vm]['dataread']  for vm in content.keys() ]) , \
                                 'datawrite' : avg([ per_vm_averages[vm]['datawrite'] for vm in content.keys() ]) , \
                                 'reads'     : avg([ per_vm_averages[vm]['reads']     for vm in content.keys() ]) , \
                                 'writes'    : avg([ per_vm_averages[vm]['writes']    for vm in content.keys() ])}, \
                     'vms': per_vm_averages }
    return {'by_time':iops_by_time_by_vm, 'averages':all_averages}

def gatherConfigData(zone):
    health_data = {}
    if not quiet: print "Gathering cloud configuration data."
    if not quiet: print "Gathering control server data."
    health_data['cp_data'] = { \
        'version' : runCmd("rpm -qa onapp-cp") , \
        'kernel': runCmd("uname -r") , \
        'distro' : runCmd("cat /etc/redhat-release") , \
        'timezone' : runCmd("readlink /etc/localtime").lstrip('../usr/share/zoneinfo/') , \
        'ip_address' : runCmd("ip route get 1 | awk '{print $NF;exit}'", shell=True, shlexy=False) , \
        'motherboard' : motherboardCheck(), \
        'chassis' : chassisCheck(), \
        'disks' : diskHWCheck(), \
        'network_interfaces' : interfaceCheck(), \
        'cpu' : cpuCheck()}
    health_data['zone_data'] = { 'hypervisors': {}, 'backup_servers': {} }
    hv_data = dpsql("SELECT id, label, ip_address, hypervisor_type FROM hypervisors WHERE hypervisor_group_id={} AND online=1".format(zone), unlist=False)
    for hv in hv_data:
        if not quiet: print "Gathering data for hypervisor {} @ {}".format(hv['label'], hv['ip_address'])
        rData = {}
        hv_ver_bash_cmd = "ssh -p{} {} root@{} \"cat /onapp/onapp-store-install.version 2>/dev/null || cat /onapp/onapp-hv-tools.version 2>/dev/null || grep Version /onappstore/package-version.txt 2>/dev/null || echo '???'\""
        hv_ver_cmd = [ 'su', 'onapp', '-c', hv_ver_bash_cmd.format(ONAPP_CONFIG['ssh_port'], SSH_OPTIONS, hv['ip_address']) ]
        hv_kernel_cmd = [ 'su', 'onapp', '-c', 'ssh {} -p{} root@{} "uname -r 2>/dev/null" 2>/dev/null'.format(SSH_OPTIONS, ONAPP_CONFIG['ssh_port'], hv['ip_address']) ]
        hv_distro_cmd = [ 'su', 'onapp', '-c', 'ssh {} -p{} root@{} "cat /etc/redhat-release 2>/dev/null" 2>/dev/null'.format(SSH_OPTIONS, ONAPP_CONFIG['ssh_port'], hv['ip_address']) ]
        rData['version'] = runCmd(hv_ver_cmd);
        rData['kernel'] = runCmd(hv_kernel_cmd);
        rData['distro'] = runCmd(hv_distro_cmd);
        rData['memory'] = runCmd(['su','onapp','-c','ssh {} -p{} root@{} "free -m"'.format(SSH_OPTIONS, ONAPP_CONFIG['ssh_port'], hv['ip_address'])]).split('\n')[1].split()[1]
        rData['ip_address'] = hv['ip_address']
        rData['type'] = hv['hypervisor_type']
        rData['cpu'] = cpuCheck(hv['ip_address'])
        rData['motherboard'] = motherboardCheck(hv['ip_address'])
        rData['chassis'] = chassisCheck(hv['ip_address'])
        rData['disks'] = diskHWCheck(hv['ip_address'])
        rData['network_interfaces'] = interfaceCheck(hv['ip_address'])
        rData['label'] = hv['label']
        health_data['zone_data']['hypervisors'][hv['id']] = rData
    bs_data = dpsql("SELECT id, label, ip_address FROM backup_servers WHERE enabled=1 AND id IN (SELECT id FROM backup_server_joins WHERE target_join_type='HypervisorGroup' AND target_join_id={})".format(zone), unlist=False)
    if not bs_data: return health_data;
    for bs in bs_data:
        if not quiet: print "Gathering data for backup server {} @ {}".format(bs['label'], bs['ip_address'])
        rData = {}
        hv_ver_bash_cmd = "ssh -p{} root@{} \"cat /onapp/onapp-store-install.version 2>/dev/null || cat /onapp/onapp-hv-tools.version 2>/dev/null || grep Version /onappstore/package-version.txt 2>/dev/null || echo '???'\""
        hv_ver_cmd = [ 'su', 'onapp', '-c', hv_ver_bash_cmd.format(ONAPP_CONFIG['ssh_port'], hv['ip_address']) ]
        hv_kernel_cmd = [ 'su', 'onapp', '-c', 'ssh {} -p{} root@{} "uname -r 2>/dev/null" 2>/dev/null'.format(SSH_OPTIONS, ONAPP_CONFIG['ssh_port'], hv['ip_address']) ]
        hv_distro_cmd = [ 'su', 'onapp', '-c', 'ssh {} -p{} root@{} "cat /etc/redhat-release 2>/dev/null" 2>/dev/null'.format(SSH_OPTIONS, ONAPP_CONFIG['ssh_port'], hv['ip_address']) ]
        rData['version'] = runCmd(hv_ver_cmd);
        rData['kernel'] = runCmd(hv_kernel_cmd);
        rData['distro'] = runCmd(hv_distro_cmd);
        rData['memory'] = runCmd(['su','onapp','-c','ssh {} -p{} root@{} "free -m"'.format(SSH_OPTIONS, ONAPP_CONFIG['ssh_port'], bs['ip_address'])]).split('\n')[1].split()[1]
        rData['ip_address'] = bs['ip_address']
        rData['cpu'] = cpuCheck(bs['ip_address'])
        rData['motherboard'] = motherboardCheck(hv['ip_address'])
        rData['chassis'] = chassisCheck(hv['ip_address'])
        rData['disks'] = diskHWCheck(hv['ip_address'])
        rData['network_interfaces'] = interfaceCheck(hv['ip_address'])
        rData['label'] = bs['label']
        health_data['zone_data']['backup_servers'][bs['id']] = rData
    return health_data;

# def processIOPSDataByTime(content):
#     all_data = {}
#     vms = content.keys()
#     for vm in vms:
#         disks = content[vm].keys();
#         for disk in disks:
#             for n, hour in enumerate(content[vm][disk]['stattimes']):
#                 if hour not in all_data.keys():
#                     all_data[hour] = {}
#                 if vm not in all_data[hour].keys():
#                     all_data[hour][vm] = {}
#                 if disk not in all_data[hour][vm].keys():
#                     all_data[hour][vm][disk] = {}
#                 all_data[hour][vm][disk] = { \
#                     'dataread':  content[vm][disk]['dataread'][n],  \
#                     'datawrite': content[vm][disk]['datawrite'][n], \
#                     'reads':     content[vm][disk]['reads'][n],     \
#                     'writes':    content[vm][disk]['writes'][n] }

def writeConfigFile(f, defData):
    global testVMs
    with open(f, 'w') as file:
        print 'Writing configuration file'
        file.write(str(defData))
        file.write('\n')
        file.write(str(testVMs))

def loadConfigFile(f):
    with open(f, 'r') as file:
        content = [ line for line in file.readlines() ]
    config = {}
    if os.path.isfile(BATCHES_OUTPUT_FILE) and os.stat(BATCHES_OUTPUT_FILE).st_size > 32:
        with open(BATCHES_OUTPUT_FILE, 'r') as batchesFile:
            lastbatch = batchesFile.readlines()[-1]
            batchData = eval(lastbatch)
            config['latest'] = batchData
    else:
        config['latest'] = False;
    config['defData'] = eval(content[0])
    config['testVMs'] = eval(content[1])
    return config;

class OnappException(Exception):
    def __init__(self, d, f, reason=False):
        self.data = d;
        self.func = f;
        self.reason = reason;
        print('OnappError, Action: {}, Data: {}'.format(f, d))
        if self.reason is not False: print('Reason: {}'.format(reason))

if __name__ == "__main__":
    if CLEAN_TEST_VMS:
        vms = loadConfigFile(CONFIG_FILE)['testVMs']
        deletejobs = [ Job('DeleteVM', vm_id=vm['id'], destroy_all_backups=1) for vm in vms ]
        print "Deleting all test VMs."
        runParallel(deletejobs)
        print "Delete jobs should be running now. Please monitor through interface."
        sys.exit()
    if ONLY_GENERATE_OUTPUT:
        file = ONLY_GENERATE_OUTPUT
        if not os.path.isfile(file):
            raise EnvironmentError("File does not exist: {}".format(file))
        f_handle = open(file, 'r')
        content = [ ast.literal_eval(line.strip()) for line in f_handle.readlines() ]
        f_handle.close()
        processed_content = newProcessOutput(content)
        print processed_content;
        f_handle = open("{}.processed".format(file), 'w')
        f_handle.write(str(processed_content))
        f_handle.flush()
        f_handle.close()
        print "Processed content has been written to {}.processed".format(file)
        sys.exit()
    beginTime = datetime.datetime.now()
    if just_iops_duration is not False:
        if not os.path.isfile(CONFIG_FILE) or os.stat(CONFIG_FILE).st_size < 8:
            print 'Config file is not found or empty'
            sys.exit()
        else:
            iops_vms = loadConfigFile(CONFIG_FILE)['testVMs']
            if not iops_vms:  print 'No VMs found or config file was empty.'
            try:
                status_jobs = [ Job('VMStatus', vm_id=vms['id']) for vms in iops_vms ]
                status_jobs_results = runParallel(status_jobs)
            except HTTPError:
                print 'Virtual machine in config file may not exist or had an error checking on via API'
                sys.exit()
            now = datetime.datetime.now();
            iops = gatherIOPSData(iops_vms, {'start': now - datetime.timedelta(hours=int(just_iops_duration)), 'end':now})
            processed = newProcessIOPSData(iops)
            if VERBOSE:
                print " -------- Python Data ---------- "
                print str(iops)
                print " -------- Processed Data ----------- "
                print json.dumps(processed, indent=1)
            print 'Writing IOPS Data to file IOPS.json and IOPS.pydat'
            with open('IOPS.json', 'w') as iops_file:
                iops_file.write(json.dumps(processed))
            with open('IOPS.pydat', 'w') as pydat_file:
                pydat_file.write(str(iops))
        sys.exit();
    if os.path.isfile(CONFIG_FILE) and os.stat(CONFIG_FILE).st_size > 8:
        if not quiet: print "Found configuration file from previous run, attempting restart."
        restartParameters = loadConfigFile(CONFIG_FILE);
        #beginTime = restartParameters['beginTime']
        confhvs = []
        for v in restartParameters['testVMs']:
            if v['hypervisor_id'] not in confhvs: confhvs.append(v['hypervisor_id'])
        zone = dsql("SELECT DISTINCT hypervisor_group_id FROM hypervisors WHERE id in ({})".format(','.join(str(t) for t in confhvs)))
        if type(zone) is long:
            HVZONE = zone;
        else:
            raise OnappException(zone, "Determine Hypervisor Zone", "There are multiple or no zones for these virtual machines.")
    else:
        restartParameters = False;
        confhvs = []
        if use_existing_virtual_machines:
            for v in ListVMs():
                if v['hypervisor_id'] not in confhvs: confhvs.append(v['hypervisor_id'])
            zone = dsql("SELECT DISTINCT hypervisor_group_id FROM hypervisors WHERE id in ({})".format(','.join(str(t) for t in confhvs)))
            if type(zone) is long:
                HVZONE = zone;
            else:
                raise OnappException(zone, "Determine Hypervisor Zone", "There are multiple or no zones for these virtual machines.")
        else:
            HVZONE = getHVZone();
    if DELETE_VMS and not use_existing_virtual_machines and not restartParameters:
        actually_delete = raw_input("Are you sure you wish to delete the virtual machines used after the test is complete? (Y/N)")
        while actually_delete not in ['Y', 'N']:
            actually_delete = raw_input('Invalid. (Y/N)?')
        if actually_delete == 'Y':
            DELETE_VMS = True;
        elif actually_delete == 'N':
            DELETE_VMS = False;
    unlockJobs, destroyJobs, returnData, testParameters = runBatchesTest(int(batchsize), restartParameters)
    # else: print "Right now, run it with the -b flag for batch testing, or import as library."
    print "Gathering data and submitting it..."
    rData = {}
    rData['batches'] = newProcessOutput(returnData)
    if VERBOSE: print "Generating hourly stats for IOPS data."
    hourly_status_result = runCmd(['su', 'onapp', '-c', 'cd /onapp/interface; RAILS_ENV=production rake vm:generate_hourly_stats'])
    all_the_iops_data = gatherIOPSData(testVMs, {'start':beginTime, 'end':datetime.datetime.now()})
    processed_iops_data = newProcessIOPSData(all_the_iops_data)
    rData['iops'] = processed_iops_data
    rData['config'] = gatherConfigData(HVZONE)
    rData['config'].update(testParameters)

    print('Writing Python data to {}'.format(PYTHON_TEST_RESULTS_FILE))
    with open(PYTHON_TEST_RESULTS_FILE, 'w') as pFile:
        pFile.write(str(rData))

    print('Writing all JSON data to {}'.format(JSON_TEST_RESULTS_FILE))
    with open('test_results.json', 'w') as jFile:
        jFile.write(json.dumps(rData))

    if SEND_RESULTS:
        submit_result = apiCall('/api/burnin?token={}'.format(SEND_RESULTS), data=rData, target='https://architecture.onapp.com', method='POST')

    #returnData['iops'] = newProcessIOPSData(
    if DELETE_VMS:
        runParallel(unlockJobs)
        time.sleep(4)
        runParallel(destroyJobs)
