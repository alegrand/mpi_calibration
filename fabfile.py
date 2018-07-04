#! /usr/bin/env python3

import re
import datetime
import fabric
import logging
import colorlog
import time
import sys
import socket
import tempfile
import argparse
import zipfile
import yaml
import random
import json
import io

handler = colorlog.StreamHandler()
formatter = colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s][%(levelname)s] %(message_log_color)s%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    secondary_log_colors={
        'message': {
            'DEBUG': 'white',
            'INFO': 'white',
            'WARNING': 'white',
            'ERROR': 'white',
            'CRITICAL': 'white',
        }
    }
)
handler.setFormatter(formatter)
logger = colorlog.getLogger(__name__)
log_stream = io.StringIO()
io_handler = logging.StreamHandler(log_stream)
io_handler.setFormatter(logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s'))
logger.addHandler(handler)
logger.addHandler(io_handler)
logger.setLevel(logging.DEBUG)


class Time:
    def __init__(self, hours=None, minutes=None, seconds=None):
        assert hours or minutes or seconds
        self.hours = hours or 0
        self.minutes = minutes or 0
        self.seconds = seconds or 0

    def __repr__(self):
        return '%.2d:%.2d:%.2d' % (self.hours, self.minutes, self.seconds)


class Job:
    auto_oardel = False

    def __init__(self, jobid, connection, deploy=False):
        self.jobid = jobid
        self.connection = connection
        self.deploy = deploy

    def __del__(self):
        if self.auto_oardel:
            self.oardel()

    @staticmethod
    def __generic_run(connection, title, command, hide_output=True, **kwargs):
        logger.info('[%s] %s' % (title, command))
        if 'hide' not in kwargs:
            kwargs['hide'] = True
        if hide_output:
            command = '%s &> /dev/null' % command
        return connection.run(command, **kwargs)

    def run_frontend(self, command, **kwargs):
        return self.__generic_run(self.connection, 'frontend', command, **kwargs)

    def run_nodes(self, command, directory='/tmp', **kwargs):
        command = 'cd %s && %s' % (directory, command)
        return self.__generic_run(self.nodes, 'allnodes', command, **kwargs)

    @classmethod
    def run_node(cls, node, command, directory='/tmp', **kwargs):
        command = 'cd %s && %s' % (directory, command)
        return cls.__generic_run(node, node.host, command, **kwargs)

    @classmethod
    def put(cls, node, origin_file, target_file):
        logger.info('[%s] put: %s → %s' %
                    (node.host, origin_file, target_file))
        node.put(origin_file, target_file)

    def put_nodes(self, origin_file, target_file):
        for node in self.nodes:
            self.put(node, origin_file, target_file)

    @classmethod
    def get(cls, node, origin_file, target_file):
        logger.info('[%s] get: %s → %s' %
                    (node.host, origin_file, target_file))
        node.get(origin_file, target_file)

    def put_frontend(self, origin_file, target_file, **kwargs):
        logger.info('[frontend] put: %s → %s' % (origin_file, target_file))
        self.connection.put(origin_file, target_file)

    def get_frontend(self, origin_file, target_file, **kwargs):
        logger.info('[frontend] get: %s → %s' % (origin_file, target_file))
        self.connection.get(origin_file, target_file)

    def oardel(self):
        self.run_frontend('oardel %d' % self.jobid)

    @property
    def oar_node_file(self):
        return '/var/lib/oar/%d' % self.jobid

    def oarstat(self):
        result = self.run_frontend('oarstat -fJ -j %d' % self.jobid, hide_output=False)
        return json.loads(result.stdout)[str(self.jobid)]

    def __find_hostnames(self):
        # TODO Use oarstat -fJ
        sleep_time = 5
        while True:  # we wait for the job to be launched, i.e., the oarfile to exist
            stat = self.oarstat()
            hostnames = stat['assigned_network_address']
            if not hostnames:
                time.sleep(sleep_time + random.uniform(0, sleep_time/5))
                sleep_time = min(sleep_time*2, 60)
            else:
                break
        hostnames.sort()
        self.__hostnames = hostnames

    @property
    def hostnames(self):
        try:
            return list(self.__hostnames)
        except AttributeError:
            self.__find_hostnames()
            return list(self.__hostnames)

    def kadeploy(self, env='debian9-x64-min'):
        assert self.deploy
        # Wait for the oar_node_file to be available. Not required, just aesthetic.
        self.hostnames
        self.run_frontend('kadeploy3 -k -f %s -e %s' %
                          (self.oar_node_file, env))
        return self

    def __repr__(self):
        return '%s(%d)' % (self.__class__.__name__, self.jobid)

    @classmethod
    def oarsub(cls, connection, constraint, walltime, nb_nodes, *, deploy=True, queue=None, immediate=True, script=None):
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        constraint = '%s/nodes=%d,walltime=%s' % (
            constraint, nb_nodes, walltime)
        deploy_str = '-t deploy ' if deploy else '-t allow_classic_ssh'
        queue_str = '-q %s ' % queue if queue else ''
        cmd = 'oarsub %s%s -l "%s"' % (queue_str, deploy_str, constraint)
        if immediate:
            cmd += ' -r "%s"' % date
        if script:
            assert not immediate
            cmd += " '%s'" % script
        result = cls.__generic_run(
            connection, 'frontend', cmd, hide_output=False)
        regex = re.compile('OAR_JOB_ID=(\d+)')
        jobid = int(regex.search(result.stdout).groups()[0])
        return cls(jobid, connection=connection, deploy=deploy)

    @classmethod
    def oarsub_cluster(cls, site, username, clusters, walltime, nb_nodes, *, deploy=True, queue=None, immediate=True, script=None):
        connection = cls.g5k_connection(site, username)
        clusters = ["'%s'" % clus for clus in clusters]
        constraint = "{cluster in (%s)}" % ', '.join(clusters)
        return cls.oarsub(connection, constraint, walltime, nb_nodes, deploy=deploy, queue=queue, immediate=immediate, script=script)

    @classmethod
    def oarsub_hostnames(cls, site, username, hostnames, walltime, nb_nodes=None, *, deploy=True, queue=None, immediate=True, script=None):
        def expandg5k(host, site):
            if 'grid5000' not in host:
                host = '%s.%s.grid5000.fr' % (host, site)
            return host
        connection = cls.g5k_connection(site, username)
        hostnames = ["'%s'" % expandg5k(host, site) for host in hostnames]
        constraint = "{network_address in (%s)}" % ', '.join(hostnames)
        if nb_nodes is None:
            nb_nodes = len(hostnames)
        return cls.oarsub(connection, constraint, walltime, nb_nodes, deploy=deploy, queue=queue, immediate=immediate, script=script)

    @classmethod
    def g5k_connection(cls, site, username):
        if 'grid5000' in socket.getfqdn():  # already inside G5K, no need for a gateway
            return fabric.Connection(site, user=username)
        else:
            gateway = fabric.Connection('access.grid5000.fr', user=username)
            return fabric.Connection(site, user=username, gateway=gateway)

    def __open_nodes_connection(self):
        sleep_time = 5
        while True:
            try:
                self.run_nodes('hostname')
            except fabric.exceptions.GroupException:
                time.sleep(sleep_time + random.uniform(0, sleep_time/5))
                sleep_time = min(sleep_time*2, 60)
            else:
                break

    @property
    def nodes(self):
        try:
            return self.__nodes
        except AttributeError:
            if self.deploy:
                user = 'root'
            else:
                user = self.connection.user
            connections = [fabric.Connection(
                host, user=user, gateway=self.connection) for host in self.hostnames]
            connections = fabric.ThreadingGroup.from_connections(connections)
            self.__nodes = connections
            self.__open_nodes_connection()
            return self.__nodes

    def apt_install(self, *packages):
        sudo = 'sudo-g5k ' if not self.deploy else ''
        cmd = '{0}apt update && {0}apt upgrade -y'.format(sudo)
        self.run_nodes(cmd)
        cmd = sudo + 'apt install -y %s' % ' '.join(packages)
        self.run_nodes(cmd)
        return self

    def __add_raw_information(self, archive_name, filename, command):
        if not self.deploy:
            command = 'sudo-g5k %s' % command
        self.run_nodes(command, hide_output=False)
        origin = self.nodes[0]
        self.run_node(origin, 'cp %s information/%s' % (filename, origin.host))
        for host in self.hostnames:
            if host == origin.host:
                continue
            self.run_node(origin, 'scp %s:/tmp/%s information/%s' % (host, filename, host))

    def add_raw_information(self, archive_name):
        origin = self.nodes[0]
        for host in self.hostnames:
            self.run_node(origin, 'mkdir -p information/%s' % host)
        commands_with_files = {
                    'cpuinfo.txt': 'cp /proc/cpuinfo cpuinfo.txt',
                    'environment.txt': 'env > environment.txt',
                    'topology.xml': 'lstopo topology.xml',
                    'topology.pdf': 'lstopo topology.pdf',
                    'lspci.txt': 'lspci -v > lspci.txt',
                    'dmidecode.txt': 'dmidecode > dmidecode.txt',
                    }
        for filename, command in commands_with_files.items():
            self.__add_raw_information(archive_name, filename, command)
        self.run_node(origin, 'zip -ru %s information' % archive_name)
        self.run_node(origin, 'rm -rf information')

    def platform_information(self):
        commands = {'kernel': 'uname -r',
                    'version': 'cat /proc/version',
                    'gcc': 'gcc -dumpversion',
                    'mpi': 'mpirun --version | head -n 1',
                    'cpu': 'cat /proc/cpuinfo  | grep "name"| uniq | cut -d: -f2 ',
                    }
        result = {host: {} for host in self.hostnames}
        for cmd_name, cmd in commands.items():
            output = self.run_nodes(cmd, hide_output=False)
            for host, res in output.items():
                result[host.host][cmd_name] = res.stdout.strip()
            if len(set([result[h][cmd_name] for h in self.hostnames])) != 1:
                logger.warning('Different settings found for %s (command %s)' % (cmd_name, cmd))
        arp_cmd = 'arp -a' if self.deploy else 'sudo-g5k arp -a'
        arp_output = self.run_nodes(arp_cmd, hide_output=False)
        for node, arp in arp_output.items():
            arp_dict = {}
            for line in arp.stdout.strip().split('\n'):
                hostname, *rest = line.split()
                try:
                    arp_dict[hostname].append(rest)
                except KeyError:
                    arp_dict[hostname] = [rest]
            origin = node.host
            res = result[origin]
            res['ip_address'] = self.run_node(node, 'hostname -I', hide_output=False).stdout.strip()
            res['arp'] = {}
            res = res['arp']
            for hostname, interfaces in arp_dict.items():
                res[hostname] = []
                for line in interfaces:
                    res[hostname].append(' '.join(line))
        result['site'] = self.connection.host
        result['jobid'] = self.jobid
        result['deployment'] = self.deploy
        result['command'] = ' '.join(sys.argv)
        return result


def mpi_install(job):
    logger.info(str(job))
    logger.info('Nodes: %s and %s' % tuple(job.hostnames))
    time.sleep(5)
    if job.deploy:
        if isinstance(job.deploy, str):
            job.kadeploy(env=job.deploy)
        else:
            job.kadeploy()
    job.apt_install(
        'build-essential',
        'python3',
        'python3-dev',
        'zip',
        'make',
        'git',
        'time',
        'libopenmpi-dev',
        'openmpi-bin',
        'libxml2',
        'libxml2-dev',
        'hwloc',
        'pciutils',
        'net-tools',
    )
    job.run_nodes(
        'git clone https://gitlab.inria.fr/simgrid/platform-calibration.git')
    job.run_nodes('cd platform-calibration/src/calibration && make')
    return job


def send_key(job):
    if not job.deploy:  # no need for that if this is not a fresh deploy
        return
    origin = job.nodes[0]
    target = job.nodes[1]
    job.run_node(origin, 'ssh-keygen -b 2048 -t rsa -f .ssh/id_rsa -q -N ""', directory='~')
    tmp_file = tempfile.NamedTemporaryFile(dir='.')
    job.get(origin, '.ssh/id_rsa.pub', tmp_file.name)
    job.put(target, tmp_file.name, '/tmp/id_rsa.pub')
    tmp_file.close()
    job.run_node(
        target, 'cat /tmp/id_rsa.pub >> .ssh/authorized_keys', hide_output=False, directory='~')
    job.run_node(
        origin, 'ssh -o "StrictHostKeyChecking no" %s hostname' % target.host, directory='~')
    short_target = target.host[:target.host.find('.')]
    job.run_node(
        origin, 'ssh -o "StrictHostKeyChecking no" %s hostname' % short_target, directory='~')


def run_calibration(job):
    def remove_g5k(hostname):
        return hostname[:hostname.index('.')]
    origin = job.nodes[0]
    target = job.nodes[1]
    xml_content = '''<?xml version="1.0"?>
        <config id="Config">
        <!-- prefix name for the output files -->
         <prefix value="exp"/>
        <!-- directory name for the output files (as seen from calibrate.c) -->
         <dirname value="exp"/>
        <!-- Name of the file that contains all message sizes we can choose from. -->
         <sizeFile value="zoo_sizes"/>
        <!-- Minimum size of the messages to send-->
         <minSize value="0"/>
        <!-- Maximum size of the messages to send-->
         <maxSize value="1000000"/>
        <!-- Number of iterations per size of message-->
         <iterations value="5"/>
        </config>
    '''
    tmp_file = tempfile.NamedTemporaryFile(dir='.')
    with open(tmp_file.name, 'w') as exp_file:
        exp_file.write(xml_content)
    path = '/tmp/platform-calibration/src/calibration'
    node_exp_filename = 'exp.xml'
    job.put_nodes(tmp_file.name, path + '/' + node_exp_filename)
    tmp_file.close()
    job.run_nodes('mkdir -p %s' % (path + '/exp'))
    host = ','.join([node.host for node in job.nodes])
    start_date = datetime.datetime.now()
    job.run_node(origin, 'mpirun --allow-run-as-root -np 2 -host %s ./calibrate -f %s' %
                 (host, node_exp_filename), directory=path)
    end_date = datetime.datetime.now()
    archive_name = '%s-%s_%s_%d.zip' % (remove_g5k(origin.host), remove_g5k(target.host), datetime.date.today(),
                                        job.jobid)
    archive_path = '/tmp/%s' % archive_name
    job.run_node(origin, 'zip -r %s exp' % archive_path, directory=path)
    job.add_raw_information(archive_path)
    job.get(origin, archive_path, archive_name)
    tmp_file = tempfile.NamedTemporaryFile(dir='.')
    job_info = job.platform_information()
    job_info['start'] = start_date.isoformat()
    job_info['stop'] = end_date.isoformat()
    with open(tmp_file.name, 'w') as f:
        yaml.dump(job_info, f, default_flow_style=False)
    archive = zipfile.ZipFile(archive_name, 'a')
    archive.write(tmp_file.name, 'info.yaml')
    with open(tmp_file.name, 'w') as f:
        yaml.dump(job.oarstat(), f, default_flow_style=False)
    archive.write(tmp_file.name, 'oarstat.yaml')
    with open(tmp_file.name, 'w') as f:
        log = log_stream.getvalue()
        log = log.encode('ascii', 'ignore').decode()  # removing any non-ascii character
        f.write(log)
    archive.write(tmp_file.name, 'commands.log')
    archive.close()
    tmp_file.close()


def mpi_calibration(job):
    mpi_install(job)
    send_key(job)
    run_calibration(job)
    return job


def get_job(args, nb_nodes=2, check_nb_nodes=False):
    user = args.username
    site = args.site
    deploy = args.deploy
    queue = args.queue
    if hasattr(args, 'cluster'):
        job = Job.oarsub_cluster(site, user, clusters=[
                                 args.cluster], walltime=Time(minutes=15), nb_nodes=2, deploy=deploy, queue=queue)
    elif hasattr(args, 'nodes'):
        job = Job.oarsub_hostnames(
            site, user, hostnames=args.nodes, walltime=Time(minutes=15), deploy=deploy, queue=queue)
    else:
        connection = Job.g5k_connection(site, user)
        job = Job(args.jobid, connection, deploy=deploy)
    if check_nb_nodes:
        if len(job.hostnames) != 2:
            logger.error(
                'Wrong number of nodes for job: got %d, expected 2.' % len(job.hostnames))
            logger.error('Hostname(s): %s' % ' '.join(job.hostnames))
            job.oardel()
            sys.exit()
    return job


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Automatic MPI calibration')
    parser.add_argument('site', choices=['grenoble', 'lille', 'luxembourg', 'lyon', 'nancy', 'nantes', 'rennes', 'sophia'],
                        help='Site for the experiment.')
    parser.add_argument('username', type=str,
                        help='username to use for the experiment.')
    parser.add_argument('--deploy', choices=['debian9-x64-%s' % mode for mode in ['min', 'base', 'nfs', 'big']],
                        default=False, help='Do a full node deployment.')
    parser.add_argument('--queue', choices=['testing', 'production'],
                        default=None, help='Use a non-default queue.')
    sp = parser.add_subparsers()
    sp_cluster = sp.add_parser('cluster', help='Cluster for the experiment.')
    sp_cluster.add_argument('cluster', type=str)
    sp_nodes = sp.add_parser('nodes', help='Nodes for the experiment.')
    sp_nodes.add_argument('nodes', type=str, nargs=2)
    sp_jobid = sp.add_parser('jobid', help='Job ID for the experiment.')
    sp_jobid.add_argument('jobid', type=int)
    args = parser.parse_args()
    job = get_job(args, check_nb_nodes=True)
    mpi_calibration(job)
    job.oardel()
