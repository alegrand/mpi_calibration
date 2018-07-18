#! /usr/bin/env python3

import re
import datetime
import fabric
import logging
import colorlog
import time
import os
import sys
import socket
import tempfile
import argparse
import zipfile
import yaml
import random
import json
import io
import lxml.etree

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


class Nodes:
    def __init__(self, nodes, name, working_dir):
        self.nodes = fabric.ThreadingGroup.from_connections(nodes)
        self.name = name
        self.working_dir = working_dir

    def __iter__(self):
        yield from self.nodes

    def run(self, command, **kwargs):
        if 'directory' in kwargs:
            directory = os.path.join(self.working_dir, kwargs['directory'])
            del kwargs['directory']
        else:
            directory = self.working_dir
        logger.info('[%s | %s] %s' % (self.name, directory, command))
        if 'hide' not in kwargs:
            kwargs['hide'] = True
        command = 'cd %s && %s' % (directory, command)
        if 'hide_output' in kwargs:
            if kwargs['hide_output']:
                command = '%s &> /dev/null' % command
            del kwargs['hide_output']
        else:  # hide output by default
            command = '%s &> /dev/null' % command
        return self.nodes.run(command, **kwargs)

    def run_unique(self, *args, **kwargs):
        result = list(self.run(*args, **kwargs).values())
        for res in result[1:]:
            assert res.stdout == result[0].stdout
            assert res.stderr == result[1].stderr
        return result[0]

    def put(self, origin_file, target_file):
        target_file = os.path.join(self.working_dir, target_file)
        logger.info('[%s] put: %s → %s' % (self.name, origin_file, target_file))
        for node in self.nodes:
            node.put(origin_file, target_file)

    def get(self, origin_file, target_file):
        assert len(self.nodes) == 1
        origin_file = os.path.join(self.working_dir, origin_file)
        logger.info('[%s] get: %s → %s' % (self.name, origin_file, target_file))
        for node in self.nodes:
            node.get(origin_file, target_file)

    @property
    def hostnames(self):
        return [node.host for node in self.nodes]

    def __write_large_file(self, content, target_file):
        tmp_file = tempfile.NamedTemporaryFile(dir='.')
        with open(tmp_file.name, 'w') as f:
            f.write(content)
        self.put(tmp_file.name, target_file)
        tmp_file.close()

    def write_files(self, content, *target_files):
        target_files = [os.path.join(self.working_dir, target) for target in target_files]
        if len(content) < 80:  # arbitrary threshold...
            cmd = "echo -n '%s' | tee %s" % (content, ' '.join(target_files))
            self.run(cmd)
        else:
            self.__write_large_file(content, target_files[0])
            if len(target_files) > 1:
                remaining_files = ' '.join(target_files[1:])
                cmd = 'cat %s | tee %s' % (target_files[0], remaining_files)
                self.run(cmd)

    @property
    def cores(self):
        try:
            return self.__cores
        except AttributeError:
            self.__cores = self._get_all_cores()
            return self.__cores

    @property
    def hyperthreads(self):
        try:
            return self.__hyperthreads
        except AttributeError:
            self.__hyperthreads = [group[1:] for group in self.cores]
            self.__hyperthreads = sum(self.__hyperthreads, [])
            return self.__hyperthreads

    def enable_hyperthreading(self):
        self.__set_hyperthreads(1)

    def disable_hyperthreading(self):
        self.__set_hyperthreads(0)

    def __set_hyperthreads(self, value):
        assert value in (0, 1)
        filenames = ['/sys/devices/system/cpu/cpu%d/online' % core_id for core_id in self.hyperthreads]
        self.write_files(str(value), *filenames)

    def _get_all_cores(self):
        ref_cores = None
        all_xml = self.__get_platform_xml()
        for node, xml in all_xml.items():
            cores = self.__get_all_cores(xml)
            if ref_cores is None:
                ref_cores = cores
                ref_node = node
            elif cores != ref_cores:
                raise ValueError('Got different topologies for nodes %s and %s' % (ref_node.host, node.host))
        return ref_cores

    def __get_all_cores(self, xml):
        xml = xml.findall('object')[0]
        return self.__process_cache(xml)

    def __get_platform_xml(self):
        result = self.run('lstopo topology.xml && cat topology.xml', hide_output=False)
        xml = {}
        for node, output in result.items():
            xml[node] = lxml.etree.fromstring(output.stdout.encode('utf8'))
        return xml

    def __process_cache(self, xml):
        cache = xml.findall('object')
        result = []
        for obj in cache:
            if obj.get('type') == 'Core':
                result.append(self.__process_core(obj))
            elif obj.get('type') in ('Machine', 'NUMANode', 'Package', 'Cache', 'L3Cache',
                                     'L2Cache', 'L1Cache', 'L1iCache'):
                result.extend(self.__process_cache(obj))
        return result

    def __process_core(self, xml):
        result = []
        for pu in xml.findall('object'):
            assert pu.get('type') == 'PU'
            result.append(int(pu.get('os_index')))
        return result


class Job:
    auto_oardel = False

    def __init__(self, jobid, frontend, deploy=False):
        self.jobid = jobid
        self.frontend = frontend
        self.deploy = deploy
        self.user = frontend.nodes[0].user
        self.site = frontend.nodes[0].host

    def __del__(self):
        if self.auto_oardel:
            try:
                self.oardel()
            except Exception:
                pass

    def oardel(self):
        self.frontend.run('oardel %d' % self.jobid)

    @property
    def oar_node_file(self):
        return '/var/lib/oar/%d' % self.jobid

    def oarstat(self):
        result = self.frontend.run_unique('oarstat -fJ -j %d' % self.jobid, hide_output=False)
        return json.loads(result.stdout)[str(self.jobid)]

    @classmethod
    def _oarstat_user(cls, frontend):
        try:
            result = frontend.run_unique('oarstat -J -u', hide_output=False)
        except fabric.exceptions.GroupException as e:  # no job
            return {}
        return json.loads(result.stdout)

    @classmethod
    def get_jobs(cls, site, username):
        connection = cls.g5k_connection(site, username)
        frontend = Nodes([connection], name='frontend-%s' % site, working_dir='/home/%s' % username)
        stat = cls._oarstat_user(frontend)
        jobs = []
        for jobid, job_stat in stat.items():
            if job_stat['state'] in ('Running', 'Waiting'):
                job = int(jobid)
                deploy = 'deploy' in job_stat['types']
                jobs.append(cls(job, frontend, deploy=deploy))
        if len(jobs) == 0:
            raise ValueError('No jobs were found for user %s on site %s' % (username, site))
        return jobs

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
        self.frontend.run('kadeploy3 -k -f %s -e %s' % (self.oar_node_file, env))
        return self

    def __repr__(self):
        return '%s(%d)' % (self.__class__.__name__, self.jobid)

    @classmethod
    def oarsub(cls, frontend, constraint, walltime, nb_nodes, *,
               deploy=True, queue=None, immediate=True, script=None):
        name = random.choice('☕🥐')
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        constraint = '%s/nodes=%d,walltime=%s' % (
            constraint, nb_nodes, walltime)
        deploy_str = '-t deploy ' if deploy else '-t allow_classic_ssh'
        queue_str = '-q %s ' % queue if queue else ''
        cmd = 'oarsub -n "%s" %s%s -l "%s"' % (name, queue_str, deploy_str, constraint)
        if immediate:
            cmd += ' -r "%s"' % date
        if script:
            assert not immediate
            cmd += " '%s'" % script
        result = frontend.run_unique(cmd, hide_output=False)
        regex = re.compile('OAR_JOB_ID=(\d+)')
        jobid = int(regex.search(result.stdout).groups()[0])
        return cls(jobid, frontend=frontend, deploy=deploy)

    @classmethod
    def oarsub_cluster(cls, site, username, clusters, walltime, nb_nodes, *,
                       deploy=True, queue=None, immediate=True, script=None):
        connection = cls.g5k_connection(site, username)
        frontend = Nodes([connection], name='frontend', working_dir='/home/%s' % username)
        clusters = ["'%s'" % clus for clus in clusters]
        constraint = "{cluster in (%s)}" % ', '.join(clusters)
        return cls.oarsub(frontend, constraint, walltime, nb_nodes, deploy=deploy,
                          queue=queue, immediate=immediate, script=script)

    @classmethod
    def oarsub_hostnames(cls, site, username, hostnames, walltime, nb_nodes=None, *,
                         deploy=True, queue=None, immediate=True, script=None):
        def expandg5k(host, site):
            if 'grid5000' not in host:
                host = '%s.%s.grid5000.fr' % (host, site)
            return host
        connection = cls.g5k_connection(site, username)
        frontend = Nodes([connection], name='frontend', working_dir='/home/%s' % username)
        hostnames = ["'%s'" % expandg5k(host, site) for host in hostnames]
        constraint = "{network_address in (%s)}" % ', '.join(hostnames)
        if nb_nodes is None:
            nb_nodes = len(hostnames)
        return cls.oarsub(frontend, constraint, walltime, nb_nodes, deploy=deploy,
                          queue=queue, immediate=immediate, script=script)

    @classmethod
    def g5k_connection(cls, site, username):
        if 'grid5000' in socket.getfqdn():  # already inside G5K, no need for a gateway
            connection = fabric.Connection(site, user=username)
        else:
            gateway = fabric.Connection('access.grid5000.fr', user=username)
            connection = fabric.Connection(site, user=username, gateway=gateway)
        return connection

    def __open_nodes_connection(self):
        sleep_time = 5
        while True:
            try:
                self.nodes.run('echo "hello world"')
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
                user = self.user
            connections = [fabric.Connection(host, user=user, gateway=self.frontend.nodes[0])
                           for host in self.hostnames]
            self.__nodes = Nodes(connections, name='allnodes', working_dir='/tmp')
            self.orchestra = Nodes(connections[1:], name='orchestra', working_dir='/tmp')
            self.director = Nodes([connections[0]], name='director', working_dir='/tmp')
            self.__open_nodes_connection()
            return self.__nodes

    def apt_install(self, *packages):
        sudo = 'sudo-g5k ' if not self.deploy else ''
        cmd = '{0}apt update && {0}DEBIAN_FRONTEND=noninteractive apt upgrade -yq'.format(sudo)
        self.nodes.run(cmd)
        cmd = sudo + 'DEBIAN_FRONTEND=noninteractive apt install -y %s' % ' '.join(packages)
        self.nodes.run(cmd)
        return self

    def __add_raw_information(self, archive_name, filename, command):
        if not self.deploy:
            command = 'sudo-g5k %s' % command
        self.nodes.run(command, hide_output=False)
        self.director.run('cp %s information/%s' % (filename, self.director.hostnames[0]))
        for host in self.hostnames:
            if host == self.director.hostnames[0]:
                continue
            self.director.run('scp %s:/tmp/%s information/%s' % (host, filename, host))

    def add_raw_information(self, archive_name):
        for host in self.hostnames:
            self.director.run('mkdir -p information/%s' % host)
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
        self.director.run('zip -ru %s information' % archive_name)
        self.director.run('rm -rf information')

    def platform_information(self):
        commands = {'kernel': 'uname -r',
                    'version': 'cat /proc/version',
                    'gcc': 'gcc -dumpversion',
                    'mpi': 'mpirun --version | head -n 1',
                    'cpu': 'cat /proc/cpuinfo  | grep "name"| uniq | cut -d: -f2 ',
                    }
        result = {host: {} for host in self.hostnames}
        for cmd_name, cmd in commands.items():
            output = self.nodes.run(cmd, hide_output=False)
            for host, res in output.items():
                result[host.host][cmd_name] = res.stdout.strip()
            if len(set([result[h][cmd_name] for h in self.hostnames])) != 1:
                logger.warning('Different settings found for %s (command %s)' % (cmd_name, cmd))
        arp_cmd = 'arp -a' if self.deploy else 'sudo-g5k arp -a'
        arp_output = self.nodes.run(arp_cmd, hide_output=False)
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
#            res['ip_address'] = node.run('hostname -I').stdout.strip()
            res['arp'] = {}
            res = res['arp']
            for hostname, interfaces in arp_dict.items():
                res[hostname] = []
                for line in interfaces:
                    res[hostname].append(' '.join(line))
        result['site'] = self.site
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
    job.nodes.run('git clone https://gitlab.inria.fr/simgrid/platform-calibration.git')
    job.nodes.run('make', directory='platform-calibration/src/calibration')
    return job


def send_key(job):
    if not job.deploy:  # no need for that if this is not a fresh deploy
        return
    job.director.run('ssh-keygen -b 2048 -t rsa -f .ssh/id_rsa -q -N ""', directory='/root')
    tmp_file = tempfile.NamedTemporaryFile(dir='.')
    job.director.get('/root/.ssh/id_rsa.pub', tmp_file.name)
    job.orchestra.put(tmp_file.name, '/tmp/id_rsa.pub')
    tmp_file.close()
    job.orchestra.run('cat /tmp/id_rsa.pub >> .ssh/authorized_keys', hide_output=False, directory='/root')
    for host in job.orchestra.hostnames:
        job.director.run('ssh -o "StrictHostKeyChecking no" %s hostname' % host, directory='/root')
        short_target = host[:host.find('.')]
        job.director.run('ssh -o "StrictHostKeyChecking no" %s hostname' % short_target, directory='/root')


def run_calibration(job):
    def remove_g5k(hostname):
        return hostname[:hostname.index('.')]
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
    path = '/tmp/platform-calibration/src/calibration'
    node_exp_filename = 'exp.xml'
    job.nodes.write_files(xml_content, path + '/' + node_exp_filename)
    job.nodes.run('mkdir -p %s' % (path + '/exp'))
    host = ','.join([node.host for node in job.nodes])
    start_date = datetime.datetime.now()
    job.director.run('mpirun --allow-run-as-root -np 2 -host %s ./calibrate -f %s' % (host, node_exp_filename),
                     directory=path)
    end_date = datetime.datetime.now()
    archive_name = '%s-%s_%s_%d.zip' % (remove_g5k(job.director.hostnames[0]),
                                        remove_g5k(job.orchestra.hostnames[0]),
                                        datetime.date.today(),
                                        job.jobid)
    archive_path = '/tmp/%s' % archive_name
    job.director.run('zip -r %s exp' % archive_path, directory=path)
    job.add_raw_information(archive_path)
    job.director.get(archive_path, archive_name)
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
    if args.submission_type == 'cluster':
        job = Job.oarsub_cluster(site, user, clusters=[
                                 args.cluster], walltime=Time(minutes=15), nb_nodes=2, deploy=deploy, queue=queue)
    elif args.submission_type == 'nodes':
        job = Job.oarsub_hostnames(
            site, user, hostnames=args.nodes, walltime=Time(minutes=15), deploy=deploy, queue=queue)
    else:
        assert args.submission_type == 'jobid'
        connection = Job.g5k_connection(site, user)
        frontend = Nodes([connection], name='frontend', working_dir='/home/%s' % user)
        job = Job(args.jobid, frontend, deploy=deploy)
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
    sp = parser.add_subparsers(dest='submission_type')
    sp.required = True
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
