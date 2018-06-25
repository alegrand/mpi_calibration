#! /usr/bin/env python3

import re
import datetime
import invoke
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

    def __init__(self, jobid, connection):
        self.jobid = jobid
        self.connection = connection

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

    def run_nodes(self, command, **kwargs):
        return self.__generic_run(self.nodes, 'allnodes', command, **kwargs)

    @classmethod
    def run_node(cls, node, command, **kwargs):
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

    def __find_hostnames(self):
        tmp_file = tempfile.NamedTemporaryFile(dir='.')
        sleep_time = 1
        while True:  # we wait for the job to be launched, i.e., the oarfile to exist
            try:
                self.run_frontend('test -f %s' % self.oar_node_file)
            except invoke.UnexpectedExit:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time*2, 60)
            else:
                break
        self.get_frontend(self.oar_node_file, tmp_file.name)
        hostnames = set()
        with open(tmp_file.name) as node_file:
            for line in node_file:
                hostnames.add(line.strip())
        tmp_file.close()
        self.__hostnames = list(sorted(hostnames))

    @property
    def hostnames(self):
        try:
            return list(self.__hostnames)
        except AttributeError:
            self.__find_hostnames()
            return list(self.__hostnames)

    def kadeploy(self, env='debian9-x64-min'):
        # Wait for the oar_node_file to be available. Not required, just aesthetic.
        self.hostnames
        self.run_frontend('kadeploy3 -k -f %s -e %s' %
                          (self.oar_node_file, env))
        return self

    def __repr__(self):
        return '%s(%d)' % (self.__class__.__name__, self.jobid)

    @classmethod
    def oarsub(cls, connection, constraint, walltime, nb_nodes, immediate=True, script=None):
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        constraint = '%s/nodes=%d,walltime=%s' % (
            constraint, nb_nodes, walltime)
        cmd = 'oarsub -t deploy -l "%s"' % constraint
        if immediate:
            cmd += ' -r "%s"' % date
        if script:
            assert not immediate
            cmd += " '%s'" % script
        result = cls.__generic_run(
            connection, 'frontend', cmd, hide_output=False)
        regex = re.compile('OAR_JOB_ID=(\d+)')
        jobid = int(regex.search(result.stdout).groups()[0])
        return cls(jobid, connection=connection)

    @classmethod
    def oarsub_cluster(cls, site, username, clusters, walltime, nb_nodes, immediate=True, script=None):
        connection = cls.g5k_connection(site, username)
        clusters = ["'%s'" % clus for clus in clusters]
        constraint = "{cluster in (%s)}" % ', '.join(clusters)
        return cls.oarsub(connection, constraint, walltime, nb_nodes, immediate=immediate, script=script)

    @classmethod
    def oarsub_hostnames(cls, site, username, hostnames, walltime, nb_nodes=None, immediate=True, script=None):
        def expandg5k(host, site):
            if 'grid5000' not in host:
                host = '%s.%s.grid5000.fr' % (host, site)
            return host
        connection = cls.g5k_connection(site, username)
        hostnames = ["'%s'" % expandg5k(host, site) for host in hostnames]
        constraint = "{network_address in (%s)}" % ', '.join(hostnames)
        if nb_nodes is None:
            nb_nodes = len(hostnames)
        return cls.oarsub(connection, constraint, walltime, nb_nodes, immediate=immediate, script=script)

    @classmethod
    def g5k_connection(cls, site, username):
        if 'grid5000' in socket.getfqdn():  # already inside G5K, no need for a gateway
            return fabric.Connection(site, user=username)
        else:
            gateway = fabric.Connection('access.grid5000.fr', user=username)
            return fabric.Connection(site, user=username, gateway=gateway)

    @property
    def nodes(self):
        try:
            return self.__nodes
        except AttributeError:
            connections = [fabric.Connection(
                host, user='root', gateway=self.connection) for host in self.hostnames]
            connections = fabric.ThreadingGroup.from_connections(connections)
            self.__nodes = connections
            self.run_nodes('hostname')  # openning all the connections
            return self.__nodes

    def apt_install(self, *packages):
        self.run_nodes('apt update && apt upgrade -y')
        cmd = 'apt install -y %s' % ' '.join(packages)
        self.run_nodes(cmd)
        return self

    def __add_raw_information(self, archive_name, filename, command):
        self.run_nodes(command, hide_output=False)
        origin = self.nodes[0]
        self.run_node(origin, 'cp %s information/%s' % (filename, origin.host))
        for host in self.hostnames:
            if host == origin.host:
                continue
            self.run_node(origin, 'scp %s:%s information/%s' % (host, filename, host))

    def add_raw_information(self, archive_name):
        origin = self.nodes[0]
        for host in self.hostnames:
            self.run_node(origin, 'mkdir -p information/%s' % host)
        commands_with_files = {
                    'cpuinfo.txt': 'cp /proc/cpuinfo cpuinfo.txt',
                    'environment.txt': 'env > environment.txt',
                    'topology.xml': 'lstopo topology.xml',
                    'topology.pdf': 'lstopo topology.pdf',
                    'lspci.txt': 'lspci -v > lspci.txt'
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
        result['site'] = self.connection.host
        result['jobid'] = self.jobid
        result['command'] = ' '.join(sys.argv)
        return result


def mpi_install(job):
    logger.info(str(job))
    logger.info('Nodes: %s and %s' % tuple(job.hostnames))
    time.sleep(5)
    job.kadeploy().apt_install(
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
        'pciutils'
    )
    job.run_nodes(
        'git clone https://gitlab.inria.fr/simgrid/platform-calibration.git')
    job.run_nodes('cd platform-calibration/src/calibration && make')
    return job


def send_key(job):
    origin = job.nodes[0]
    target = job.nodes[1]
    job.run_node(origin, 'ssh-keygen -b 2048 -t rsa -f .ssh/id_rsa -q -N ""')
    tmp_file = tempfile.NamedTemporaryFile(dir='.')
    job.get(origin, '.ssh/id_rsa.pub', tmp_file.name)
    job.put(target, tmp_file.name, '/tmp/id_rsa.pub')
    tmp_file.close()
    job.run_node(
        target, 'cat /tmp/id_rsa.pub >> .ssh/authorized_keys', hide_output=False)
    job.run_node(
        origin, 'ssh -o "StrictHostKeyChecking no" %s hostname' % target.host)
    short_target = target.host[:target.host.find('.')]
    job.run_node(
        origin, 'ssh -o "StrictHostKeyChecking no" %s hostname' % short_target)


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
    path = 'platform-calibration/src/calibration'
    node_exp_filename = 'exp.xml'
    job.put_nodes(tmp_file.name, path + '/' + node_exp_filename)
    tmp_file.close()
    job.run_nodes('mkdir -p %s' % (path + '/exp'))
    with origin.cd(path):
        host = ','.join([node.host for node in job.nodes])
        logger.info('[%s] cd %s' % (origin.host, path))
        start_date = datetime.datetime.now()
        job.run_node(origin, 'mpirun --allow-run-as-root -np 2 -host %s ./calibrate -f %s' %
                     (host, node_exp_filename))
        end_date = datetime.datetime.now()
        job.run_node(origin, 'zip -r exp.zip exp')
        archive_name = '%s-%s_%s_%d.zip' % (remove_g5k(origin.host), remove_g5k(target.host), datetime.date.today(),
                                            job.jobid)
        job.run_node(origin, 'mv exp.zip ~/%s' % archive_name)
        logger.info('[%s] cd ~' % origin.host)
    job.add_raw_information(archive_name)
    job.get(origin, archive_name, archive_name)
    tmp_file = tempfile.NamedTemporaryFile(dir='.')
    job_info = job.platform_information()
    job_info['start'] = start_date.isoformat()
    job_info['stop'] = end_date.isoformat()
    with open(tmp_file.name, 'w') as f:
        yaml.dump(job_info, f, default_flow_style=False)
    archive = zipfile.ZipFile(archive_name, 'a')
    archive.write(tmp_file.name, 'info.yaml')
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
    if hasattr(args, 'cluster'):
        job = Job.oarsub_cluster(site, user, clusters=[
                                 args.cluster], walltime=Time(minutes=15), nb_nodes=2)
    elif hasattr(args, 'nodes'):
        job = Job.oarsub_hostnames(
            site, user, hostnames=args.nodes, walltime=Time(minutes=15))
    else:
        connection = Job.g5k_connection(site, user)
        job = Job(args.jobid, connection)
    if check_nb_nodes:
        if len(job.hostnames) != 2:
            logger.error(
                'Wrong number of nodes for job: got %d, expected 2.' % len(job.hostnames))
            job.oardel()
            sys.exit()
    return job


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Automatic MPI calibration')
    parser.add_argument('site', choices=['lyon', 'rennes', 'nancy'],
                        help='Site for the experiment.')
    parser.add_argument('username', type=str,
                        help='Username to use for the experiment.')
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

# TODO
# Call the script with:
#   - <site> <cluster>
#   - <site> <node1> <node2>
#   - <site> <jobid>
# Call the script from the laptop or a frontend.
# Then, do another script that copy this script and do a 'oarsub "python fabfile.py jobid"'.
