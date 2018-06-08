import re
import os
import datetime
import invoke
import fabric
import backoff
import logging
import colorlog

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s][%(levelname)s] - %(message)s'))
formatter = colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s][%(levelname)s] %(message_log_color)s%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    secondary_log_colors={
        'message': {
            'DEBGU': 'white',
            'INFO': 'white',
            'WARNING': 'white',
            'ERROR': 'white',
            'CRITICAL': 'white',
        }
    }
)
handler.setFormatter(formatter)
logger = colorlog.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class Job:
    auto_oardel = False

    def __init__(self, jobid, connection):
        self.jobid = jobid
        self.connection = connection

    def __del__(self):
        if self.auto_oardel:
            self.oardel()

    @staticmethod
    def __generic_run(connection, title, command, **kwargs):
        logger.info('[%s] %s' % (title, command))
        return connection.run(command, hide=True, **kwargs)

    def run_frontend(self, command, **kwargs):
        return self.__generic_run(self.connection, 'frontend', command, **kwargs)

    def run_nodes(self, command, **kwargs):
        return self.__generic_run(self.nodes, 'nodes', command, **kwargs)

    @classmethod
    def run_node(cls, command, node, **kwargs):
        return cls.__generic_run(node, node.host, command, **kwargs)

    def oardel(self):
        self.run_frontend('oardel %d' % self.jobid)

    @property
    def oar_node_file(self):
        return '/var/lib/oar/%d' % self.jobid

    @backoff.on_exception(backoff.expo, FileNotFoundError)
    def __find_hostnames(self):
        filename = os.path.join('/', 'tmp', 'oarfile')
        self.connection.get(self.oar_node_file, filename)
        hostnames = set()
        with open(filename) as node_file:
            for line in node_file:
                hostnames.add(line.strip())
        self.__hostnames = list(sorted(hostnames))

    @property
    def hostnames(self):
        try:
            return list(self.__hostnames)
        except AttributeError:
            self.__find_hostnames()
            return list(self.__hostnames)

    @backoff.on_exception(backoff.expo, invoke.UnexpectedExit)
    def kadeploy(self, env='debian9-x64-min'):
        self.hostnames  # Wait for the oar_node_file to be available. Not required, just aesthetic.
        self.run_frontend('kadeploy3 -k -f %s -e %s' % (self.oar_node_file, env))
        return self

    def __repr__(self):
        return '%s(%d)' % (self.__class__.__name__, self.jobid)

    @classmethod
    def oarsub(cls, connection, constraint, walltime, nb_nodes):
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        constraint = '%s/nodes=%d,walltime=%d' % (constraint, nb_nodes, walltime)
        cmd = 'oarsub -t deploy -l "%s" -r "%s"' % (constraint, date)
        result = cls.__generic_run(connection, 'frontend', cmd)
        regex = re.compile('OAR_JOB_ID=(\d+)')
        jobid = int(regex.search(result.stdout).groups()[0])
        return cls(jobid, connection=connection)

    @classmethod
    def oarsub_cluster(cls, site, username, clusters, walltime, nb_nodes):
        connection = cls.g5k_connection(site, username)
        clusters = ["'%s'" % clus for clus in clusters]
        constraint = "{cluster in (%s)}" % ', '.join(clusters)
        return cls.oarsub(connection, constraint, walltime, nb_nodes)

    @classmethod
    def oarsub_hostnames(cls, site, username, hostnames, walltime, nb_nodes=None):
        def expandg5k(host, site):
            if 'grid5000' not in host:
                host = '%s.%s.grid5000.fr' % (host, site)
            return host
        connection = cls.g5k_connection(site, username)
        hostnames = ["'%s'" % expandg5k(host, site) for host in hostnames]
        constraint = "{network_address in (%s)}" % ', '.join(hostnames)
        if nb_nodes is None:
            nb_nodes = len(hostnames)
        return cls.oarsub(connection, constraint, walltime, nb_nodes)

    @classmethod
    def g5k_connection(cls, site, username):
        gateway = fabric.Connection('access.grid5000.fr', user=username)
        return fabric.Connection(site, user=username, gateway=gateway)

    @property
    def nodes(self):
        try:
            return self.__nodes
        except AttributeError:
            connections = [fabric.Connection(host, user='root', gateway=self.connection) for host in self.hostnames]
            connections = fabric.ThreadingGroup.from_connections(connections)
            self.run_nodes('hostname')  # openning all the connections
            self.__nodes = connections
            return self.__nodes

    def apt_install(self, *packages):
        self.run_nodes('apt update && apt upgrade -y')
        cmd = 'apt install -y %s' % ' '.join(packages)
        self.run_nodes(cmd)
        return self


#    cmd = 'oarsub -t deploy -l "{cluster in ('nova')}/nodes=1,walltime=4" -r "$(date '+%Y-%m-%d %H:%M:%S')"

def mpi_install(host1, host2, site, username):
    job = Job.oarsub_hostnames(site, username, hostnames=[host1, host2], walltime=2)
    print(job)
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
    )
    job.run_nodes('git clone https://gitlab.inria.fr/simgrid/platform-calibration.git')
    job.run_nodes('cd platform-calibration/src/calibration && make')
    return job


def send_key(job):
    origin = job.nodes[0]
    target = job.nodes[1]
    origin.run('ssh-keygen -b 2048 -t rsa -f ~/.ssh/id_rsa -q -N ""', hide=job.hide, echo=True)
    origin.get('/root/.ssh/id_rsa.pub', '/tmp/id_rsa.pub')
    target.put('/tmp/id_rsa.pub', '/tmp/id_rsa.pub')
    target.run('cat /tmp/id_rsa.pub >> ~/.ssh/authorized_keys', hide=job.hide, echo=True)
    origin.run('ssh -o "StrictHostKeyChecking no" %s hostname' % target.host, hide=job.hide, echo=True)
