import re
import os
import datetime
import invoke
import fabric
import backoff


class Job:
    auto_oardel = False
    hide = False

    def __init__(self, jobid, connection):
        self.jobid = jobid
        self.connection = connection

    def __del__(self):
        if self.auto_oardel:
            self.oardel()

    def oardel(self):
        self.connection.run('oardel %d' % self.jobid, hide=self.hide, echo=True)

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
        self.connection.run('kadeploy3 -k -f %s -e %s' % (self.oar_node_file, env), hide=self.hide, echo=True)
        return self

    def __repr__(self):
        return '%s(%d)' % (self.__class__.__name__, self.jobid)

    @classmethod
    def oarsub(cls, connection, constraint, walltime, nb_nodes):
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        constraint = '%s/nodes=%d,walltime=%d' % (constraint, nb_nodes, walltime)
        cmd = 'oarsub -t deploy -l "%s" -r "%s"' % (constraint, date)
        result = connection.run(cmd, hide=cls.hide, echo=True)
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
            connections.run('hostname', hide=self.hide, echo=True)  # openning all the connections
            self.__nodes = connections
            return self.__nodes

    def apt_install(self, *packages):
        self.nodes.run('apt update && apt upgrade -y', hide=self.hide, echo=True)
        cmd = 'apt install -y %s' % ' '.join(packages)
        self.nodes.run(cmd, hide=self.hide, echo=True)
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
    job.nodes.run('git clone https://gitlab.inria.fr/simgrid/platform-calibration.git', hide=job.hide, echo=True)
    job.nodes.run('cd platform-calibration/src/calibration && make', hide=job.hide, echo=True)
    return job


def send_key(job):
    origin = job.nodes[0]
    target = job.nodes[1]
    origin.run('ssh-keygen -b 2048 -t rsa -f ~/.ssh/id_rsa -q -N ""', hide=job.hide, echo=True)
    origin.get('/root/.ssh/id_rsa.pub', '/tmp/id_rsa.pub')
    target.put('/tmp/id_rsa.pub', '/tmp/id_rsa.pub')
    target.run('cat /tmp/id_rsa.pub >> ~/.ssh/authorized_keys', hide=job.hide, echo=True)
    origin.run('ssh -o "StrictHostKeyChecking no" %s hostname' % target.host, hide=job.hide, echo=True)
