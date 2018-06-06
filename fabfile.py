import re
import os
import datetime
import invoke
import fabric
import backoff


class Job:
    def __init__(self, jobid, connection=invoke, hide=True):
        self.jobid = jobid
        self.connection = connection
        self.hide = hide

    def __del__(self):
        self.connection.run('oardel %d' % self.jobid, hide=self.hide)

    @property
    def oar_node_file(self):
        return '/var/lib/oar/%d' % self.jobid

    @backoff.on_exception(backoff.expo, FileNotFoundError)
    def __find_nodes(self):
        try:
            filename = os.path.join('/', 'tmp', 'oarfile')
            self.connection.get(self.oar_node_file, filename)
        except AttributeError:
            filename = self.oar_node_file
        nodes = set()
        with open(filename) as node_file:
            for line in node_file:
                nodes.add(line.strip())
        self.__nodes = list(sorted(nodes))

    @property
    def nodes(self):
        try:
            return list(self.__nodes)
        except AttributeError:
            self.__find_nodes()
            return list(self.__nodes)

    @backoff.on_exception(backoff.expo, invoke.UnexpectedExit)
    def kadeploy(self, env='debian9-x64-min'):
        self.connection.run('kadeploy3 -k -f %s -e %s' % (self.oar_node_file, env), hide=self.hide)

    def __repr__(self):
        return '%s(%d)' % (self.__class__.__name__, self.jobid)

    @classmethod
    def oarsub(cls, constraint, nb_nodes, walltime, connection=invoke, hide=True):
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        constraint = '%s/nodes=%d,walltime=%d' % (constraint, nb_nodes, walltime)
        cmd = 'oarsub -t deploy -l "%s" -r "%s"' % (constraint, date)
        result = connection.run(cmd, hide=hide)
        regex = re.compile('OAR_JOB_ID=(\d+)')
        jobid = int(regex.search(result.stdout).groups()[0])
        return cls(jobid, connection=connection, hide=hide)

    @classmethod
    def g5k_connection(cls, site, username):
        gateway = fabric.Connection('access.grid5000.fr', user=username)
        return fabric.Connection(site, user=username, gateway=gateway)


#    cmd = 'oarsub -t deploy -l "{cluster in ('nova')}/nodes=1,walltime=4" -r "$(date '+%Y-%m-%d %H:%M:%S')"
