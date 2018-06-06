import re
import datetime
import invoke
import backoff


class Job:
    def __init__(self, jobid, hide=True):
        self.jobid = jobid
        self.hide = hide

    @property
    def oar_node_file(self):
        return '/var/lib/oar/%d' % self.jobid

    @backoff.on_exception(backoff.expo, FileNotFoundError)
    def __find_nodes(self):
        nodes = set()
        with open(self.oar_node_file) as node_file:
            for line in node_file:
                nodes.add(line.strip())
        self.__nodes = list(sorted(nodes))

    @property
    def nodes(self):
        try:
            return self.__nodes
        except AttributeError:
            self.__find_nodes()
            return self.__nodes

    @backoff.on_exception(backoff.expo, invoke.UnexpectedExit)
    def kadeploy(self, env='debian9-x64-min'):
        invoke.run('kadeploy3 -k -f %s -e %s' % (self.oar_node_file, env), hide=self.hide)

    def __repr__(self):
        return '%s(%d)' % (self.__class__.__name__, self.jobid)

    @classmethod
    def oarsub(cls, constraint, hide=True):
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cmd = 'oarsub -t deploy -l "%s" -r "%s"' % (constraint, date)
        result = invoke.run(cmd, hide=hide)
        regex = re.compile('OAR_JOB_ID=(\d+)')
        jobid = int(regex.search(result.stdout).groups()[0])
        return cls(jobid, hide=hide)

    def __del__(self):
        invoke.run('oardel %d' % self.jobid, hide=self.hide)


#    cmd = 'oarsub -t deploy -l "{cluster in ('nova')}/nodes=1,walltime=4" -r "$(date '+%Y-%m-%d %H:%M:%S')"
