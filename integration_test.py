import unittest
from fabfile import Job, Time, Nodes

Job.auto_oardel = True


class Util(unittest.TestCase):
    site = 'lyon'
    cluster = 'taurus'
    nb_nodes = 4
    user = 'tocornebize'


class TestBasic(Util):
    def test_frontend(self):
        frontend = Job.g5k_connection(self.site, self.user)
        result = frontend.run('hostname -f', hide=True).stdout.strip()
        self.assertEqual(result, 'f%s.%s.grid5000.fr' % (self.site, self.site))


class TestNodes(Util):
    def assert_run(self, expected_result, *args, **kwargs):
        kwargs['hide_output'] = False
        result = self.node.run(*args, **kwargs)
        self.assertEqual(len(result), 1)
        result = list(result.values())[0].stdout.strip()
        self.assertEqual(result, expected_result)

    def test_run(self):
        frontend = Job.g5k_connection(self.site, self.user)
        self.node = Nodes([frontend], name='foo', working_dir='/tmp')
        self.assert_run('f%s.%s.grid5000.fr' % (self.site, self.site), 'hostname -f')
        self.assert_run('', 'mkdir -p foo/bar')
        self.assert_run('/tmp/foo/bar', 'pwd', directory='foo/bar')
        directory = '/home/%s' % self.user
        self.assert_run(directory, 'pwd', directory=directory)


class TestJob(Util):

    def test_job(self):
        job = Job.oarsub_cluster(site=self.site,
                                 username=self.user,
                                 clusters=[self.cluster],
                                 walltime=Time(minutes=15),
                                 nb_nodes=self.nb_nodes,
                                 deploy=False,
                                 )
        result = job.run_frontend('hostname -f', hide_output=False).stdout.strip()
        self.assertEqual(result, 'f%s.%s.grid5000.fr' % (self.site, self.site))
        hosts = job.hostnames
        self.assertEqual(len(set(hosts)), self.nb_nodes)
        for host in hosts:
            self.assertEqual(host[:len(self.cluster)], self.cluster)
        self.assertEqual(set(job.hostnames), set([node.host for node in job.nodes]))
        result = job.run_nodes('hostname -f', hide_output=False)
        for node, res in result.items():
            self.assertEqual(node.host, res.stdout.strip())


if __name__ == '__main__':
    unittest.main()
