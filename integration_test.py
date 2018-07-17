import unittest
import tempfile
import random
import string
import hashlib
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

    def test_put_get(self):
        frontend = Job.g5k_connection(self.site, self.user)
        self.node = Nodes([frontend], name='foo', working_dir='/home/%s' % self.user)
        tmp_file = tempfile.NamedTemporaryFile(dir='.')
        with open(tmp_file.name, 'w') as f:
            f.write('hello, world!\n')
        filename = 'test_fabfile'
        self.node.put(tmp_file.name, filename)
        self.assert_run('4dca0fd5f424a31b03ab807cbae77eb32bf2d089eed1cee154b3afed458de0dc  %s' % filename,
                        'sha256sum %s' % filename)
        tmp_new = tempfile.NamedTemporaryFile(dir='.')
        self.node.get(filename, tmp_new.name)
        with open(tmp_new.name, 'r') as f:
            content = f.read()
        self.assertEqual(content, 'hello, world!\n')
        self.assert_run('', 'rm -f %s' % filename)

    def test_write_files(self):
        frontend = Job.g5k_connection(self.site, self.user)
        self.node = Nodes([frontend], name='foo', working_dir='/home/%s' % self.user)
        for size in [1, 10, 50, 100, 1000, 10000]:
            for filenames in [['test_fabfile'], ['test_fabfile%d' % i for i in range(10)]]:
                content = ''.join(random.choices(string.ascii_lowercase + '\n\t ', k=size))
                content_hash = hashlib.sha256(content.encode('ascii')).hexdigest()
                self.node.write_files(content, *filenames)
                for filename in filenames:
                    self.assert_run('%s  %s' % (content_hash, filename), 'sha256sum %s' % filename)
                self.assert_run('', 'rm -f %s' % ' '.join(filenames))


class TestJob(Util):

    def test_job(self):
        job = Job.oarsub_cluster(site=self.site,
                                 username=self.user,
                                 clusters=[self.cluster],
                                 walltime=Time(minutes=15),
                                 nb_nodes=self.nb_nodes,
                                 deploy=False,
                                 )
        result = job.frontend.run_unique('hostname -f', hide_output=False).stdout.strip()
        self.assertEqual(result, 'f%s.%s.grid5000.fr' % (self.site, self.site))
        hosts = job.hostnames
        self.assertEqual(len(set(hosts)), self.nb_nodes)
        for host in hosts:
            self.assertEqual(host[:len(self.cluster)], self.cluster)
        self.assertEqual(set(job.hostnames), set(job.nodes.hostnames))
        result = job.nodes.run('hostname -f', hide_output=False)
        for node, res in result.items():
            self.assertEqual(node.host, res.stdout.strip())
        result = job.nodes.run_unique('pwd', hide_output=False)
        self.assertEqual(result.stdout.strip(), '/tmp')


if __name__ == '__main__':
    unittest.main()
