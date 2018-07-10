import unittest
from unittest.mock import MagicMock, call, PropertyMock
import collections
import datetime
import fabric
import json
import random
import fabfile


def build_cmd(cmd):
    return 'cd /tmp && %s &> /dev/null' % cmd


def cmd_to_call(cmd):
    return call(build_cmd(cmd), hide=True)


class Util(unittest.TestCase):
    site = 'lyon'
    username = 'tocornebize'
    clusters = ['taurus', 'nova']
    walltime = fabfile.Time(minutes=15)
    nb_nodes = 5
    result_cls = collections.namedtuple('result', ['stdout', 'stderr'])
    oar_job_id = 1234
    result_oarsub = result_cls('some text\n ... OAR_JOB_ID=%d...\nsome other text' % oar_job_id, '')


class BasicTest(Util):
    def test_oarsub_cluster(self):
        fabric.Connection.run = MagicMock(return_value=self.result_oarsub)
        job = fabfile.Job.oarsub_cluster(self.site, self.username, self.clusters,
                                         self.walltime, self.nb_nodes, deploy=False)
        self.assertEqual(job.jobid, self.oar_job_id)
        cluster_str = ["'%s'" % clus for clus in self.clusters]
        cluster_str = ', '.join(cluster_str)
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        request = 'oarsub -t allow_classic_ssh -l "{cluster in (%s)}/nodes=%d,walltime=%s" -r "%s"' % (
                cluster_str, self.nb_nodes, self.walltime, now)
        fabric.Connection.run.assert_called_once_with(request, hide=True)

    def test_nodes(self):
        job = fabfile.Job(self.oar_job_id, connection=fabfile.Job.g5k_connection(self.site, self.username))
        expected_hosts = ['foo-%d' % i for i in range(self.nb_nodes)]
        random.shuffle(expected_hosts)
        json_output = json.dumps({str(self.oar_job_id): {'assigned_network_address': expected_hosts}})
        fabric.Connection.run = MagicMock(return_value=self.result_cls(stdout=json_output, stderr=''))
        hosts = job.hostnames
        self.assertEqual(list(sorted(expected_hosts)), hosts)
        fabric.Connection.run.assert_called_once_with('oarstat -fJ -j %d' % self.oar_job_id, hide=True)
        fabric.Connection.run.reset_mock()
        nodes = job.nodes
        self.assertEqual(len(nodes), self.nb_nodes)
        for node, hostname in zip(nodes, job.hostnames):
            self.assertIsInstance(node, fabric.Connection)
            self.assertEqual(node.host, hostname)
        calls = fabric.Connection.run.call_args_list
        self.assertEqual(len(calls), self.nb_nodes)
        for c in calls:
            self.assertEqual(c, cmd_to_call('hostname'))


class RunTest(Util):
    def setUp(self):
        expected_hosts = ['foo-%d' % i for i in range(self.nb_nodes)]
        connections = [fabric.Connection(host, user=self.username) for host in expected_hosts]
        connections = fabric.ThreadingGroup.from_connections(connections)
        for node in connections:
            node.run = MagicMock()
        fabfile.Job.nodes = PropertyMock(return_value=connections)
        self.job = fabfile.Job(self.oar_job_id, connection=fabfile.Job.g5k_connection(self.site, self.username))
        self.job.connection.run = MagicMock()

    def test_run_nodes(self):
        self.job.run_nodes('hello world')
        for node in self.job.nodes:
            node.run.assert_called_once_with(build_cmd('hello world'), hide=True)

    def test_run_frontend(self):
        self.job.run_frontend('foo bar')
        self.job.connection.run.assert_called_once_with('foo bar &> /dev/null', hide=True)


if __name__ == '__main__':
    unittest.main()
