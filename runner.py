import fabfile
import random
import itertools


random.seed(42)


def run_all(username, site, cluster, possible_node_id, nb_runs=None):
    possible_nodes = ['%s-%d' % (cluster, node_id) for node_id in possible_node_id]
    combinations = list(itertools.combinations(possible_nodes, 2))
    nb_runs = nb_runs or len(combinations)
    choices = random.sample(combinations, nb_runs)
    for i, (node1, node2) in enumerate(choices):
        job = 'foo'
        job = fabfile.Job.oarsub_hostnames(
            site='lyon',
            username='tocornebize',
            hostnames=[node1, node2],
            walltime=fabfile.Time(minutes=15),
            immediate=False,
            script='python3 fabfile.py lyon tocornebize jobid $OAR_JOB_ID')
        fabfile.logger.info('[%3d/%3d]\t %s: %s and %s' % (i+1, nb_runs, job, node1, node2))


run_all('tocornebize', 'lyon', 'nova', range(8))
