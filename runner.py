import fabfile
import random
import itertools
import collections
import invoke


random.seed(42)


def run_all(username, site, cluster, possible_node_id, nb_runs=None, deploy=True):
    possible_nodes = ['%s-%d' % (cluster, node_id) for node_id in possible_node_id]
    combinations = list(itertools.combinations(possible_nodes, 2))
    nb_runs = nb_runs or len(combinations)
    choices = random.sample(combinations, nb_runs)
    failure_count = 0
    node_failure_count = collections.Counter()
    node_tentative_count = collections.Counter()
    deploy_str = '--deploy ' if deploy else ''
    script = 'python3 fabfile.py %s%s tocornebize jobid $OAR_JOB_ID' % (deploy_str, site)
    for i, (node1, node2) in enumerate(choices):
        node_tentative_count[node1] += 1
        node_tentative_count[node2] += 1
        try:
            job = fabfile.Job.oarsub_hostnames(
                site=site,
                username=username,
                hostnames=[node1, node2],
                walltime=fabfile.Time(minutes=15),
                immediate=False,
                script=script,
                deploy=deploy)
            fabfile.logger.info('[%3d/%3d]\t %s: %s and %s' % (i+1, nb_runs, job, node1, node2))
        except invoke.exceptions.UnexpectedExit:
            fabfile.logger.warning('oarsub failed for nodes %s and %s' % (node1, node2))
            failure_count += 1
            node_failure_count[node1] += 1
            node_failure_count[node2] += 1
    if failure_count > 0:
        fabfile.logger.warning('oarsub failed %d times' % failure_count)
        fabfile.logger.warning('failure count per node:')
    for node, nb_failures in sorted(node_failure_count.items()):
        fabfile.logger.warning('    node %s failed %d/%d times' % (node, nb_failures, node_tentative_count[node]))
    for node, nb_failures in sorted(node_failure_count.items()):
        if nb_failures == node_tentative_count[node] and nb_failures > 1:
            fabfile.logger.error('Node %s failed every time, consider removing it from your test.' % node)


run_all('tocornebize', 'lyon', 'taurus', range(1, 20), 10, deploy=False)
