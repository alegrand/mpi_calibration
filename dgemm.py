from fabfile import Job, Time, logger


def install(job):
    job.apt_install(
        'build-essential',
        'zip',
        'make',
        'git',
        'time',
        'hwloc',
        'pciutils',
        'cmake',
    )
    job.nodes.run('wget https://github.com/xianyi/OpenBLAS/archive/v0.3.1.zip -O openblas.zip')
    job.nodes.run('unzip openblas.zip && mv OpenBLAS-* openblas')
    job.nodes.run('make -j 64', directory='openblas')
    job.nodes.run('make install PREFIX=/tmp', directory='openblas')
    job.nodes.run('wget https://github.com/Ezibenroc/m2_internship_scripts/archive/master.zip -O scripts.zip')
    job.nodes.run('unzip scripts.zip && mv m2_internship* scripts')


def run_test(job):
    nb_nodes = 64
    job.nodes.run('LD_LIBRARY_PATH=/tmp/lib python3 runner.py --csv_file /tmp/result.csv --lib openblas --dgemm ' +
                  '--size_range 1,10000 -np %d' % nb_nodes, directory='/tmp/scripts/cblas_tests')


if __name__ == '__main__':
    job = Job.oarsub_cluster(site='grenoble',
                             username='tocornebize',
                             clusters=['dahu'],
                             walltime=Time(hours=2),
                             nb_nodes=1,
                             deploy=False,
                             queue='testing')
    logger.info(str(job))
    logger.info('Node: %s' % job.hostnames[0])
    install(job)
    run_test(job)
    job.nodes.get('/tmp/result_dgemm.csv', 'result.csv')
    job.oardel()
