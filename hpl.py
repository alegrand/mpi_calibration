from fabfile import Job, Time, logger


def install(job):
    job.kadeploy().apt_install(
        'build-essential',
        'zip',
        'make',
        'git',
        'time',
        'libopenmpi-dev',
        'openmpi-bin',
        'hwloc',
        'pciutils',
        'net-tools',
        'cmake',
        'libboost-all-dev',
        'libblas-dev',
    )
    job.run_nodes('wget https://github.com/simgrid/simgrid/archive/master.zip -O simgrid.zip')
    job.run_nodes('unzip simgrid.zip')
    job.run_nodes('mkdir build && cd build && cmake -Denable_documentation=OFF .. && make -j 32 && make install',
                  directory='/tmp/simgrid-master')
    job.run_nodes('wget https://github.com/Ezibenroc/hpl/archive/master.zip -O hpl.zip')
    job.run_nodes('unzip hpl.zip')
    job.run_nodes('sed -ri "s|TOPdir\s*=.+|TOPdir="`pwd`"|g" Make.SMPI && make startup arch=SMPI',
                  directory='/tmp/hpl-master')
    job.run_nodes('make SMPI_OPTS="-DSMPI_OPTIMIZATION -DSMPI_DGEMM_COEFFICIENT=2.445036e-10 -DSMPI_DTRSM_COEFFICIENT=1.259681e-10" arch=SMPI',
                  directory='/tmp/hpl-master')
    job.run_nodes('sysctl -w vm.overcommit_memory=1 && sysctl -w vm.max_map_count=2000000000')
    job.run_nodes('mkdir -p /root/huge && mount none /root/huge -t hugetlbfs -o rw,mode=0777 && echo 1 >> /proc/sys/vm/nr_hugepages',
                  hide_output=False)


def run_test(job):
    job.run_nodes('rm -rf HPL.dat && ln -s HPL.dat.144 HPL.dat', directory='/tmp/hpl-master/bin/SMPI')
    job.run_nodes('smpirun -wrapper /usr/bin/time --cfg=smpi/privatize-global-variables:dlopen --cfg=smpi/display-timing:yes --cfg=smpi/shared-malloc-blocksize:2097152 -hostfile hostnames-taurus-144-hpl -platform platform_taurus_hpl.xml --cfg=smpi/shared-malloc-hugepage:/root/huge -np 144 xhpl', directory='/tmp/hpl-master/bin/SMPI')


if __name__ == '__main__':
    job = Job.oarsub_cluster(site='lyon',
                             username='tocornebize',
                             clusters=['taurus', 'nova'],
                             walltime=Time(minutes=30),
                             nb_nodes=1,
                             )
    logger.info(str(job))
    logger.info('Node: %s' % job.hostnames[0])
    install(job)
    run_test(job)
