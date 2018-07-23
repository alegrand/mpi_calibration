import tempfile
import os
import fabric
from fabfile import Job, Time, logger

HPL_DIR = '/tmp/hpl-2.2'


def install_packages(job):
    job.apt_install(
        'build-essential',
        'zip',
        'make',
        'git',
        'time',
        'hwloc',
        'pciutils',
        'cmake',
        'cpufrequtils',
        'linux-cpupower',
        'openmpi-bin',
        'libopenmpi-dev',
        'net-tools',
    )


def install_blas(job):
    job.nodes.run('wget https://github.com/xianyi/OpenBLAS/archive/v0.3.1.zip -O openblas.zip')
    job.nodes.run('unzip openblas.zip && mv OpenBLAS-* openblas')
    job.nodes.run('make -j 64', directory='openblas')
    job.nodes.run('make install PREFIX=/tmp', directory='openblas')


def install_hpl(job):
    job.nodes.run('wget http://www.netlib.org/benchmark/hpl/hpl-2.2.tar.gz')
    job.nodes.run('tar -xvf hpl-2.2.tar.gz')
    job.nodes.write_files(HPL_MAKEFILE, os.path.join(HPL_DIR, 'Make.Debian'))
    job.nodes.run('make startup arch=Debian', directory=HPL_DIR)
    job.nodes.run('LD_LIBRARY_PATH=/tmp/lib make -j 64 arch=Debian', directory=HPL_DIR)


def install(job):
    install_packages(job)
    install_blas(job)
    install_hpl(job)


def generate_hpl_file(*, size, block_size, proc_p, proc_q, pfact=0, rfact=0, bcast=0, depth=0, swap=2, mem_align=8):
    assert 0 < block_size <= size
    assert proc_p > 0
    assert proc_q > 0
    assert depth >= 0
    assert pfact in (0, 1, 2)
    assert rfact in (0, 1, 2)
    assert bcast in (0, 1, 2, 3, 4, 5)
    assert swap in (0, 1, 2)
    assert mem_align % 4 == 0
    return '''\
HPLinpack benchmark input file
Innovative Computing Laboratory, University of Tennessee
HPL.out         output file name (if any)
6               device out (6=stdout,7=stderr,file)
1               # of problems sizes (N)
{size}          # default: 29 30 34 35  Ns
1               # default: 1            # of NBs
{block_size}    # 1 2 3 4      NBs
0               PMAP process mapping (0=Row-,1=Column-major)
1               # of process grids (P x Q)
{proc_p}        Ps
{proc_q}        Qs
16.0            threshold
1               # of panel fact
{pfact}         PFACTs (0=left, 1=Crout, 2=Right)
1               # of recursive stopping criterium
2               NBMINs (>= 1)
1               # of panels in recursion
2               NDIVs
1               # of recursive panel fact.
{rfact}         RFACTs (0=left, 1=Crout, 2=Right)
1               # of broadcast
{bcast}         BCASTs (0=1rg,1=1rM,2=2rg,3=2rM,4=Lng,5=LnM)
1               # of lookahead depth
{depth}         DEPTHs (>=0)
{swap}          SWAP (0=bin-exch,1=long,2=mix)
{block_size}    swapping threshold
0               L1 in (0=transposed,1=no-transposed) form
0               U  in (0=transposed,1=no-transposed) form
1               Equilibration (0=no,1=yes)
{mem_align}     memory alignment in double (> 0)
'''.format(
            size=size,
            block_size=block_size,
            proc_p=proc_p,
            proc_q=proc_q,
            pfact=pfact,
            rfact=rfact,
            bcast=bcast,
            depth=depth,
            swap=swap,
            mem_align=mem_align,
    )


def setup_hpl(job, **kwargs):
    hpl_file = generate_hpl_file(**kwargs)
    job.nodes.write_files(hpl_file, os.path.join(HPL_DIR, 'bin/Debian/HPL.dat'))


def run_hpl(job):
    nb_cores = len(job.nodes.cores)
    nb_nodes = len(job.hostnames)
    hosts = ','.join(job.hostnames)
    cmd = 'mpirun --allow-run-as-root --bind-to none --timestamp-output -np %d -x OMP_NUM_THREADS=%d -H %s -x LD_LIBRARY_PATH=/tmp/lib ./xhpl' % (
        nb_nodes,
        nb_cores,
        hosts
    )
    return job.director.run_unique(cmd, hide_output=False, directory=HPL_DIR+'/bin/Debian')


def parse_hpl(stdout):
    lines = stdout.split('\n')
    for i, line in enumerate(lines):
        if 'Time' in line and 'Gflops' in line:
            result = lines[i + 2].split()
            result = float(result[-2]), float(result[-1])
        if 'FAILED' in line:
            residual = float(line.split()[-3])
            logger.warning('HPL test failed with residual %.2e (should be < 16).' % residual)
    return result


def run(job, **kwargs):
    setup_hpl(job, **kwargs)
    output = run_hpl(job)
    time, gflops = parse_hpl(output.stdout)
    return time, gflops, output


def estimate_peak(job, matrix_size=8192, nb_cores=None):
    nb_cores = nb_cores or len(job.nodes.cores)
    arg = ' '.join([str(matrix_size)]*6)
    cmd = 'OMP_NUM_THREADS=%d LD_LIBRARY_PATH=/tmp/lib ./dgemm_test %s ' % (nb_cores, arg)
    try:
        all_output = job.nodes.run(cmd, hide_output=False)
    except fabric.exceptions.GroupException:  # not installed yet
        job.nodes.run('wget https://raw.githubusercontent.com/Ezibenroc/m2_internship_scripts/master/cblas_tests/dgemm_test.c')
        job.nodes.run('LD_LIBRARY_PATH=/tmp/lib gcc -DUSE_OPENBLAS ./dgemm_test.c -fopenmp -I /tmp/include \
                /tmp/lib/libopenblas.so -O3 -o ./dgemm_test')
        all_output = job.nodes.run(cmd, hide_output=False)
    gflops = []
    for output in all_output.values():
        time = float(output.stdout)
        gflops.append(2*matrix_size**3/time * 1e-9)
    return sum(gflops)


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
    run(job, size=30000, block_size=128, proc_p=2, proc_q=2)
    job.oardel()

HPL_MAKEFILE = '''
# -*- makefile -*-
#
#  -- High Performance Computing Linpack Benchmark (HPL)
#     HPL - 2.0 - September 10, 2008
#     Antoine P. Petitet
#     University of Tennessee, Knoxville
#     Innovative Computing Laboratory
#     (C) Copyright 2000-2008 All Rights Reserved
#
#  -- Copyright notice and Licensing terms:
#
#  Redistribution  and  use in  source and binary forms, with or without
#  modification, are  permitted provided  that the following  conditions
#  are met:
#
#  1. Redistributions  of  source  code  must retain the above copyright
#  notice, this list of conditions and the following disclaimer.
#
#  2. Redistributions in binary form must reproduce  the above copyright
#  notice, this list of conditions,  and the following disclaimer in the
#  documentation and/or other materials provided with the distribution.
#
#  3. All  advertising  materials  mentioning  features  or  use of this
#  software must display the following acknowledgement:
#  This  product  includes  software  developed  at  the  University  of
#  Tennessee, Knoxville, Innovative Computing Laboratory.
#
#  4. The name of the  University,  the name of the  Laboratory,  or the
#  names  of  its  contributors  may  not  be used to endorse or promote
#  products  derived   from   this  software  without  specific  written
#  permission.
#
#  -- Disclaimer:
#
#  THIS  SOFTWARE  IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
#  ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,  INCLUDING,  BUT NOT
#  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
#  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE UNIVERSITY
#  OR  CONTRIBUTORS  BE  LIABLE FOR ANY  DIRECT,  INDIRECT,  INCIDENTAL,
#  SPECIAL,  EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES  (INCLUDING,  BUT NOT
#  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#  DATA OR PROFITS; OR BUSINESS INTERRUPTION)  HOWEVER CAUSED AND ON ANY
#  THEORY OF LIABILITY, WHETHER IN CONTRACT,  STRICT LIABILITY,  OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ######################################################################
#
# ----------------------------------------------------------------------
# - shell --------------------------------------------------------------
# ----------------------------------------------------------------------
#
SHELL        = /bin/sh
#
CD           = cd
CP           = cp -f
LN_S         = ln -f -s
MKDIR        = mkdir -p
RM           = /bin/rm -f
TOUCH        = touch
#
# ----------------------------------------------------------------------
# - Platform identifier ------------------------------------------------
# ----------------------------------------------------------------------
#
ARCH         = $(arch)
#
# ----------------------------------------------------------------------
# - HPL Directory Structure / HPL library ------------------------------
# ----------------------------------------------------------------------
#
TOPdir       = %s
INCdir       = $(TOPdir)/include
BINdir       = $(TOPdir)/bin/$(ARCH)
LIBdir       = $(TOPdir)/lib/$(ARCH)
#
HPLlib       = $(LIBdir)/libhpl.a
#
# ----------------------------------------------------------------------
# - MPI directories - library ------------------------------------------
# ----------------------------------------------------------------------
# MPinc tells the  C  compiler where to find the Message Passing library
# header files,  MPlib  is defined  to be the name of  the library to be
# used. The variable MPdir is only used for defining MPinc and MPlib.
#
MPdir        =
MPinc        =
MPlib        =
#
# ----------------------------------------------------------------------
# - Linear Algebra library (BLAS or VSIPL) -----------------------------
# ----------------------------------------------------------------------
# LAinc tells the  C  compiler where to find the Linear Algebra  library
# header files,  LAlib  is defined  to be the name of  the library to be
# used. The variable LAdir is only used for defining LAinc and LAlib.
#
LAdir        = /tmp/lib
LAinc        =
LAlib        = /tmp/lib/libopenblas.so
#
# ----------------------------------------------------------------------
# - F77 / C interface --------------------------------------------------
# ----------------------------------------------------------------------
# You can skip this section  if and only if  you are not planning to use
# a  BLAS  library featuring a Fortran 77 interface.  Otherwise,  it  is
# necessary  to  fill out the  F2CDEFS  variable  with  the  appropriate
# options.  **One and only one**  option should be chosen in **each** of
# the 3 following categories:
#
# 1) name space (How C calls a Fortran 77 routine)
#
# -DAdd_              : all lower case and a suffixed underscore  (Suns,
#                       Intel, ...),                           [default]
# -DNoChange          : all lower case (IBM RS6000),
# -DUpCase            : all upper case (Cray),
# -DAdd__             : the FORTRAN compiler in use is f2c.
#
# 2) C and Fortran 77 integer mapping
#
# -DF77_INTEGER=int   : Fortran 77 INTEGER is a C int,         [default]
# -DF77_INTEGER=long  : Fortran 77 INTEGER is a C long,
# -DF77_INTEGER=short : Fortran 77 INTEGER is a C short.
#
# 3) Fortran 77 string handling
#
# -DStringSunStyle    : The string address is passed at the string loca-
#                       tion on the stack, and the string length is then
#                       passed as  an  F77_INTEGER  after  all  explicit
#                       stack arguments,                       [default]
# -DStringStructPtr   : The address  of  a  structure  is  passed  by  a
#                       Fortran 77  string,  and the structure is of the
#                       form: struct {char *cp; F77_INTEGER len;},
# -DStringStructVal   : A structure is passed by value for each  Fortran
#                       77 string,  and  the  structure is  of the form:
#                       struct {char *cp; F77_INTEGER len;},
# -DStringCrayStyle   : Special option for  Cray  machines,  which  uses
#                       Cray  fcd  (fortran  character  descriptor)  for
#                       interoperation.
#
F2CDEFS      =
#
# ----------------------------------------------------------------------
# - HPL includes / libraries / specifics -------------------------------
# ----------------------------------------------------------------------
#
HPL_INCLUDES = -I$(INCdir) -I$(INCdir)/$(ARCH) $(LAinc) $(MPinc)
HPL_LIBS     = $(HPLlib) $(LAlib) $(MPlib) -lm
#
# - Compile time options -----------------------------------------------
#
# -DHPL_COPY_L           force the copy of the panel L before bcast;
# -DHPL_CALL_CBLAS       call the cblas interface;
# -DHPL_CALL_VSIPL       call the vsip  library;
# -DHPL_DETAILED_TIMING  enable detailed timers;
#
# By default HPL will:
#    *) not copy L before broadcast,
#    *) call the Fortran 77 BLAS interface
#    *) not display detailed timing information.
#
HPL_OPTS     = -DHPL_CALL_CBLAS -DHPL_NO_MPI_DATATYPE -DHPL_USE_GETTIMEOFDAY
#
# ----------------------------------------------------------------------
#
HPL_DEFS     = $(F2CDEFS) $(HPL_OPTS) $(HPL_INCLUDES)
#
# ----------------------------------------------------------------------
# - Compilers / linkers - Optimization flags ---------------------------
# ----------------------------------------------------------------------
#
CC           = mpicc
CCNOOPT      = $(HPL_DEFS)
CCFLAGS      = $(HPL_DEFS) -fomit-frame-pointer -O3 -funroll-loops -W -Wall $(shell dpkg-buildflags --get CFLAGS)
#
LINKER       = mpicc
LINKFLAGS    = $(CCFLAGS) $(shell dpkg-buildflags --get LDFLAGS)
#
ARCHIVER     = ar
ARFLAGS      = r
RANLIB       = echo
#
# ----------------------------------------------------------------------
''' % HPL_DIR
