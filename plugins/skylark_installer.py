from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log
from itertools import chain
import os, inspect, time

# Assumes that the mpich2 installer has already been run to provide mpich2
#TODO: switch using Here docs
#TODO: support adding and removing nodes
# CAVEATS: how much free space need for this? Should start with an 8Gb disk for the total AMI, not counting actual free space you want to have at the end
# CAVEAT: ideally, should check if skylark is already installed, and if so just exit

class SkylarkInstaller(DefaultClusterSetup):
    SKYLARK_BASHRC = '/etc/profile.d/skylarksettings.sh'
    nproc = "6" # number of processors to use in making (should get output of system command nproc)

    # list the packages in order of install
    apt_packages = ['gfortran', 'git', 'cmake', 'libblas-dev', 'libblas3gf',
                    'liblapack-dev', 'liblapack3gf', 'libcr-dev', 'cython',
                    #'libboost-all-dev', 
                    'python-setuptools',
                    'python-matplotlib', 'ipython',
                    'ipython-notebook', 'python-pandas', 'python-sympy',
                    'python-nose', 'swig', 'swig-examples', 'swig2.0-examples',
                    'libhdf5-serial-dev', 'doxygen', 'graphviz',
                    'python-sphinx', 'dvipng', 'libfftw3-dev',
                    'libfftw3-mpi-dev', 'unzip', 'subversion', 'maven'] 

    easy_install_packages = ['mpi4py', 'h5py']

    libinstall_directory = '/usr/local'
    python_local_dist_directory = "/usr/local/lib/python2.7/dist-packages"

    installation_directories = {
        "SKYLARK_INSTALL_DIR" : "/opt/Skylark/install"
    }
    installation_directories["PYTHON_SITE_PACKAGES"] = installation_directories["SKYLARK_INSTALL_DIR"]
    installation_directories["PYTHONPATH"] = installation_directories["SKYLARK_INSTALL_DIR"] + "/lib/python2.7/site-packages:" + os.getenv("PYTHONPATH", "")
    installation_directories["LD_LIBRARY_PATH"] = os.getenv("LD_LIBRARY_PATH", "") + ":/usr/local/lib"

    # TODO: automate the selection of the JAVAHOME
    bashrc_directories = {
        "JAVA_HOME" : "/usr/lib/jvm/java-7-openjdk-amd64/",
        "LIBHDFS_ROOT" : "/usr/local/hadoop",
        "SKYLARK_SRC_DIR" : "/opt/Skylark/libskylark",
        "SKYLARK_BUILD_DIR" : "/opt/Skylark/build",
        "SKYLARK_INSTALL_DIR" : "/opt/Skylark/install",
        "PYTHON_SITE_PACKAGES" : installation_directories["PYTHON_SITE_PACKAGES"],
        "PYTHONPATH" : installation_directories["PYTHONPATH"],
        "LD_LIBRARY_PATH" : "/usr/local/hadoop/lib/native:"  
        + installation_directories["SKYLARK_INSTALL_DIR"] 
        + "/lib:/usr/local/lib:/usr/lib/x86_64-linux-gnu:" + os.getenv("LD_LIBRARY_PATH", ""),
    }
#        "LD_PRELOAD" : installation_directories["COMBBLAS_ROOT"] + "/libMPITypelib.so:"
#        + installation_directories["COMBBLAS_ROOT"] + "/libCommGridlib.so"}

    openblas_source = "http://github.com/xianyi/OpenBLAS/tarball/v0.2.8"
    openblas_directory = 'xiany-OpenBLAS-9c51cdf'
    boost_source = "http://sourceforge.net/projects/boost/files/boost/1.53.0/boost_1_53_0.tar.gz"
    boost_directory = "boost_1_53_0"
#    elemental_source = "https://github.com/elemental/Elemental/archive/0.86-rc1.zip"
#    elemental_directory = "Elemental-0.86-rc1"
    combblas_source = "http://gauss.cs.ucsb.edu/~aydin/CombBLAS_FILES/CombBLAS_beta_14_0.tgz"
    kdt_source = "http://sourceforge.net/projects/kdt/files/kdt-0.3.tar.gz"
    kdt_directory = "kdt-0.3"
    fftw_source = "http://www.fftw.org/fftw-3.3.3.tar.gz"
    fftw_directory = "fftw-3.3.3"
    random123_source = "http://www.thesalmons.org/john/random123/releases/1.08/Random123-1.08.tar.gz"
    random123_directory = "Random123-1.08"
    random123_includes = "Random123-1.08/include/Random123"
    spiral_source = "http://www.ece.cmu.edu/~spiral/software/spiral-wht-1.8.tgz"
    spiral_directory = "spiral-wht-1.8"
    hadoop_source = "http://ftp.wayne.edu/apache/hadoop/common/hadoop-2.7.0/hadoop-2.7.0-src.tar.gz"

    def __init__(self):
        super(SkylarkInstaller, self).__init__()
        log.debug("Installing Skylark")

    def _follow_instructions(self, instructions, node):
        node.ssh.execute(';'.join(instructions))

    def _installedq(self, node):
        return node.ssh.path_exists(self.SKYLARK_BASHRC)

    def _configure_bashrc(self, node):
        bashrc_addon = node.ssh.remote_file(self.SKYLARK_BASHRC, 'w')
        for envvar, val in self.bashrc_directories.iteritems():
            bashrc_addon.write("%s=%s\n" % (envvar, val))
            bashrc_addon.write("export %s\n" % envvar)
        # FIX: OVERWRITES
        bashrc = node.ssh.remote_file('.bashrc', 'a')
        bashrc.write('source ' + self.SKYLARK_BASHRC + '\n')

    def _install_apt_packages(self, node):
        log.info("\tUpdating apt information")
        # node.apt_command('update')
        log.info("\tInstalling a bunch of apt packages")
        for pkg in self.apt_packages:
            log.info("\t...%s" % pkg)
            node.apt_command('install %s' % pkg)
        node.apt_command('clean') # free up space: the install process eats up a lot of disk space

    def _install_easy_install_packages(self, node):
        log.info('\tInstalling easy_install packages')
        for pkg in self.easy_install_packages:
            log.info("\t...%s" % pkg)
            node.ssh.execute('easy_install %s' % pkg)

# Autodetection of chipset seems to fail on EC2
    def _install_openblas(self, node):
        log.info("\tInstalling OpenBLAS")
     #   instructions = [
     #       'wget -O OpenBLAS.tgz %s' % self.openblas_source,
     #       'tar xzvf OpenBLAS.tgz',
     #       'cd %s' % self.openblas_directory,
     #       'make -j4',
     #       'make PREFIX=%s install' % self.libinstall_directory,
     #       'cd ..'
     #   ]
     #   self._follow_instructions(instructions, node)
        node.apt_command('install libopenblas-dev')

    def _install_boost(self, node):
        log.info("\tInstalling Boost")
        instructions = [
            'wget -O boost.tgz %s' % self.boost_source,
            'tar xvfz boost.tgz',
            'cd %s' % self.boost_directory,
            # seems like this script always returns false regardless of actual success?
            './bootstrap.sh --with-libraries=mpi,python,random,serialization,program_options,system,filesystem',
            'echo "using mpi ;" >> project-config.jam',
            './b2 -j 6 link=static,shared',
            './b2 install',
            'cd ..',
            'rm -rf boost.tgz %s' % self.boost_directory
        ]
        self._follow_instructions(instructions, node)

    def _install_elemental(self, node):
        log.info("\tInstalling Elemental")
        instructions = [
            "git clone https://github.com/elemental/Elemental.git Elemental",
            "cd Elemental",
            "git checkout 4a16736e44b24ced2d0dd9d3f688ce2d149611ba",
            "git clone https://github.com/poulson/metis.git external/metis",
            "mkdir build",
            "cd build",
            'cmake -DEL_USE_64BIT_INTS=ON -DCMAKE_BUILD_TYPE=Release -DMATH_LIBS="-L/usr/lib:/usr/lib/lapack -llapack -lopenblas -lm" ..',
            "make -j %s" % self.nproc,
            "make install",
            "cd ../..",
            "rm -r Elemental"
        ]
        self._follow_instructions(instructions, node)

    def _install_combblas(self, node):
        log.info("\tInstalling CombBLAS")
        instructions = [
            "wget -O combblas.tgz %s" % self.combblas_source,
            "tar xvfz combblas.tgz",
            "rm combblas.tgz"
        ]
        self._follow_instructions(instructions, node)

        # Expects the combblas.patch file to be in the same directory as this source file
        patchfname = os.path.dirname(inspect.getsourcefile(SkylarkInstaller)) + '/combblas.patch'
        log.info(patchfname)
        node.ssh.put(patchfname, 'CombBLAS/combblas.patch')

        instructions = [
            "cd CombBLAS",
            "yes | git apply --ignore-space-change --ignore-whitespace combblas.patch",
            "rm combblas.patch",
            "cmake .",
            "make -j %s" % self.nproc,
            "cp *.so /usr/local/lib",
            "mkdir /usr/local/include/CombBLAS",
            "cp *.h /usr/local/include/CombBLAS",
            "cp *.cpp /usr/local/include/CombBLAS",
            "cp -R SequenceHeaps /usr/local/include/CombBLAS",
            "cp -R psort-1.0 /usr/local/include/CombBLAS",
            "cp -R graph500-1.2 /usr/local/include/CombBLAS",
            "cd ..",
            "rm -r CombBLAS"
        ]
        self._follow_instructions(instructions, node)

    def _install_kdt(self, node):
        log.info("\tInstalling KDT")
        instructions = [
            "wget -O kdt.tgz %s" % self.kdt_source,
            "tar xvfz kdt.tgz",
            "cd %s" % self.kdt_directory,
            "export CC=mpicxx",
            "export CXX=mpicxx",
            "python ./setup.py build",
            "python ./setup.py install",
            "cd ..",
            "rm -rf kdt.tgz %s" % self.kdt_directory
        ]
        self._follow_instructions(instructions, node)

    def _install_fftw(self, node):
        log.info("\tInstalling FFTW")
        instructions = [
            "wget -O fftw.tgz %s" % self.fftw_source,
            "tar xvfz fftw.tgz",
            "cd %s" % self.fftw_directory,
            "./configure --enable-shared",
            "make -j 6",
            "make install",
            "cd ..",
            "rm -rf fftw.tgz %s" % self.fftw_directory
        ]
        self._follow_instructions(instructions, node)

    def _install_random123(self, node):
        log.info("\tInstalling Random123")
        instructions = [
            "wget -O random123.tgz %s" % self.random123_source,
            "tar xvfz random123.tgz",
            "cp -r %s /usr/local/include" % self.random123_includes,
            "rm -rf random123.tgz %s" % self.random123_directory
        ]
        self._follow_instructions(instructions, node)

    def _install_spiral(self, node):
        log.info("\tInstalling Spiral")
        instructions = [
            "wget -O spiral.tgz %s" % self.spiral_source,
            "tar xzvf spiral.tgz",
            "cd %s" % self.spiral_directory,
            './configure CFLAGS="-fPIC -fopenmp" --enable-RAM=16000 --enable-DDL --enable-IL --enable-PARA=8',
            "make -j %s" % self.nproc,
            "make install",
            "cd ..",
            "rm -r spiral.tgz %s" % self.spiral_directory
        ]
        self._follow_instructions(instructions, node)

    def _install_skylark(self, node):
        log.info("\tInstalling Skylark")

        # Expects the combblas.patch file to be in the same directory as this source file
        patchfname = os.path.dirname(inspect.getsourcefile(SkylarkInstaller)) + '/find_fftw.patch'
        node.ssh.put(patchfname, '/home/find_fftw.patch')

        instructions = [
            "mkdir -p $SKYLARK_BUILD_DIR",
            "mkdir -p $SKYLARK_INSTALL_DIR",
            "yes | git clone https://github.com/xdata-skylark/libskylark.git $SKYLARK_SRC_DIR",
            "cd $SKYLARK_SRC_DIR",
            "git checkout development",
            "mv /home/find_fftw.patch .",
            "yes | git apply --ignore-space-change --ignore-whitespace find_fftw.patch",
            "rm find_fftw.patch",
            "cd $SKYLARK_BUILD_DIR",
            "CC=mpicc CXX=mpicxx cmake -DCMAKE_INSTALL_PREFIX=$SKYLARK_INSTALL_DIR" +
            " -DUSE_COMBBLAS=ON $SKYLARK_SRC_DIR",
            "make -j %s" % self.nproc,
            "make install",
            "make doc"
        ]
        self._follow_instructions(instructions, node)

    def _fix_hdf5serial(self, node):
        # The package libhdf5-serial-dev in Vivid installs in a stupid
        # directory that is so nonstandard h5py can't find it, so generate
        # appropriate symlinks

        AMD_KERNEL_DIR = '/usr/lib/x86_64-linux-gnu/hdf5/serial'
        INTEL_KERNEL_DIR = '/usr/lib/i386-linux-gnu/hdf5/serial'

        # os.path.exists and os.path.isdir don't return True when the path exists! so use shell scripting
        instructions = [
            'ln -s /usr/include/hdf5/serial/* /usr/include',
            "if [ -d %s ]; then ln -s %s/* /usr/lib; else ln -s %s/* /usr/lib; fi" % (AMD_KERNEL_DIR, AMD_KERNEL_DIR, INTEL_KERNEL_DIR)
        ]
        self._follow_instructions(instructions, node)

    def _doinstall(self, node):
        if not self._installedq(node):
            self._install_apt_packages(node)
            self._fix_hdf5serial(node)
            self._install_easy_install_packages(node)
            self._install_openblas(node)
            self._install_boost(node)
            self._install_elemental(node)
            self._install_combblas(node)
            self._install_kdt(node)
            self._install_fftw(node)
            self._install_random123(node)
            self._install_spiral(node)
            self._configure_bashrc(node)
            self._install_skylark(node)
 

    def run(self, nodes, master, user, user_shell, volumes):
        log.info("Installing Skylark")
        for node in nodes:
            self.pool.simple_job(self._doinstall, (node), jobid=node.alias)
        self.pool.wait(numtasks = len(nodes))
