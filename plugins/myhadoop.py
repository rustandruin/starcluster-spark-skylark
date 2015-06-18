from starcluster import threadpool
from starcluster import clustersetup
from starcluster.logger import log

import posixpath

# Installs Hadoop 2.x a la
# http://codesfusion.blogspot.com/2013/10/setup-hadoop-2x-220-on-ubuntu.html
# https://raseshmori.wordpress.com/2012/10/14/install-hadoop-nextgen-yarn-multi-node-cluster/
# http://khangaonkar.blogspot.com/2014/02/hadoop-2x-yarn-cluster-setup-tutorial.html

user_env_templ = """\
export HADOOP_HOME=%(hadoop_home)s
export HADOOP_PREFIX=%(hadoop_home)s
export PATH=$PATH:$HADOOP_PREFIX/bin
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
"""

core_site_templ = """\
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://%(master)s:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>%(hadoop_tmpdir)s</value>
  </property>
</configuration>
"""

hdfs_site_templ = """\
<?xml version="1.0" encoding="UTF-8"?>
 <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
 <configuration>
   <property>
     <name>dfs.replication</name>
     <value>%(replication)d</value>
   </property>
   <property>
     <name>dfs.permissions</name>
     <value>false</value>
   </property>
   <property>
     <name>dfs.namenode.name.dir</name>
     <value>file:/mnt/hdfs/%(user)s/namenode</value>
   </property>
   <property>
     <name>dfs.datanode.data.dir</name>
     <value>file:/mnt/hdfs/%(user)s/datanode</value>
   </property>
 </configuration>
 """

mapred_site_templ = """\
<?xml version="1.0"?>
<configuration>
 <property>
   <name>mapreduce.framework.name</name>
   <value>yarn</value>
 </property>
</configuration>
"""

yarn_site_templ = """\
<?xml version="1.0"?>
 <configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>%(master)s:8025</value>
  </property>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>%(master)s:8030</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>%(master)s:8040</value>
  </property>
 </configuration>
 """
 
class HadoopInstaller(clustersetup.ClusterSetup):
    """
    Configures Hadoop according to 
    https://raseshmori.wordpress.com/2012/10/14/install-hadoop-nextgen-yarn-multi-node-cluster/
    Installs from source if necessary
    """

    def __init__(self, hadoop_tmpdir='/mnt/hadoop'):
        self.hadoop_tmpdir = hadoop_tmpdir
        self.hadoop_home = '/usr/local/hadoop'
        self.hadoop_conf = self.hadoop_home + '/etc/hadoop'
        self._pool = None
        self.ubuntu_javas = ['/usr/lib/jvm/java-7-openjdk-amd64']

    @property
    def pool(self):
        if self._pool is None:
            self._pool = threadpool.get_thread_pool(20, disable_threads=False)
        return self._pool

    def _get_java_home(self, node):
        for java in self.ubuntu_javas:
            if node.ssh.isdir(java):
                return java
            raise Exception("Can't find JAVA home")

    def _setup_hadoop_user(self, node, user):
        node.ssh.execute("gpasswd -a %s hadoop" % user)

    def _setup_hadoop_config(self, node):
        hadoop_config_file = posixpath.join(self.hadoop_home, 'libexec/hadoop-config.sh')
        node.ssh.execute('mv {0} {0}.old'.format(hadoop_config_file))
        fin = node.ssh.remote_file(hadoop_config_file + '.old', 'r')
        fout = node.ssh.remote_file(hadoop_config_file, 'w')
        fout.write('export JAVA_HOME=%s\n'% self._get_java_home(node))
        for line in fin:
            fout.write(line)
        fin.close()
        fout.close()

    def _setup_yarn_env(self, node, cfg):
        env_file = posixpath.join(self.hadoop_home, 'etc/hadoop/yarn-env.sh')
        node.ssh.execute('mv {0} {0}.old'.format(env_file))
        fout = node.ssh.remote_file(env_file, 'w')
        fin = node.ssh.remote_file(env_file + '.old', 'r')
        fout.write('export JAVA_HOME=%s\n' % self._get_java_home(node))
        for line in fin:
            fout.write(line)
        fin.close()
        fout.close()

    def _setup_core_site(self, node, cfg):
        fname = posixpath.join(self.hadoop_home, 'etc/hadoop/core-site.xml')
        fout = node.ssh.remote_file(fname, 'w')
        fout.write(core_site_templ % cfg)
        fout.close()

    def _setup_hdfs_site(self, node, cfg):
        fname = posixpath.join(self.hadoop_home, 'etc/hadoop/hdfs-site.xml')
        fout = node.ssh.remote_file(fname, 'w')
        fout.write(hdfs_site_templ % cfg)
        fout.close()

    def _setup_mapred_site(self, node, cfg):
        fname = posixpath.join(self.hadoop_home, 'etc/hadoop/mapred-site.xml')
        fout = node.ssh.remote_file(fname, 'w')
        fout.write(mapred_site_templ)
        fout.close()

    def _setup_yarn_site(self, node, cfg):
        fname = posixpath.join(self.hadoop_home, 'etc/hadoop/yarn-site.xml')
        fout = node.ssh.remote_file(fname, 'w')
        fout.write(yarn_site_templ % cfg)
        fout.close()

    def _setup_slaves(self, node, nodelist):
        fname = posixpath.join(self.hadoop_home, 'etc/hadoop/slaves')
        fout = node.ssh.remote_file(fname, 'w')
        for name in nodelist:
            fout.write(name + "\n")
        fout.close()
        
    def _create_hdfs(self, node, user, cfg):
        node.ssh.execute('mkdir -p %(hadoop_tmpdir)s' % cfg)
        node.ssh.execute('mkdir -p /mnt/hdfs/%(user)s/namenode' % cfg)
        node.ssh.execute('mkdir -p /mnt/hdfs/%(user)s/datanode' % cfg)
        node.ssh.execute('chown -R {0}:hadoop {1}'.format(user, cfg['hadoop_tmpdir']))
        node.ssh.execute('chown -R {0}:hadoop /mnt/hdfs/{0}/namenode'.format(user))
        node.ssh.execute('chown -R {0}:hadoop /mnt/hdfs/{0}/datanode'.format(user))

    def _format_namenode(self, master, user):
        master.ssh.execute(' '.join( 
            ['su -l %s -c " ' % user,
             'source /home/%s/sethadoopenv.sh; ' % user,
             posixpath.join(self.hadoop_home, 'bin/hdfs'),
             ' namenode -format -force "']))

    def _setup_user_env(self, node, user, cfg):
        env_file = posixpath.join('/home/%s' % user, 'sethadoopenv.sh')
        fout = node.ssh.remote_file(env_file, 'w')
        fout.write(user_env_templ % cfg)
        fout.close()

        env_file = posixpath.join('/home/%s' % user, '.bashrc')
        fout = node.ssh.remote_file(env_file, 'a')
        fout.write('source $HOME/sethadoopenv.sh\n')
        fout.close()

    def _write_hadoop_scripts(self, master, user):
        fname = '/home/starthadoop-%s.sh' % user
        fout = master.ssh.remote_file(fname, 'w')
        fout.write("""\
su -l {0} -c "\
source /home/{0}/sethadoopenv.sh;\
\$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode;\
\$HADOOP_HOME/sbin/hadoop-daemons.sh start datanode;\
\$HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager;\
\$HADOOP_HOME/sbin/yarn-daemons.sh start nodemanager;\
\$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver"
""".format(user))
        fout.close()
        master.ssh.execute('chmod +x ' + fname)

        fname = '/home/stophadoop-%s.sh' % user
        fout = master.ssh.remote_file(fname, 'w')
        fout.write("""\
su -l {0} -c "\
source /home/{0}/sethadoopenv.sh;\
\$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver\
\$HADOOP_HOME/sbin/yarn-daemons.sh stop nodemanager;\
\$HADOOP_HOME/sbin/yarn-daemon.sh stop resourcemanager;\
\$HADOOP_HOME/sbin/hadoop-daemons.sh stop datanode;\
\$HADOOP_HOME/sbin/hadoop-daemon.sh stop namenode;"
""".format(user))
        fout.close()
        master.ssh.execute('chmod +x ' + fname)

    def _open_ports(self, master):
        ports = [50070, 50030]
        ec2 = master.ec2
        for group in master.cluster_groups:
            for port in ports:
                has_perm = ec2.has_permission(group, 'tcp', port, port, '0.0.0.0/0')
                if not has_perm:
                    ec2.conn.authorize_security_group(group_id=group.id, ip_protocol='tcp', from_port=port, to_port=port, cidr_ip='0.0.0.0/0')

    def _configure_hadoop(self, master, nodes, user):
        log.info("Configuring Hadoop...")

        log.info("Adding user %s to hadoop group" % user)
        for node in nodes:
            self.pool.simple_job(self._setup_hadoop_user, (node, user), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        node_aliases = map(lambda n: n.alias, nodes)
        cfg = {'master':master.alias, 
               'user':user,
               'replication':2,
               'hadoop_home' : self.hadoop_home,
               'hadoop_tmpdir': posixpath.join(self.hadoop_tmpdir, 'hadoop-%s' % user)}

        log.info("Installing configuration templates...")
        for node in nodes:
            self.pool.simple_job(self._setup_hadoop_config, (node, ), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        for node in nodes:
            self.pool.simple_job(self._setup_yarn_env, (node, cfg), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        for node in nodes:
            self.pool.simple_job(self._setup_core_site, (node, cfg), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        for node in nodes:
            self.pool.simple_job(self._setup_hdfs_site, (node, cfg), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        for node in nodes:
            self.pool.simple_job(self._setup_mapred_site, (node, cfg), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        for node in nodes:
            self.pool.simple_job(self._setup_yarn_site, (node, cfg), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        log.info('Configuring slaves')
        for node in nodes:
            self.pool.simple_job(self._setup_slaves, (node, node_aliases), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        log.info("Setting up user environment")
        for node in nodes:
            self.pool.simple_job(self._setup_user_env, (node, user, cfg), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        log.info("Creating HDFS")
        for node in nodes:
            self.pool.simple_job(self._create_hdfs, (node, user, cfg), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        self._format_namenode(master, user)

#        unnecessary: use start-/stop-dfs.sh and start-/stop-yarn.sh
#        log.info("Writing launch/end scripts")
#        self._write_hadoop_scripts(master, user)

    def _hadoop_installedq(self, node):
        return node.ssh.path_exists(self.hadoop_home)

    def _create_hadoop_group(self, node):
        node.ssh.execute('groupadd hadoop')

    def _install_apts(self, node):
        apt_packages = ['maven', 'cmake', 'pkg-config', 'libssl-dev', 'snappy',
                        'libsnappy-dev', 'libbz2-dev', 'libjansson-dev',
                        'libfuse-dev']
        for pkg in apt_packages:
            node.apt_command('install %s' % pkg)
        node.apt_command('clean')

    def _install_protobuf(self, node):
        instructions = """\
wget http://protobuf.googlecode.com/files/protobuf-2.5.0.tar.gz;
tar zvxf protobuf-2.5.0.tar.gz;
cd protobuf-2.5.0;
./configure;
make;
make check;
sudo make install;
sudo ldconfig;
cd ~
rm -rf protobuf-2.5.0*
"""
        node.ssh.execute(instructions) 

    def _build_hadoop(self, node):
        instructions = """\
wget http://www.interior-dsgn.com/apache/hadoop/common/hadoop-2.5.2/hadoop-2.5.2-src.tar.gz
tar xzvf hadoop-2.5.2-src.tar.gz
cd hadoop-2.5.2-src
mvn clean
MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512M" mvn compile -Pnative -Drequire.snappy -Drequire.openssl
mvn package -Pdist,native -DskipTests
cp -r hadoop-dist/target/hadoop-2.5.2 {0}
chown -R :hadoop {0}
cd ~
rm -rf hadoop*
""" 
        node.ssh.execute(instructions.format(self.hadoop_home))

    def _chown_hadoop(self, node, user):
        node.ssh.execute("chown -R {0} {1}".format(user, self.hadoop_home) )

    def _install_hadoop(self, master, nodes, user):
        if self._hadoop_installedq(master):
            return

        log.info("Installing Hadoop...")

        log.info("Creating hadoop usergroup")
        for node in nodes:
            self.pool.simple_job(self._create_hadoop_group, (node,), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        log.info("Installing apt packages")
        for node in nodes:
            self.pool.simple_job(self._install_apts, (node,), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        
        log.info("Installing protobuf")
        for node in nodes:
            self.pool.simple_job(self._install_protobuf, (node,), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        log.info("Building Hadoop...")
        for node in nodes:
            self.pool.simple_job(self._build_hadoop, (node, ), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        log.info("Setting Hadoop owner to %s" % user)
        for node in nodes:
            self.pool.simple_job(self._chown_hadoop, (node, user), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        
    def run(self, nodes, master, user, shell, volumes):
        self._install_hadoop(master, nodes, user)
        self._configure_hadoop(master, nodes, user)
        self._open_ports(master)
        log.info("Job tracker status: http://%s:50030" % master.dns_name)
        log.info("Namenode status: http://%s:50070" % master.dns_name)

