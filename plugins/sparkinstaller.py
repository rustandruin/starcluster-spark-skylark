# Installs Spark
# TODO: support adding and removing nodes 
# TODO: check for if spark already installed: if so, just 
# need to update the slaves file
# CAVEAT: as is, Spark will be broken if you change the number
# nodes without deleting spark and rerunning this setup plugin

from starcluster import clustersetup
from starcluster.logger import log

class SparkInstaller(clustersetup.DefaultClusterSetup):

    spark_home = '/opt/Spark'
    spark_source = 'http://mirror.metrocast.net/apache/spark/spark-1.3.1/spark-1.3.1.tgz'
    spark_directory = 'spark-1.3.1'
    spark_profile = '/etc/profile.d/spark.sh'

    def __init__(self, pythonpath = "", ldlibrarypath ="" ):
        super(SparkInstaller, self).__init__()
        self._pythonpath = pythonpath
        self._ldlibrarypath = ldlibrarypath

    def _isinstalledq(self, node):
        return node.ssh.path_exists(self.spark_home)

    def _build_spark(self, node):
        if not self._isinstalledq(node):
            log.info("...building on %s" % node.alias)
            instructions = [
                "wget -O spark.tgz %s" % self.spark_source,
                "tar xvf spark.tgz",
                "rm spark.tgz",
                "mv %s %s" % (self.spark_directory, self.spark_home),
                "cd %s" % self.spark_home,
                'export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"',
                "build/mvn -Phadoop-provided -Phadoop-2.4 -Pyarn -Dhadoop.version=2.4.0 -DskipTests clean package 2>&1 "
            ]
            node.ssh.execute(' && '.join(instructions))
            log.info("...done building on %s" % node.alias)

    def run(self, nodes, master, user, shell, volumes):
        log.info("Installing Spark")

        aliases = [n.alias for n in nodes]

        for node in nodes:
            self.pool.simple_job(self._build_spark, (node), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

        log.info("...writing conf/slaves file to all nodes")
        for node in nodes:
            slaves_conf = node.ssh.remote_file("%s/conf/slaves" % self.spark_home, 'w')
            slaves_conf.write('\n'.join(aliases) + '\n')
            slaves_conf.close()
            slaves_profile = node.ssh.remote_file("%s" % self.spark_profile, 'w')
            profile_settings = [
                "export SPARK_HOME=%s" % self.spark_home,
                "export PATH=$PATH:$SPARK_HOME/bin"
            ]
            slaves_profile.write('\n'.join(profile_settings))
            slaves_profile.close()

        log.info("...writing spark-env script to all nodes")
        for node in nodes:
            sparkenv_conf = node.ssh.remote_file("%s/conf/spark-env.sh" % self.spark_home, 'w')
            sparkenv_settings = [
                "#!/usr/bin/env bash",
                "export PYTHONPATH=$PYTHONPATH:{0}".format(self._pythonpath),
                "export LD_LIBRARY_PATH={0}".format(self._ldlibrarypath)
            ]
            sparkenv_conf.write('\n'.join(sparkenv_settings))

        log.info("...writing start/stopspark scripts to /home")
        startspark = [
            "#!/usr/bin/env bash",
            "MASTER=%s" % master.alias,
            'ssh $MASTER "(cd %s; ./sbin/start-master.sh)"' % self.spark_home,
            'ssh $MASTER "(cd %s; ./sbin/start-slaves.sh)"' % self.spark_home
        ]
        startspark_file = master.ssh.remote_file("/home/startspark.sh", "w")
        startspark_file.write("\n".join(startspark))
        startspark_file.close()

        stopspark = [
            "#!/usr/bin/env bash",
            "MASTER=%s" % master.alias,
            'ssh $MASTER "(cd %s; ./sbin/stop-slaves.sh)"' % self.spark_home,
            'ssh $MASTER "(cd %s; ./sbin/stop-master.sh)"' % self.spark_home
        ]
        stopspark_file = master.ssh.remote_file("/home/stopspark.sh", "w")
        stopspark_file.write("\n".join(stopspark))
        stopspark_file.close()

