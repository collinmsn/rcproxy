import os
import sys
import logging
import time
from redistrib import command
logging.getLogger().setLevel(logging.INFO)

PORTS = [
    7001,
    7002,
    7003,
]

REPLICAS = 3

cluster = "cluster"
tmpl_file = "redis.conf.tmpl"


def mkdir_p(dir):
    os.system("mkdir -p %s" % dir)


def start_servers():
    logging.info("start servers")
    tmpl = open(tmpl_file, 'r').read()
    for port in PORTS:
        for r in range(REPLICAS):
            instance_port = port + r * 100
            dir = "%s/%s" % (cluster, instance_port)
            mkdir_p(dir)
            with open(os.path.join(dir, "redis.conf"), 'w') as fp:
                fp.write(tmpl.format(PORT=instance_port))

            cmd = "cd %s; redis-server redis.conf" % dir
            os.system(cmd)

def start_cluster():
    logging.info("start cluster")
    time.sleep(2)
    servers = [('127.0.0.1', port) for port in PORTS]
    try:
        command.start_cluster_on_multi(servers)
    except Exception as e:
        logging.error(e)
    time.sleep(5)
    for port in PORTS:
        for r in range(1, REPLICAS):
            slave_port = port + r * 100
            logging.info("replicate: 127.0.0.1:%d <- 127.0.0.1:%d", port, slave_port)
            command.replicate("127.0.0.1", port, "127.0.0.1", slave_port)

if os.path.exists(cluster):
    start_servers()
    logging.warn("cluster dir has already exists")
    sys.exit(0)
else:
    start_servers()
    start_cluster()
logging.info("done")









