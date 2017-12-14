#/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Peng Chao
# Copyright:
# Date:
# Distributed under terms of the license.

import sys
import os
import getopt
from ConfigParser import SafeConfigParser


class CataractCommand(object):
    def __init__(self, configfile):
        self.config = SafeConfigParser()
        self.config.read(configfile)
        self.spark_params = ['spark-submit']
        self.cataract_params = []

    def read_config(self):
        self.set_spark()
        self.set_dependencies('jars')
        self.set_dependencies('files')
        self.set_cataract()

    def parse_cmd(self, argv):
        option_params = ['partition=', 'union=', 'save-partition=', 'jar=',
                         'deploy-mode=', 'executor-cores=', 'executor-memory=',
                         'num-executors=', 'driver-memory=']

        cataract_part = {}
        spark_part = {}
        files = {}

        try:
            opts, argv = getopt.getopt(argv, "t:c:j:", option_params)
        except getopt.GetoptError as e:
            print("Usage: %s -i 2016082015 -o /user/hive/external/test/ -p partition_nums" % sys.argv[0])
            sys.exit(2)

        for opt, arg in opts:
            if opt == '-t':
                input_time = arg
            elif opt == '-o':
                hive_path = arg
            elif opt == '-p':
                partitions = int(arg)

    def set_spark(self):
        env = dict(self.config.items('spark'))
        self.set_spark_param(env, 'master', 'local[4]')
        self.set_spark_param(env, 'deploy-mode', None)
        self.set_spark_param(env, 'name', None)

        self.set_spark_param(env, 'executor-cores', None)
        self.set_spark_param(env, 'executor-memory', '2g')
        self.set_spark_param(env, 'num-executors', '5')
        self.set_spark_param(env, 'driver-memory', '4g')

        self.cataract_params.append(env.get('jar', None))
        self.cataract_params.append(env.get('class', 'com.guazi.cataract.Cataract'))

    def set_dependencies(self, part):
        values = []
        for f in self.read_values(part):
            values.append(f)

        if len(values) != 0:
            self.spark_params.append("--" + part)
            self.spark_params.append(','.join(values))

    def set_cataract(self):
        env = dict(self.config.items('cataract'))
        for k, v in env.items():
            if v == '':
                continue
            self.cataract_params.append(k + ":" + v)

    def read_values(self, section):
        return [x[1] for x in self.config.items(section)]

    def set_spark_param(self, env, key, default):
        # default is None: don't add this key
        dv = env.get(key, default)
        if dv is not None or default is not None:
            self.spark_params.append('--' + key)
            self.spark_params.append(dv)

    def get_command(self):
        cmds = []
        cmds.extend(self.spark_params)
        cmds.extend(self.cataract_params)
        return ' '.join(cmds)


if __name__ == '__main__':
    cmd = CataractCommand(sys.argv[1])
    cmd.read_config()
    cmd_str = cmd.get_command()
    print cmd_str
    os.system(cmd_str)
