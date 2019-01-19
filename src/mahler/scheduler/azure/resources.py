# -*- coding: utf-8 -*-
"""
:mod:`mahler.scheduler.azure.resources -- TODO
==============================================

.. module:: resources
    :platform: Unix
    :synopsis: TODO

TODO: Write long description

"""
import getpass
import logging
import os
import pprint
import subprocess

from mahler.core.resources import Resources


logger = logging.getLogger(__name__)


SUBMISSION_ROOT = os.environ['FLOW_SUBMISSION_DIR']

FLOW_OPTIONS_TEMPLATE = "mem=20000M;time=2:59:00;job-name={job_name}"

FLOW_TEMPLATE = "flow-submit {container} --config {file_path} --options {options}"

COMMAND_TEMPLATE = "CUDA_VISIBLE_DEVICES={gpu_id} mahler execute{container}{tags}{options}"

SUBMIT_COMMANDLINE_TEMPLATE = "{flow} launch {command}"


class AzureResources(Resources):
    """
    """

    def __init__(self, nodes, max_workers=100):
        """
        """
        self.nodes = nodes
        self.max_workers = max_workers

    def _available(self):
        """
        """
        command = 'squeue -h -r -o \'%j %N %P\' -u {user}'.format(user=getpass.getuser())
        out = subprocess.check_output(command.split(" "))
        out = str(out, encoding='utf-8')
        ressources = dict()
        for line in out.split("\n"):
            line = line.strip()
            if not line:
                continue

            name, node, partition = line.split(" ")

            # We only support jobs using a single GPU
            gpu_id = int(name.split("-")[1])
            gpu_usage = 0.5 if name.endswith('-half') else 1

            if partition not in resources:
                resources[partition] = dict()

            if node not in resources[partition]:
                resources[partition][node] = defaultdict(int)

            resources[partition][node][gpu_id] += gpu_usage

        logger.debug('Nodes availability')
        for partition, nodes in sorted(resources.items()):
            logging.debug('  {}'.format(partition))
            for node, gpus in nodes.items():
                logging.debug('    {}'.format(node))
                for gpu, usage in sorted(gpus.items()):
                    logging.debug('      {}: {}'.format(gpu, usage))

        availability = dict()
        total_available = 0
        for partition, nodes in self.nodes.items():
            availability[partition] = dict()
            for node, node_resources in nodes.items():
                availability[partition][node] = dict(gpu=dict())
                for gpu_id in node_resources['gpu']:
                    gpu_available = max(1 - resources[partition][node])
                    total_available += gpu_available
                    availability[partition][node]['gpu'][gpu_id] = gpu_available

        logging.debug('total: {}'.format(total_jobs))

        # return max(self.max_workers - total_jobs, 0)
        return total_available, availability

    def available(self):
        """
        """
        return self._available()[0]

    def submit(self, tasks, container=None, tags=tuple(), working_dir=None):
        """
        """
        for task in tasks:
            self.submit_task(task, container=container, tags=tags, working_dir=working_dir)

    def pick_gpu(self, gpu_usage, nodes_available):
        for partition, nodes in nodes_available.items():
            for node, gpus in nodes.items():
                for gpu, availability in gpus.items():
                    if availability >= gpu_usage:
                        return partition, node, gpu

        return None, None, None

    def _make_job_name(self, task, gpu_id):
        name = "gpu-{}".format(gpu_id)
        if task['resources']['gpu'] == 0.5:
            name += "-half"
        return name

    def submit_task(self, task, container=None, tags=tuple(), working_dir=None):
        total_available, nodes_available = self._available()
        logger.info('{} nodes available'.format(pprint.pformat(nodes_available)))
        if not total_available:
            return

        partition, node, gpu = self.pick_gpu(task['resources']['gpu'], nodes_available)
        if partition is None:
            return

        flow_options = FLOW_OPTIONS_TEMPLATE.format(job_name=self._make_job_name(task, gpu))

        submission_dir = os.path.join(SUBMISSION_ROOT, container)
        if not os.path.isdir(submission_dir):
            os.makedirs(submission_dir)

        if tags:
            file_name = ".".join(tag for tag in sorted(tags) if tag) + ".sh"
        else:
            file_name = "all.sh"

        file_path = os.path.join(submission_dir, file_name)

        flow_command = FLOW_TEMPLATE.format(
            container=container, file_path=file_path, options=flow_options)

        command = COMMAND_TEMPLATE.format(
            gpu_id=gpu,
            container=" --container " + container if container else "",
            tags=" --tags " + " ".join(tags) if tags else "",
            options=" --working-dir={}".format(working_dir) if working_dir else "")

        submit_command = SUBMIT_COMMANDLINE_TEMPLATE.format(flow=flow_command, command=command)

        print("Executing:")
        print(submit_command)
        out = subprocess.check_output(submit_command.split(" "))
        print("\nCommand output")
        print("------")
        print(str(out, encoding='utf-8'))
