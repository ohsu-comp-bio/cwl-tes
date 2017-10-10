from __future__ import absolute_import, print_function, unicode_literals

import logging
import os
import tempfile
import shutil
import tes
import threading
import time

from builtins import str
from cwltool.draft2tool import CommandLineTool
from cwltool.errors import WorkflowException, UnsupportedRequirement
from cwltool.mutation import MutationManager
from cwltool.pathmapper import PathMapper
from cwltool.process import cleanIntermediate, relocateOutputs
from cwltool.stdfsaccess import StdFsAccess
from cwltool.workflow import defaultMakeTool
from pprint import pformat
from schema_salad.ref_resolver import file_uri

log = logging.getLogger("tes-backend")


class TESWorkflow(object):

    def __init__(self, url, kwargs):
        self.threads = []
        self.kwargs = kwargs
        self.client = tes.HTTPClient(url)
        if kwargs.get("basedir") is not None:
            self.basedir = kwargs.get("basedir")
        else:
            self.basedir = os.getcwd()
        self.fs_access = StdFsAccess(self.basedir)

    def executor(self, tool, job_order, **kwargs):
        final_output = []
        final_status = []

        def output_callback(out, processStatus):
            final_status.append(processStatus)
            final_output.append(out)

        if "basedir" not in kwargs:
            raise WorkflowException("Must provide 'basedir' in kwargs")

        output_dirs = set()

        if kwargs.get("outdir"):
            finaloutdir = os.path.abspath(kwargs.get("outdir"))
        else:
            finaloutdir = None

        if kwargs.get("tmp_outdir_prefix"):
            kwargs["outdir"] = tempfile.mkdtemp(
                prefix=kwargs["tmp_outdir_prefix"]
            )
        else:
            kwargs["outdir"] = tempfile.mkdtemp()

        output_dirs.add(kwargs["outdir"])
        kwargs["mutation_manager"] = MutationManager()

        jobReqs = None
        if "cwl:requirements" in job_order:
            jobReqs = job_order["cwl:requirements"]
        elif ("cwl:defaults" in tool.metadata and
              "cwl:requirements" in tool.metadata["cwl:defaults"]):
            jobReqs = tool.metadata["cwl:defaults"]["cwl:requirements"]
        if jobReqs:
            for req in jobReqs:
                tool.requirements.append(req)

        if kwargs.get("default_container"):
            tool.requirements.insert(0, {
                "class": "DockerRequirement",
                "dockerPull": kwargs["default_container"]
            })

        jobs = tool.job(job_order, output_callback, **kwargs)
        try:
            for runnable in jobs:
                if runnable:
                    builder = kwargs.get("builder", None)
                    if builder is not None:
                        runnable.builder = builder
                    if runnable.outdir:
                        output_dirs.add(runnable.outdir)
                    runnable.run(**kwargs)
                else:
                    time.sleep(1)

        except WorkflowException as e:
            raise e
        except Exception as e:
            log.error("Got exception")
            raise WorkflowException(str(e))

        # wait for all processes to finish
        self.wait()

        if final_output and final_output[0] and finaloutdir:
            final_output[0] = relocateOutputs(
                final_output[0], finaloutdir,
                output_dirs, kwargs.get("move_outputs"),
                kwargs["make_fs_access"](""))

        if kwargs.get("rm_tmpdir"):
            cleanIntermediate(output_dirs)

        if final_output and final_status:
            return (final_output[0], final_status[0])
        else:
            return (None, "permanentFail")

    def make_exec_tool(self, spec, **kwargs):
        return TESCommandLineTool(
            spec, self, fs_access=self.fs_access, **kwargs
        )

    def make_tool(self, spec, **kwargs):
        if "class" in spec and spec["class"] == "CommandLineTool":
            return self.make_exec_tool(spec, **kwargs)
        else:
            return defaultMakeTool(spec, **kwargs)

    def add_thread(self, thread):
        self.threads.append(thread)

    def wait(self):
        while True:
            if all([not t.is_alive() for t in self.threads]):
                break
        for t in self.threads:
            t.join()


class TESCommandLineTool(CommandLineTool):

    def __init__(self, spec, tes_workflow, fs_access, **kwargs):
        super(TESCommandLineTool, self).__init__(spec, **kwargs)
        self.spec = spec
        self.tes_workflow = tes_workflow
        self.fs_access = fs_access

    def makeJobRunner(self, use_container=True, **kwargs):
        return TESTask(self.spec, self.tes_workflow, self.fs_access)

    def makePathMapper(self, reffiles, stagedir, **kwargs):
        return PathMapper(reffiles, kwargs["basedir"], stagedir)


class TESTask(object):

    def __init__(self, spec, tes_workflow, fs_access):
        self.spec = spec
        self.tes_workflow = tes_workflow
        self.fs_access = fs_access

        self.outputs = None
        self.docker_workdir = "/var/spool/cwl"
        self.inplace_update = False

    def find_docker_requirement(self):
        default = "python:2.7"
        container = default
        if self.tes_workflow.kwargs["default_container"]:
            container = self.tes_workflow.kwargs["default_container"]

        reqs = self.spec.get("requirements", []) + self.spec.get("hints", [])
        for i in reqs:
            if i.get("class", "NA") == "DockerRequirement":
                container = i.get(
                    "dockerPull",
                    i.get("dockerImageId", default)
                )
        return container

    def create_input_parameter(self, name, d):
        if "contents" in d:
            return tes.Input(
                name=name,
                description="cwl_input:%s" % (name),
                path=d["path"],
                content=d["contents"],
                type=d["class"].upper()
            )
        else:
            return tes.Input(
                name=name,
                description="cwl_input:%s" % (name),
                url=d["location"],
                path=d["path"],
                type=d["class"].upper()
            )

    def parse_job_order(self, k, v, inputs):
        if isinstance(v, dict):
            if all([i in v for i in ["location", "path", "class"]]):
                inputs.append(self.create_input_parameter(k, v))

                if "secondaryFiles" in v:
                    for f in v["secondaryFiles"]:
                        self.parse_job_order(f["basename"], f, inputs)

            else:
                for sk, sv in v.items():
                    if isinstance(sv, dict):
                        self.parse_job_order(sk, sv, inputs)

                    else:
                        break

        elif isinstance(v, list):
            for i in range(len(v)):
                if isinstance(v[i], dict):
                    self.parse_job_order("%s[%s]" % (k, i), v[i], inputs)

                else:
                    break

        return inputs

    def parse_listing(self, listing, inputs):
        for item in listing:

            if "writable" in item:
                raise UnsupportedRequirement(
                    "The TES spec does not allow for writable inputs"
                    )

            if "contents" in item:
                loc = self.fs_access.join(self.tmpdir, item["basename"])
                with self.fs_access.open(loc, "wb") as gen:
                    gen.write(item["contents"])
            else:
                loc = item["location"]

            parameter = tes.Input(
                name=item["basename"],
                description="InitialWorkDirRequirement:cwl_input:%s" % (
                    item["basename"]
                ),
                url=file_uri(loc),
                path=self.fs_access.join(
                    self.docker_workdir, item["basename"]
                ),
                type=item["class"].upper()
                )
            inputs.append(parameter)

        return inputs

    def collect_input_parameters(self):
        inputs = []

        # find all primary and secondary input files
        for k, v in self.joborder.items():
            self.parse_job_order(k, v, inputs)

        # manage InitialWorkDirRequirement
        self.parse_listing(self.generatefiles["listing"], inputs)

        return inputs

    def create_task_msg(self):
        input_parameters = self.collect_input_parameters()
        output_parameters = []

        if self.stdout is not None:
            parameter = tes.Output(
                name="stdout",
                url=self.output2url(self.stdout),
                path=self.output2path(self.stdout)
            )
            output_parameters.append(parameter)

        if self.stderr is not None:
            parameter = tes.Output(
               name="stderr",
               url=self.output2url(self.stderr),
               path=self.output2path(self.stderr)
            )
            output_parameters.append(parameter)

        output_parameters.append(
            tes.Output(
                name="workdir",
                url=self.output2url(""),
                path=self.docker_workdir,
                type="DIRECTORY"
            )
        )

        container = self.find_docker_requirement()

        cpus = None
        ram = None
        disk = None
        for i in self.requirements:
            if i.get("class", "NA") == "ResourceRequirement":
                cpus = i.get("coresMin", i.get("coresMax", None))
                ram = i.get("ramMin", i.get("ramMax", None))
                ram = ram / 953.674 if ram is not None else None
                disk = i.get("outdirMin", i.get("outdirMax", None))
                disk = disk / 953.674 if disk is not None else None
            elif i.get("class", "NA") == "DockerRequirement":
                if i.get("dockerOutputDirectory", None) is not None:
                    output_parameters.append(
                        tes.Output(
                            name="dockerOutputDirectory",
                            url=self.output2url(""),
                            path=i.get("dockerOutputDirectory"),
                            type="DIRECTORY"
                        )
                    )

        create_body = tes.Task(
            name=self.name,
            description=self.spec.get("doc", ""),
            executors=[
                tes.Executor(
                    command=self.command_line,
                    image=container,
                    workdir=self.docker_workdir,
                    stdout=self.output2path(self.stdout),
                    stderr=self.output2path(self.stderr),
                    stdin=self.stdin,
                    env=self.environment
                )
            ],
            inputs=input_parameters,
            outputs=output_parameters,
            resources=tes.Resources(
                cpu_cores=cpus,
                ram_gb=ram,
                disk_gb=disk
            ),
            tags={"CWLDocumentId": self.spec.get("id")}
        )

        return create_body

    def run(self, pull_image=True, rm_container=True, rm_tmpdir=True,
            move_outputs="move", **kwargs):
        # useful for debugging
        log.debug(
            "[job %s] self.__dict__ in run() ----------------------" %
            (self.name)
        )
        log.debug(pformat(self.__dict__))

        task = self.create_task_msg()

        log.info(
            "[job %s] CREATED TASK MSG----------------------" %
            (self.name)
        )
        log.info(pformat(task))

        try:
            task_id = self.tes_workflow.client.create_task(task)
            log.info(
                "[job %s] SUBMITTED TASK ----------------------" %
                (self.name)
            )
            log.info("[job %s] task id: %s " % (self.name, task_id))
        except Exception as e:
            log.error(
                "[job %s] Failed to submit task to TES service:\n%s" %
                (self.name, e)
            )
            return WorkflowException(e)

        def callback():
            try:
                outputs = self.collect_outputs(self.outdir)
                cleaned_outputs = {}
                for k, v in outputs.items():
                    if isinstance(k, bytes):
                        k = k.decode("utf8")
                    if isinstance(v, bytes):
                        v = v.decode("utf8")
                    cleaned_outputs[k] = v
                self.outputs = cleaned_outputs
                self.output_callback(self.outputs, "success")
            except WorkflowException as e:
                log.error("[job %s] job error:\n%s" % (self.name, e))
                self.output_callback({}, "permanentFail")
            except Exception as e:
                log.error("[job %s] job error:\n%s" % (self.name, e))
                self.output_callback({}, "permanentFail")
            finally:
                if self.outputs is not None:
                    log.info(
                        "[job %s] OUTPUTS ------------------" %
                        (self.name)
                    )
                    log.info(pformat(self.outputs))
                self.cleanup(rm_tmpdir)

        poll = TESTaskPollThread(
            jobname=self.name,
            taskID=task_id,
            client=self.tes_workflow.client,
            callback=callback
        )

        self.tes_workflow.add_thread(poll)
        poll.start()

    def cleanup(self, rm_tmpdir):
        log.debug(
            "[job %s] STARTING CLEAN UP ------------------" %
            (self.name)
        )
        if self.stagedir and os.path.exists(self.stagedir):
            log.debug(
                "[job %s] Removing input staging directory %s" %
                (self.name, self.stagedir)
            )
            shutil.rmtree(self.stagedir, True)

        if rm_tmpdir:
            log.debug(
                "[job %s] Removing temporary directory %s" %
                (self.name, self.tmpdir)
            )
            shutil.rmtree(self.tmpdir, True)

    def output2url(self, path):
        if path is not None:
            return file_uri(
                self.fs_access.join(self.outdir, os.path.basename(path))
            )
        return None

    def output2path(self, path):
        if path is not None:
            return self.fs_access.join(self.docker_workdir, path)
        return None


class TESTaskPollThread(threading.Thread):

    def __init__(self, jobname, taskID, client, callback, poll_interval=1,
                 poll_retries=10):
        super(TESTaskPollThread, self).__init__()
        self.daemon = True
        self.name = jobname
        self.id = taskID
        self.state = "UNKNOWN"
        self.client = client
        self.callback = callback
        self.poll_interval = poll_interval
        self.poll_retries = poll_retries

    def run(self):
        while not self.is_done(self.state):
            time.sleep(self.poll_interval)
            # slow down polling over time till it hits a max
            # if self.poll_interval < 30:
            #     self.poll_interval += 1
            log.debug(
                "[job %s] POLLING %s" %
                (self.name, pformat(self.id))
            )
            try:
                self.state = self.poll()
            except Exception as e:
                log.error("[job %s] POLLING ERROR %s" % (self.name, e))
                if self.poll_retries > 0:
                    self.poll_retries -= 1
                    continue
                else:
                    log.error("[job %s] MAX POLLING RETRIES EXCEEDED" %
                              (self.name))
                    break

        self.complete()

    def poll(self):
        task = self.client.get_task(self.id, "MINIMAL")
        return task.state

    def is_done(self, state):
        terminal_states = ["COMPLETE", "CANCELED", "EXECUTOR_ERROR",
                           "SYSTEM_ERROR"]
        if state in terminal_states:
            log.info(
                "[job %s] FINAL JOB STATE: %s ------------------" %
                (self.name, state)
            )
            if state != "COMPLETE":
                log.error(
                    "[job %s] task id: %s" % (self.name, self.id)
                )
                log.error(
                    "[job %s] logs: %s" %
                    (
                        self.name,
                        self.client.get_task(self.id, "FULL").logs
                    )

                )
            return True
        return False

    def complete(self):
        self.callback()
