from __future__ import absolute_import, print_function, unicode_literals

import logging
import os
import shutil
import tes
import time

from cwltool.draft2tool import CommandLineTool
from cwltool.errors import WorkflowException, UnsupportedRequirement
from cwltool.pathmapper import PathMapper
from cwltool.stdfsaccess import StdFsAccess
from cwltool.workflow import defaultMakeTool
from pprint import pformat
from schema_salad.ref_resolver import file_uri

from cwl_tes.pipeline import Pipeline, PipelineJob
from cwl_tes.poll import PollThread


log = logging.getLogger("tes-backend")


class TESPipeline(Pipeline):

    def __init__(self, url, kwargs):
        super(TESPipeline, self).__init__()
        self.kwargs = kwargs
        self.service = tes.HTTPClient(url)
        if kwargs.get("basedir") is not None:
            self.basedir = kwargs.get("basedir")
        else:
            self.basedir = os.getcwd()
        self.fs_access = StdFsAccess(self.basedir)

    def make_exec_tool(self, spec, **kwargs):
        return TESPipelineTool(spec, self, fs_access=self.fs_access, **kwargs)

    def make_tool(self, spec, **kwargs):
        if "class" in spec and spec["class"] == "CommandLineTool":
            return self.make_exec_tool(spec, **kwargs)
        else:
            return defaultMakeTool(spec, **kwargs)


class TESPipelineTool(CommandLineTool):

    def __init__(self, spec, pipeline, fs_access, **kwargs):
        super(TESPipelineTool, self).__init__(spec, **kwargs)
        self.spec = spec
        self.pipeline = pipeline
        self.fs_access = fs_access

    def makeJobRunner(self, use_container=True):
        return TESPipelineJob(self.spec, self.pipeline, self.fs_access)

    def makePathMapper(self, reffiles, stagedir, **kwargs):
        return PathMapper(reffiles, kwargs["basedir"], stagedir)


class TESPipelineJob(PipelineJob):

    def __init__(self, spec, pipeline, fs_access):
        super(TESPipelineJob, self).__init__(spec, pipeline)
        self.outputs = None
        self.docker_workdir = "/var/spool/cwl"
        self.fs_access = fs_access
        self.inplace_update = False

    def create_input_parameter(self, name, d):
        if "contents" in d:
            return tes.TaskParameter(
                name=name,
                description="cwl_input:%s" % (name),
                path=d["path"],
                contents=d["contents"],
                type=d["class"].upper()
            )
        else:
            return tes.TaskParameter(
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

            parameter = tes.TaskParameter(
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
            parameter = tes.TaskParameter(
                name="stdout",
                url=self.output2url(self.stdout),
                path=self.output2path(self.stdout)
            )
            output_parameters.append(parameter)

        if self.stderr is not None:
            parameter = tes.TaskParameter(
               name="stderr",
               url=self.output2url(self.stderr),
               path=self.output2path(self.stderr)
            )
            output_parameters.append(parameter)

        output_parameters.append(
            tes.TaskParameter(
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
                        tes.TaskParameter(
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
                    cmd=self.command_line,
                    image_name=container,
                    workdir=self.docker_workdir,
                    stdout=self.output2path(self.stdout),
                    stderr=self.output2path(self.stderr),
                    stdin=self.stdin,
                    environ=self.environment
                )
            ],
            inputs=input_parameters,
            outputs=output_parameters,
            resources=tes.Resources(
                cpu_cores=cpus,
                ram_gb=ram,
                size_gb=disk
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
            task_id = self.pipeline.service.create_task(task)
            log.info(
                "[job %s] SUBMITTED TASK ----------------------" %
                (self.name)
            )
            log.info("[job %s] task id: %s " % (self.name, task_id))
            operation = self.pipeline.service.get_task(task_id, "MINIMAL")
        except Exception as e:
            log.error(
                "[job %s] Failed to submit task to TES service:\n%s" %
                (self.name, e)
            )
            return WorkflowException(e)

        def callback(operation):
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

        poll = TESPipelinePoll(
            jobname=self.name,
            service=self.pipeline.service,
            operation=operation,
            callback=callback
        )

        self.pipeline.add_thread(poll)
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


class TESPipelinePoll(PollThread):

    def __init__(self, jobname, service, operation, callback):
        super(TESPipelinePoll, self).__init__(operation)
        self.name = jobname
        self.service = service
        self.callback = callback

    def run(self):
        while not self.is_done(self.operation):
            time.sleep(self.poll_interval)
            # slow down polling over time till it hits a max
            # if self.poll_interval < 30:
            #     self.poll_interval += 1
            log.debug(
                "[job %s] POLLING %s" %
                (self.name, pformat(self.id))
            )
            try:
                self.operation = self.poll()
            except Exception as e:
                log.error("[job %s] POLLING ERROR %s" % (self.name, e))
                if self.poll_retries > 0:
                    self.poll_retries -= 1
                    continue
                else:
                    log.error("[job %s] MAX POLLING RETRIES EXCEEDED" %
                              (self.name))
                    break

        self.complete(self.operation)

    def poll(self):
        return self.service.get_task(self.id, "MINIMAL")

    def is_done(self, operation):
        terminal_states = ["COMPLETE", "CANCELED", "ERROR", "SYSTEM_ERROR"]
        if operation.state in terminal_states:
            log.info(
                "[job %s] FINAL JOB STATE: %s ------------------" %
                (self.name, operation.state)
            )
            if operation.state != "COMPLETE":
                log.error(
                    "[job %s] task id: %s" % (self.name, self.id)
                )
                log.error(
                    "[job %s] logs: %s" %
                    (
                        self.name,
                        self.service.get_task(self.id, "FULL").logs
                    )

                )
            return True
        return False

    def complete(self, operation):
        self.callback(operation)
