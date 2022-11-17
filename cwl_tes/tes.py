from __future__ import absolute_import, print_function, unicode_literals

import logging
import os
import random
import time
import threading
import stat
from builtins import str
import shutil
import functools
import uuid
from tempfile import NamedTemporaryFile
from pprint import pformat
from typing import (Any, Callable, Dict, List, MutableMapping, MutableSequence,
                    Optional, Union)
from typing_extensions import Text

import tes
from six.moves import urllib

from schema_salad.ref_resolver import file_uri
from schema_salad.sourceline import SourceLine
from schema_salad import validate
from cwltool.builder import Builder
from cwltool.command_line_tool import CommandLineTool
from cwltool.context import RuntimeContext
from cwltool.errors import WorkflowException, UnsupportedRequirement
from cwltool.expression import JSON
from cwltool.job import JobBase
from cwltool.stdfsaccess import StdFsAccess
from cwltool.pathmapper import (PathMapper, uri_file_path, MapperEnt,
                                downloadHttpFile)
from cwltool.utils import onWindows, convert_pathsep_to_unix
from cwltool.workflow import default_make_tool

from .ftp import abspath

log = logging.getLogger("tes-backend")


def make_tes_tool(spec, loading_context, url, remote_storage_url, token, user, password):
    """cwl-tes specific factory for CWL Process generation."""
    if "class" in spec and spec["class"] == "CommandLineTool":
        return TESCommandLineTool(
            spec, loading_context, url, remote_storage_url, token, user, password)
    return default_make_tool(spec, loading_context)


class TESCommandLineTool(CommandLineTool):
    """cwl-tes specific CommandLineTool."""

    def __init__(self, spec, loading_context, url, remote_storage_url, token, user, password):
        super(TESCommandLineTool, self).__init__(spec, loading_context)
        self.spec = spec
        self.url = url
        self.remote_storage_url = remote_storage_url
        self.token = token
        self.user=user
        self.password=password

    def make_path_mapper(self, reffiles, stagedir, runtimeContext,
                         separateDirs):
        if self.remote_storage_url:
            return TESPathMapper(
                reffiles, runtimeContext.basedir, stagedir, separateDirs,
                runtimeContext.make_fs_access(self.remote_storage_url or ""))
        return super(TESCommandLineTool, self).make_path_mapper(
            reffiles, stagedir, runtimeContext, separateDirs)

    def make_job_runner(self, runtimeContext):
        if self.remote_storage_url:
            remote_storage_url = self.remote_storage_url + "/output_{}".format(
                uuid.uuid4())
        else:
            remote_storage_url = ""
        return functools.partial(TESTask, runtime_context=runtimeContext,
                                 url=self.url, spec=self.spec,
                                 remote_storage_url=remote_storage_url,
                                 token=self.token, user=self.user, password=self.password)


class TESPathMapper(PathMapper):

    def __init__(self, reference_files, basedir, stagedir, separateDirs=True,
                 fs_access=None):
        self.fs_access = fs_access
        super(TESPathMapper, self).__init__(reference_files, basedir, stagedir,
                                            separateDirs)

    def _download_streaming_file(self, path):
        with NamedTemporaryFile(mode='wb', delete=False) as dest:
            with self.fs_access.open(path, mode="rb") as handle:
                shutil.copyfileobj(handle, dest)
            return dest.name

    def visit(self, obj, stagedir, basedir, copy=False, staged=False):
        tgt = convert_pathsep_to_unix(
            os.path.join(stagedir, obj["basename"]))
        if obj["location"] in self._pathmap:
            return
        if obj["class"] == "Directory":
            if obj["location"].startswith("file://"):
                log.warning("a file:// based Directory slipped through: %s",
                            obj)
                resolved = uri_file_path(obj["location"])
            else:
                resolved = obj["location"]
            self._pathmap[obj["location"]] = MapperEnt(
                resolved, tgt, "WritableDirectory" if copy else "Directory",
                staged)
            if obj["location"].startswith("file://"):
                staged = False
            self.visitlisting(
                obj.get("listing", []), tgt, basedir, copy=copy, staged=staged)
        elif obj["class"] == "File":
            path = obj["location"]
            abpath = abspath(path, basedir)
            if "contents" in obj and obj["location"].startswith("_:"):
                self._pathmap[obj["location"]] = MapperEnt(
                    obj["contents"], tgt, "CreateFile", staged)
            else:
                with SourceLine(obj, "location", validate.ValidationException,
                                log.isEnabledFor(logging.DEBUG)):
                    deref = abpath
                    if urllib.parse.urlsplit(deref).scheme in [
                            'http', 'https']:
                        deref = downloadHttpFile(path)
                    elif urllib.parse.urlsplit(deref).scheme in ('ftp', 's3'):
                        deref = self._download_streaming_file(path)
                    else:
                        log.warning("unprocessed File %s", obj)
                        # Dereference symbolic links
                        st = os.lstat(deref)
                        while stat.S_ISLNK(st.st_mode):
                            rl = os.readlink(deref)
                            deref = rl if os.path.isabs(rl) \
                                else os.path.join(os.path.dirname(deref), rl)
                            st = os.lstat(deref)

                    self._pathmap[path] = MapperEnt(
                        deref, tgt, "WritableFile" if copy else "File", staged)
                    self.visitlisting(
                        obj.get("secondaryFiles", []), stagedir, basedir,
                        copy=copy, staged=staged)


class TESTask(JobBase):
    JobOrderType = Dict[Text, Union[Dict[Text, Any], List, Text]]

    def __init__(self,
                 builder,   # type: Builder
                 joborder,  # type: JSON
                 make_path_mapper,  # type: Callable[..., PathMapper]
                 requirements,  # type: List[Dict[Text, Text]]
                 hints,  # type: List[Dict[Text, Text]]
                 name,   # type: Text
                 runtime_context,
                 url,
                 spec,
                 remote_storage_url=None,
                 token=None,
                 user=None,
                 password=None):
        super(TESTask, self).__init__(builder, joborder, make_path_mapper,
                                      requirements, hints, name)
        self.runtime_context = runtime_context
        self.spec = spec
        self.outputs = None
        self.inplace_update = False
        self.basedir = runtime_context.basedir or os.getcwd()
        self.fs_access = StdFsAccess(self.basedir)

        self.id = None
        self.state = "UNKNOWN"
        self.exit_code = None
        self.poll_interval = 1
        self.poll_retries = 10
        self.client = tes.HTTPClient(url, token=token, user=user, password=password)
        self.remote_storage_url = remote_storage_url
        self.token = token
        self.user = user
        self.password = password

    def get_container(self):
        default = self.runtime_context.default_container or "python:2.7"
        container = default

        docker_req, _ = self.get_requirement("DockerRequirement")
        if docker_req:
            container = docker_req.get(
                "dockerPull",
                docker_req.get("dockerImageId", default)
            )
        return container

    def create_input(self, name, d):
        if "contents" in d:
            return tes.Input(
                name=name,
                description="cwl_input:%s" % (name),
                path=d["path"],
                content=d["contents"],
                type=d["class"].upper()
            )
        return tes.Input(
            name=name,
            description="cwl_input:%s" % (name),
            url=d["location"],
            path=d["path"],
            type=d["class"].upper()
        )

    def parse_job_order(self, k, v, inputs):
        if isinstance(v, MutableMapping):
            if all([i in v for i in ["location", "path", "class"]]):
                inputs.append(self.create_input(k, v))

                if "secondaryFiles" in v:
                    for f in v["secondaryFiles"]:
                        self.parse_job_order(f["basename"], f, inputs)

            else:
                for sk, sv in v.items():
                    if isinstance(sv, MutableMapping):
                        self.parse_job_order(sk, sv, inputs)

                    else:
                        break

        elif isinstance(v, MutableSequence):
            for i in range(len(v)):
                if isinstance(v[i], MutableMapping):
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

            if urllib.parse.urlparse(loc).scheme:
                url = loc
            else:
                url = file_uri(loc)
            parameter = tes.Input(
                name=item["basename"],
                description="InitialWorkDirRequirement:cwl_input:%s" % (
                    item["basename"]
                ),
                url=url,
                path=self.fs_access.join(
                    self.builder.outdir, item["basename"]),
                type=item["class"].upper()
            )
            inputs.append(parameter)

        return inputs

    def get_inputs(self):
        inputs = []

        # find all primary and secondary input files
        for k, v in self.joborder.items():
            self.parse_job_order(k, v, inputs)

        # manage InitialWorkDirRequirement
        self.parse_listing(self.generatefiles["listing"], inputs)

        return inputs

    def get_envvars(self):
        env = self.environment
        vars_to_preserve = self.runtime_context.preserve_environment
        if self.runtime_context.preserve_entire_environment:
            vars_to_preserve = os.environ
        if vars_to_preserve is not None:
            for key, value in os.environ.items():
                if key in vars_to_preserve and key not in env:
                    # On Windows, subprocess env can't handle unicode.
                    env[key] = str(value) if onWindows() else value
        env["HOME"] = str(self.builder.outdir) if onWindows() \
            else self.builder.outdir
        env["TMPDIR"] = str(self.builder.tmpdir) if onWindows() \
            else self.builder.tmpdir
        return env

    def create_task_msg(self):
        input_parameters = self.get_inputs()
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
                path=self.builder.outdir,
                type="DIRECTORY"
            )
        )

        container = self.get_container()

        res_reqs = self.builder.resources
        ram = res_reqs['ram'] / 953.674
        disk = (res_reqs['outdirSize'] + res_reqs['tmpdirSize']) / 953.674
        cpus = res_reqs['cores']

        docker_req, _ = self.get_requirement("DockerRequirement")
        if docker_req and hasattr(docker_req, "dockerOutputDirectory"):
            output_parameters.append(
                tes.Output(
                    name="dockerOutputDirectory",
                    url=self.output2url(""),
                    path=docker_req.dockerOutputDirectory,
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
                    workdir=self.builder.outdir,
                    stdout=self.output2path(self.stdout),
                    stderr=self.output2path(self.stderr),
                    stdin=self.stdin,
                    env=self.get_envvars()
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

    def run(self,
            runtimeContext,   # type: RuntimeContext
            tmpdir_lock=None  # type: Optional[threading.Lock]
            ):  # type: (...) -> None
        log.debug(
            "[job %s] self.__dict__ in run() ----------------------",
            self.name
        )
        log.debug(pformat(self.__dict__))
        if not self.successCodes:
            self.successCodes = [0]

        task = self.create_task_msg()

        log.info(
            "[job %s] CREATED TASK MSG----------------------",
            self.name
        )
        log.info(pformat(task))

        try:
            self.id = self.client.create_task(task)
            log.info(
                "[job %s] SUBMITTED TASK ----------------------",
                self.name
            )
            log.info("[job %s] task id: %s ", self.name, self.id)
        except Exception as e:
            log.error(
                "[job %s] Failed to submit task to TES service:\n%s",
                self.name, e
            )
            raise WorkflowException(e)

        max_tries = 10
        current_try = 1
        self.exit_code = None
        while not self.is_done():
            delay = 1.5 * current_try**2
            time.sleep(
                random.randint(
                    round(
                        delay -
                        0.5 *
                        delay),
                    round(
                        delay +
                        0.5 *
                        delay)))
            try:
                task = self.client.get_task(self.id, "MINIMAL")
                self.state = task.state
                log.debug(
                    "[job %s] POLLING %s, result: %s", self.name,
                    pformat(self.id), task.state
                )
            except Exception as e:
                log.error("[job %s] POLLING ERROR %s", self.name, e)
                if current_try <= max_tries:
                    current_try += 1
                    continue
                else:
                    log.error("[job %s] MAX POLLING RETRIES EXCEEDED",
                              self.name)
                    break

        try:
            process_status = None
            if self.state != "COMPLETE" \
                    and self.exit_code not in self.successCodes:
                process_status = "permanentFail"
                log.error("[job %s] job error:\n%s", self.name, self.state)
            remote_cwl_output_json = False
            if self.remote_storage_url:
                remote_fs_access = runtimeContext.make_fs_access(
                    self.remote_storage_url)
                remote_cwl_output_json = remote_fs_access.exists(
                    remote_fs_access.join(
                        self.remote_storage_url, "cwl.output.json"))
            if self.remote_storage_url:
                original_outdir = self.builder.outdir
                if not remote_cwl_output_json:
                    self.builder.outdir = self.remote_storage_url
                outputs = self.collect_outputs(self.remote_storage_url,
                                               self.exit_code)
                self.builder.outdir = original_outdir
            else:
                outputs = self.collect_outputs(self.outdir, self.exit_code)
            cleaned_outputs = {}
            for k, v in outputs.items():
                if isinstance(k, bytes):
                    k = k.decode("utf8")
                if isinstance(v, bytes):
                    v = v.decode("utf8")
                cleaned_outputs[k] = v
            self.outputs = cleaned_outputs
            if not process_status:
                process_status = "success"
        except (WorkflowException, Exception) as err:
            log.error("[job %s] job error:\n%s", self.name, err)
            if log.isEnabledFor(logging.DEBUG):
                log.exception(err)
            process_status = "permanentFail"
        finally:
            if self.outputs is None:
                self.outputs = {}
            with self.runtime_context.workflow_eval_lock:
                self.output_callback(self.outputs, process_status)
            log.info(
                "[job %s] OUTPUTS ------------------",
                self.name
            )
            log.info(pformat(self.outputs))
            self.cleanup(self.runtime_context.rm_tmpdir)
        return

    def is_done(self):
        terminal_states = ["COMPLETE", "CANCELED", "EXECUTOR_ERROR",
                           "SYSTEM_ERROR"]
        if self.state in terminal_states:
            log.info(
                "[job %s] FINAL JOB STATE: %s ------------------",
                self.name, self.state
            )
            if self.state != "COMPLETE":
                log.error(
                    "[job %s] task id: %s", self.name, self.id
                )
                logs = self.client.get_task(self.id, "FULL").logs
                log.error(
                    "[job %s] logs: %s",
                    self.name, logs
                )
                if isinstance(logs, MutableSequence):
                    last_log = logs[-1]
                    if isinstance(last_log, tes.TaskLog) and last_log.logs:
                        self.exit_code = last_log.logs[-1].exit_code
            return True
        return False

    def cleanup(self, rm_tmpdir):
        log.debug(
            "[job %s] STARTING CLEAN UP ------------------",
            self.name
        )
        if self.stagedir and os.path.exists(self.stagedir):
            log.debug(
                "[job %s] Removing input staging directory %s",
                self.name, self.stagedir
            )
            shutil.rmtree(self.stagedir, True)

        if rm_tmpdir:
            log.debug(
                "[job %s] Removing temporary directory %s",
                self.name, self.tmpdir
            )
            shutil.rmtree(self.tmpdir, True)

    def output2url(self, path):
        if path is not None:
            if self.remote_storage_url:
                return self.fs_access.join(
                    self.remote_storage_url, os.path.basename(path))
            return file_uri(
                self.fs_access.join(self.outdir, os.path.basename(path))
            )
        return None

    def output2path(self, path):
        if path is not None:
            return self.fs_access.join(self.builder.outdir, path)
        return None
