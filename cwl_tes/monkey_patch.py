

import urllib
import os

import sys
import cwltool.argparser
import json


# Patch functions in argparse to enable correct parsing of non file:// inputs
def FSActioncall(
    self,
    parser,  # type: argparse.ArgumentParser
    namespace,  # type: argparse.Namespace
    values,  # type: Union[AnyStr, Sequence[Any], None]
    option_string=None,  # type: Optional[str]
):  # type: (...) -> None
    url= urllib.parse.urlparse( values )
    print("This is the new FSAction: __call__ : self.dest {}, values {}".format(self.dest, values))
    if url.scheme == '':
        setattr(
            namespace,
            self.dest,
            {
                "class": self.objclass,
                "location": file_uri(str(os.path.abspath(cast(AnyStr, values)))),
            },
        )
    else:
        setattr(
            namespace,
            self.dest,
            {
                "class": self.objclass,
                "location": values,
            },
        )
         
cwltool.argparser.FSAction.__call__=FSActioncall          
             
        #print("FSAction: __call__ : self.dest {}, values {} ".format(self.dest, values ))
 
 
 
def FSAppendActioncall(
    self,
    parser,  # type: argparse.ArgumentParser
    namespace,  # type: argparse.Namespace
    values,  # type: Union[AnyStr, Sequence[Any], None]
    option_string=None,  # type: Optional[str]
):  # type: (...) -> None
    
    
    g = getattr(namespace, self.dest)
    if not g:
        g = []
        setattr(namespace, self.dest, g)
    url= urllib.parse.urlparse( values )
    print("This is the new FSAppendActionc: __call__ : self.dest {}, values {}".format(self.dest, values))
    if url.scheme ==  "" :
        g.append(
            {
                "class": self.objclass,
                "location": file_uri(str(os.path.abspath(cast(AnyStr, values)))),
            }
        )
    else:
        g.append(
            {
                "class": self.objclass,
                "location":  values,
            }
        )    
cwltool.argparser.FSAppendAction.__call__=FSAppendActioncall       


# def file_uricall(path, split_frag=False):  # type: (str, bool) -> str
#     
#     print("This is the new file_uri")
#     sys.exit(1)
#     if path.startswith("file://"):
#         return path
#     if path.startswith("s3://"):
#         return path
#     if split_frag:
#         pathsp = path.split("#", 2)
#         frag = "#" + urllib.parse.quote(str(pathsp[1])) if len(pathsp) == 2 else ""
#         urlpath = urllib.request.pathname2url(str(pathsp[0]))
#     else:
#         urlpath = urllib.request.pathname2url(path)
#         frag = ""
#     if urlpath.startswith("//"):
#         return "file:{}{}".format(urlpath, frag)
#     return "file://{}{}".format(urlpath, frag)
# schema_salad.ref_resolver.file_uri._call_=file_uricall
# # def relocateOutputscall(
#     outputObj: CWLObjectType,
#     destination_path: str,
#     source_directories: Set[str],
#     action: str,
#     fs_access: StdFsAccess,
#     compute_checksum: bool = True,
#     path_mapper: Type[PathMapper] = PathMapper,
# ) -> CWLObjectType:
#     adjustDirObjs(outputObj, functools.partial(get_listing, fs_access, recursive=True))
# 
#     if action not in ("move", "copy"):
#         return outputObj
# 
#     def _collectDirEntries(
#         obj: Union[CWLObjectType, MutableSequence[CWLObjectType], None]
#     ) -> Iterator[CWLObjectType]:
#         if isinstance(obj, dict):
#             if obj.get("class") in ("File", "Directory"):
#                 yield obj
#             else:
#                 for sub_obj in obj.values():
#                     for dir_entry in _collectDirEntries(sub_obj):
#                         yield dir_entry
#         elif isinstance(obj, MutableSequence):
#             for sub_obj in obj:
#                 for dir_entry in _collectDirEntries(sub_obj):
#                     yield dir_entry
# 
#     def _relocate(src: str, dst: str) -> None:
#         if src == dst:
#             return
# 
#         # If the source is not contained in source_directories we're not allowed to delete it
#         src = fs_access.realpath(src)
#         src_can_deleted = any(
#             os.path.commonprefix([p, src]) == p for p in source_directories
#         )
# 
#         _action = "move" if action == "move" and src_can_deleted else "copy"
# 
#         if _action == "move":
#             _logger.debug("Moving %s to %s", src, dst)
#             if fs_access.isdir(src) and fs_access.isdir(dst):
#                 # merge directories
#                 for dir_entry in scandir(src):
#                     _relocate(dir_entry.path, fs_access.join(dst, dir_entry.name))
#             else:
#                 shutil.move(src, dst)
# 
#         elif _action == "copy":
#             _logger.debug("Copying %s to %s", src, dst)
#             if fs_access.isdir(src):
#                 if os.path.isdir(dst):
#                     shutil.rmtree(dst)
#                 elif os.path.isfile(dst):
#                     os.unlink(dst)
#                 shutil.copytree(src, dst)
#             else:
#                 shutil.copy2(src, dst)
# 
#     def _realpath(
#         ob: CWLObjectType,
#     ) -> None:  # should be type Union[CWLFile, CWLDirectory]
#         if cast(str, ob["location"]).startswith("file:"):
#             ob["location"] = file_uri(
#                 os.path.realpath(uri_file_path(cast(str, ob["location"])))
#             )
#         if cast(str, ob["location"]).startswith("/"):
#             ob["location"] = os.path.realpath(cast(str, ob["location"]))
# 
#     outfiles = list(_collectDirEntries(outputObj))
#     visit_class(outfiles, ("File", "Directory"), _realpath)
#     pm = path_mapper(outfiles, "", destination_path, separateDirs=False)
#     stage_files(pm, stage_func=_relocate, symlink=False, fix_conflicts=True)
# 
#     def _check_adjust(a_file: CWLObjectType) -> CWLObjectType:
#         print("This is check_adjust")
#         location = cast(str, a_file["location"])
#         if urllib.parse.urlparse(os.path.splitdrive(a_file["location"])[1] ).scheme == "" :
#             a_file["location"] = file_uri(
#                 pm.mapper(location)[1]
#             )  # return the location of the file on the filesystem
#         else:
#             a_file["location"] = pm.mapper(location)[
#                 0
#             ]  # keep the original uri (e.g. s3, http, ftp, gs etc)
#         if "contents" in a_file:
#             del a_file["contents"]
#         return a_file
# 
#     visit_class(outputObj, ("File", "Directory"), _check_adjust)
# 
#     if compute_checksum:
#         visit_class(
#             outputObj, ("File",), functools.partial(compute_checksums, fs_access)
#         )
#     return outputObj
# 
# #uncache( cwltool.process.relocateOutputs)
# #cwltool.process.relocateOutputs.__call__=relocateOutputscall      
# import cwltool.process
# cwltool.process.relocateOutputs._call_=relocateOutputscall.__call__




def replaceURI( mapper: str, cwl_output:str):
    ''' convert the location of the output from cwltool to the original URI '''
    pathmap={}
    def getMapper():
        lines=mapper.splitlines()
        for line in lines:
            if line.startswith( "Mapper:"):
                dd=line.replace("Mapper: ", "")
                pm_dict=  json.loads(  dd ) 
                pathmap[pm_dict['target_uri']]=pm_dict
        
        
    getMapper(  )
    
    for k in pathmap:
        cwl_output=cwl_output.replace( k , pathmap[k]['resolved'])
    
    return cwl_output