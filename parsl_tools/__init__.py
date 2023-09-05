import os


from parsl.data_provider.files import File
from parsl.dataflow.memoization import id_for_memo


@id_for_memo.register(File)
def id_for_memo_File(f, output_ref=False):
    if output_ref:
        # logger.debug("hashing File as output ref without content: {}".format(f))
        return f.url
    else:
        # logger.debug("hashing File as input with content: {}".format(f))
        assert f.scheme == "file"
        filename = f.filepath
        try:
            stat_result = os.stat(filename)

            return [f.url, stat_result.st_size, stat_result.st_mtime]
        except:
            return [f.url, 0, 0]