import errno
import fnmatch
import io
import os

DEFAULT_EXTENSIONS = "c,h,C,H,cpp,hpp,cc,hh,c++,h++,cxx,hxx"
DEFAULT_CLANG_FORMAT_IGNORE = ".clang-format-ignore"


def excludes_from_file(ignore_file):
    excludes = []
    try:
        with io.open(ignore_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("#"):
                    # ignore comments
                    continue
                pattern = line.rstrip()
                if not pattern:
                    # allow empty lines
                    continue
                excludes.append(pattern)
    except EnvironmentError as e:
        if e.errno != errno.ENOENT:
            raise
    return excludes


def list_files(files, extensions=None, exclude=None):
    if extensions is None:
        extensions = []
    if exclude is None:
        exclude = []

    out = []
    for file in files:
        if os.path.isdir(file):
            for dirpath, dnames, fnames in os.walk(file):
                fpaths = [os.path.join(dirpath, fname) for fname in fnames]
                for pattern in exclude:
                    # os.walk() supports trimming down the dnames list
                    # by modifying it in-place,
                    # to avoid unnecessary directory listings.
                    dnames[:] = [
                        x
                        for x in dnames
                        if not fnmatch.fnmatch(os.path.join(dirpath, x), pattern)
                    ]
                    fpaths = [x for x in fpaths if not fnmatch.fnmatch(x, pattern)]
                for f in fpaths:
                    ext = os.path.splitext(f)[1][1:]
                    if ext in extensions:
                        out.append(f)
        else:
            out.append(file)
    return out
