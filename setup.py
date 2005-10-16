import sys
import os
from distutils.core import setup, Extension
from distutils.spawn import find_executable
from distutils.sysconfig import get_python_inc
from distutils.util import get_platform
from distutils.command.build_ext import build_ext as _build_ext

class build_ext(_build_ext):
    """ build_ext which can handle ESQL/C (*.ec) files """

    user_options = _build_ext.user_options + [
        ('esql-threadlib=', None,
        '[ESQL/C] Thread library to use with ESQL/C'),
        ('esql-informixdir=', None,
        '[ESQL/C] Informixdir to use if $INFORMIXDIR is not set'),
        ('esql-static', None,
        '[ESQL/C] statically link against ESQL/C libraries')
        ]

    boolean_options = [ 'esql_static' ]

    def initialize_options(self):
        _build_ext.initialize_options(self)

        self.esql_informixdir = None
        self.esql_threadlib = None
        self.esql_static = 0
        self.esql_parts = []

    def finalize_options(self):
        _build_ext.finalize_options(self)

        if not self.esql_informixdir:
            self.esql_informixdir = os.getenv("INFORMIXDIR")
        if not self.esql_informixdir:
            self.esql_informixdir = "/usr/informix"
        os.environ['INFORMIXDIR'] = self.esql_informixdir

        self.esql_parts.append(os.path.join(self.esql_informixdir, 'bin', 'esql'))

        if self.esql_threadlib:
            os.environ['THREADLIB'] = self.esql_threadlib
            self.esql_parts.append('-thread')
            if self.define is None:
                self.define = []
            self.define += [ ('IFX_THREAD',None), ('_REENTRANT',None) ]

        if self.esql_static:
            self.esql_parts.append('-static')

        # find esql libs/objects
        cin,cout = os.popen2(' '.join(self.esql_parts + [ '-libs' ]))
        esql_config = cout.readlines()
        cin.close()
        cout.close()

        for arg in esql_config:
            if arg.startswith('-l'):
                if self.libraries is None:
                    self.libraries = []
                self.libraries.append(arg[2:-1])
            else:
                if self.link_objects is None:
                    self.link_objects = []
                self.link_objects.append(arg[:-1])

        if self.include_dirs is None:
            self.include_dirs = []
        self.include_dirs = [ '/usr/local/informix/incl/esql' ] \
                            + self.include_dirs

        if self.library_dirs is None:
            self.library_dirs = []
        self.library_dirs += [ '/usr/local/informix/lib/esql',
                               '/usr/local/informix/lib' ]

    def build_extension(self, ext):
        # preprocess *.ec files with 'esql'
        for file in ext.sources:
            if file.endswith('.ec'):
                dir = os.path.dirname(file)
                f = os.path.basename(file)
                cmd = ' '.join(self.esql_parts + [ '-e', f ])
                print cmd

                curdir = os.getcwd()
                os.chdir(dir)
                os.system(cmd)
                os.chdir(curdir)

                ext.sources[ext.sources.index(file)] = file[:-3]+'.c'

        _build_ext.build_extension(self, ext)

    def _esql_preprocess(file):
        return cfile

def have_c_datetime():
    """ Check whether the datetime C API is available. """
    v = sys.version_info
    if sys.version_info[0] > 2:
        return 1
    elif sys.version_info[0] == 2 and sys.version_info[1] >= 4:
        return 1
    else:
        return 0

extra_macros = [('PYTHON_INCLUDE', get_python_inc(plat_specific=1)),
                ('HAVE_C_DATETIME', have_c_datetime())]

modules = [ 'informixdb' ]

# If we don't have a datetime module available, install ours
saved_path = sys.path
try:
    sys.path = sys.path[1:]
    import datetime
except:
    modules.append('datetime')
sys.path = saved_path

# On AIX we need to define _H_LOCALEDEF, so that the system's
# loc_t doesn't conflict with Informix' loc_t for BLOBs
if get_platform().startswith('aix-'):
  extra_macros.append(('_H_LOCALEDEF', None))

module1 = Extension('_informixdb',
                    sources = ['ext/_informixdb.ec'],
                    include_dirs = ['ext'],
                    define_macros = extra_macros )

setup (name = 'InformixDB',
       version = '1.4',
       description = 'InformixDB v1.4',
       py_modules = modules,
       ext_modules = [module1],
       cmdclass = { 'build_ext' : build_ext })
