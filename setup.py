import sys
import os
import shlex
import re
from distutils.core import setup, Extension
from distutils.spawn import find_executable
from distutils.sysconfig import get_python_inc
from distutils.util import get_platform
from distutils.command.build_ext import build_ext as _build_ext
from distutils.command.bdist_wininst import bdist_wininst as _bdist_wininst
from distutils.dep_util import newer_group
from distutils.errors import *

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

    boolean_options = [ 'esql-static' ]

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
          if get_platform()=="win32":
            self.esql_informixdir = "C:\\Program Files\\Informix\\Client-SDK"
          else:
            self.esql_informixdir = "/usr/informix"
        os.environ['INFORMIXDIR'] = self.esql_informixdir

        self.esql_parts.append(os.path.join(self.esql_informixdir,'bin','esql'))
        if get_platform()=="win32":
          if self.esql_parts[0].find(' ') != -1:
            self.esql_parts[0] = '"' + self.esql_parts[0] + '"'

        if self.esql_threadlib:
            os.environ['THREADLIB'] = self.esql_threadlib
            self.esql_parts.append('-thread')
            if self.define is None:
                self.define = []
            self.define += [ ('IFX_THREAD',None), ('_REENTRANT',None) ]

        if self.esql_static:
            self.esql_parts.append('-static')

        # determine esql version
        esqlver = re.compile(r"(IBM)?.*ESQL Version (\d+)\.(\d+)")
        cout = os.popen(' '.join(self.esql_parts[0:1] + [ '-V' ]),'r')
        esqlversion = None
        for line in cout:
          matchobj = esqlver.match(line)
          if matchobj:
            matchgroups = matchobj.groups()
            esqlversion = int(matchgroups[1] + matchgroups[2])
            if matchgroups[0]=="IBM":
              # Assume ESQL 9.xx for any IBM branded CSDK.
              esqlversion = 960
        if esqlversion==None:
          esqlversion = 850
        if esqlversion >= 900:
          self.esql_parts.append("-EDHAVE_ESQL9")

        # find esql libs/objects
        cout = os.popen(' '.join(self.esql_parts + [ '-libs' ]),'r')
        esql_config = []
        lexer = shlex.shlex(cout)
        lexer.wordchars += '-.\\/'
        while True:
          token = lexer.get_token()
          if token=='' or token==None: break
          if token.startswith('"') and token.endswith('"'):
            token = token[1:-1]
          esql_config.append(token)
        ret = cout.close()
        if ret != None:
          raise DistutilsSetupError, \
                "\nCan't find esql. Please set INFORMIXDIR correctly."

        if get_platform()=="win32":
          for arg in esql_config:
              if arg.endswith('.lib'):
                  if self.libraries is None:
                      self.libraries = []
                  self.libraries.append(arg[:-4])
        else:
          for arg in esql_config:
              if arg.startswith('-l'):
                  if self.libraries is None:
                      self.libraries = []
                  self.libraries.append(arg[2:])
              else:
                  if self.link_objects is None:
                      self.link_objects = []
                  self.link_objects.append(arg)

        if self.include_dirs is None:
            self.include_dirs = []
        self.include_dirs = [os.path.join(self.esql_informixdir,'incl','esql')]\
                            + self.include_dirs

        if self.library_dirs is None:
            self.library_dirs = []
        self.library_dirs += [os.path.join(self.esql_informixdir,'lib','esql'),
                              os.path.join(self.esql_informixdir,'lib')]

    def build_extension(self, ext):
        # only preprocess with esql if necessary
        fullname = self.get_ext_fullname(ext.name)
        ext_filename = os.path.join(self.build_lib,
                                        self.get_ext_filename(fullname))
        if not (self.force or newer_group(ext.sources, ext_filename, 'newer')):
            self.announce("skipping '%s' extension (up-to-date)" % ext.name)
            return

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

class bdist_wininst(_bdist_wininst):
    """ override bdist_wininst to include the license in the description. """
    def get_inidata(self):
        metadata = self.distribution.metadata
        save_long_desc = metadata.long_description
        metadata.long_description += "\n\n" + file("COPYRIGHT").read() + "\n"
        result = _bdist_wininst.get_inidata(self)
        metadata.long_description = save_long_desc
        return result

def have_c_datetime():
    """ Check whether the datetime C API is available. """
    v = sys.version_info
    if sys.version_info[0] > 2:
        return 1
    elif sys.version_info[0] == 2 and sys.version_info[1] >= 4:
        return 1
    else:
        return 0

def have_py_bool():
    if sys.version_info[0] > 2:
        return 1
    elif sys.version_info[0] == 2 and sys.version_info[1] >= 3:
        return 1
    else:
        return 0

extra_macros = [('PYTHON_INCLUDE', get_python_inc(plat_specific=1)),
                ('HAVE_C_DATETIME', have_c_datetime()),
                ('HAVE_PY_BOOL', have_py_bool()) ]

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
                    sources = [os.path.join('ext','_informixdb.ec')],
                    include_dirs = ['ext'],
                    define_macros = extra_macros )

# patch distutils if it can't cope with the "classifiers" or 
# "download_url" keywords
if sys.version < '2.2.3':
    from distutils.dist import DistributionMetadata
    DistributionMetadata.classifiers = None
    DistributionMetadata.download_url = None

setup (name = 'InformixDB',
       version = '2.4',
       description = 'InformixDB v2.4',
       long_description = \
         "InformixDB is a DB-API 2.0 compliant interface for IBM Informix\n"
         "databases.",
       maintainer = "Carsten Haese",
       maintainer_email = "chaese@users.sourceforge.net",
       url = "http://sourceforge.net/projects/informixdb",
       license = "BSD License",
       platforms = ["POSIX", "Microsoft Windows 95/98/NT/2000/XP"],
       classifiers=[
         "Development Status :: 5 - Production/Stable",
         "Environment :: Console",
         "Intended Audience :: Developers",
         "License :: OSI Approved :: BSD License",
         "Operating System :: Microsoft :: Windows :: Windows 95/98/2000",
         "Operating System :: Microsoft :: Windows :: Windows NT/2000",
         "Operating System :: POSIX",
         "Programming Language :: C",
         "Programming Language :: Python",
         "Topic :: Database :: Front-Ends"
       ],
       py_modules = modules,
       ext_modules = [module1],
       cmdclass = {
         'build_ext' : build_ext,
         'bdist_wininst': bdist_wininst
       } )
