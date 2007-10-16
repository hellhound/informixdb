set INFORMIXDIR=C:\Program Files\IBM\Informix\Client-SDK
set OLDPATH=%PATH%
set PATH=%INFORMIXDIR%\bin;C:\msys\1.0\mingw\bin;%PATH%
C:\Python25\python.exe setup.py bdist_wininst build_ext -c mingw32 --esql-threadlib=winnt
C:\Python24\python.exe setup.py bdist_wininst build_ext -c mingw32 --esql-threadlib=winnt
C:\Python23\python.exe setup.py bdist_wininst build_ext -c mingw32 --esql-threadlib=winnt
set PATH=%OLDPATH%
