########################################################################
#                 Copyright (c) 1997 by IV DocEye AB
#              Copyright (c) 1999 by Stephen J. Turner
#                Copyright (c) 2005 by Carsten Haese
#  
# By obtaining, using, and/or copying this software and/or its
# associated documentation, you agree that you have read, understood,
# and will comply with the following terms and conditions:
#  
# Permission to use, copy, modify, and distribute this software and its
# associated documentation for any purpose and without fee is hereby
# granted, provided that the above copyright notice appears in all
# copies, and that both that copyright notice and this permission notice
# appear in supporting documentation, and that the name of the author
# not be used in advertising or publicity pertaining to distribution of
# the software without specific, written prior permission.
#  
# THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
# INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS.  IN
# NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, INDIRECT OR
# CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF
# USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
# OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.
########################################################################

# $Id$
# informixdb.py
#
# This is a trivial python wrapper around the C core _informixdb.so
#
# According to the DBAPI spec, a connection object is expected to supply
# the same set of functions as do a cursor object. This is fixed here by
# an implicit cursor.
#
# NYD = Not Yet Defined by DBAPI
# NYI = Not Yet Implemented
#

try:
    import _informixdb
except ImportError:
    raise ImportError, "Cannot locate C core module for informix db interface."

from _informixdb import Error
error = Error
from _informixdb import threadsafety
apilevel = "1.0"
paramstyle = "numeric"

class ifxcursor:
    def __init__(self, conn):
        self._cursor = conn.cursor()
        self.arraysize = 1

    def __getattr__(self, attr):
        if attr == 'description':
            return self._cursor.description
        elif attr == 'sqlerrd':
            return self._cursor.sqlerrd
        elif attr == 'rowcount':
            return self._cursor.rowcount
        else:
            raise AttributeError, attr

    def callproc(self, params = None):
        pass # NYD

    def execute(self, *args):
        return apply(self._cursor.execute, args)

    def executemany(self, oper, params):
        for p in params: self.execute(oper, p)

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchmany(self, size = None):
        if size==None: size = self.arraysize
        return self._cursor.fetchmany(size)

    def fetchall(self):
        return self._cursor.fetchall()

    def next(self):
        result = self.fetchone()
        if result == None: raise StopIteration
        return result

    def __iter__(self): return self

    def close(self):
        self._cursor.close()
        self._cursor = None

    def setinputsizes(self, sizes):
        pass # NYD

    def setoutputsizes(self, sizes, col = None):
        pass # NYD

class informixdb:
    def __init__(self, database=None, user=None, password=None):
        if database==None:
          raise TypeError, "No database name given."
        if user != None and password == None:
          raise TypeError, "Username but no password given."
        if user == None and password != None:
          raise TypeError, "Password but no username given."
        if user == None: user = ""
        if password == None: password = ""
        self.conn = _informixdb.informixdb(database,user,password)
        self._ifxcursor = ifxcursor(self.conn)

    def __getattr__(self, attr):
        if attr == 'description':
            return self._ifxcursor.description
        elif attr == 'arraysize':
            return self._ifxcursor.arraysize
        elif attr == 'error':
            return _informixdb.Error
        elif attr == 'Error':
            return _informixdb.Error
        elif attr == 'sqlerrd':
            return self._ifxcursor.sqlerrd
        elif attr == 'rowcount':
            return self._ifxcursor.rowcount
        else:
            raise AttributeError, attr

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def cursor(self):
        return ifxcursor(self.conn)

    def callproc(self, params = None):
        pass # NYD

    def execute(self, *args):
        return apply(self._ifxcursor.execute, args)

    def fetchone(self):
        return self._ifxcursor.fetchone()

    def fetchmany(self, size = None):
        return self._ifxcursor.fetchmany(size)

    def fetchall(self):
        return self._ifxcursor.fetchall()

    def close(self):
        self._ifxcursor.close()
        self.conn.close()
        self._ifxcursor = None
        self.conn = None

    def setinputsizes(self, sizes):
        pass # NYD

    def setoutputsizes(self, sizes, col = None):
        pass # NYD

def connect(*args, **kwargs):
  return informixdb(*args, **kwargs)
