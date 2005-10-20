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

"""\
 DB-API 2.0 compliant interface for IBM Informix databases.

Here's a small example to get you started:

>>> import informixdb
>>> conn = informixdb.connect('mydatabase')
>>> cursor = conn.cursor()
>>> cursor.execute('SELECT * FROM names')
>>> cursor.fetchall()
[('donald', 'duck', 34), ('mickey', 'mouse', 23)]

For more information on DB-API 2.0, see
http://www.python.org/peps/pep-0249.html
"""

from _informixdb import *

# promote the class definitions of _informixdb.Cursor and
# _informixdb.Connection into this namespace so that help(informixdb)
# sees their doc strings.
from _informixdb import Cursor as _Cursor, Connection as _Connection
class Cursor(_Cursor): pass
class Connection(_Connection): pass
