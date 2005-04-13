/************************************************************************
 * 		  Copyright (c) 1997 by IV DocEye AB
 * 	       Copyright (c) 1999 by Stephen J. Turner
 * 	         Copyright (c) 1999 by Carsten Haese
 *  
 * By obtaining, using, and/or copying this software and/or its
 * associated documentation, you agree that you have read, understood,
 * and will comply with the following terms and conditions:
 *  
 * Permission to use, copy, modify, and distribute this software and its
 * associated documentation for any purpose and without fee is hereby
 * granted, provided that the above copyright notice appears in all
 * copies, and that both that copyright notice and this permission notice
 * appear in supporting documentation, and that the name of the author
 * not be used in advertising or publicity pertaining to distribution of
 * the software without specific, written prior permission.
 *  
 * THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
 * INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS.  IN
 * NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, INDIRECT OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF
 * USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 ***********************************************************************/

/*
  _informixdb.ec (formerly ifxdbmodule.ec)
  $Id$  
*/

#include <limits.h>
#include <time.h>
#define loc_t temp_loc_t
#include <ctype.h>
#undef loc_t

#ifdef WIN32
#include <value.h>
#include <sqlproto.h>
#else
#include <values.h>
#endif

#include <sqltypes.h>
#include <locator.h>
#include <datetime.h>

#include "Python.h"
#include "longobject.h"
#include "dbi.h"

#ifdef IFX_THREAD
#ifndef WITH_THREAD
#error Cannot use thread-safe Informix w/o Python threads!
#else
#define IFXDB_MT
#endif
#endif

static int ifxdbMaskNulls = 0;

static PyObject *ifxdbError;

EXEC SQL include sqlda.h;

/* NOT In USE: typedef PyObject * (* CopyFcn)(const void *); */

/* Define and handle connection object
*/
typedef struct
{
  PyObject_HEAD
  char name[20];
  int has_commit;
} connectionObject;

/* pointer cast */
static connectionObject *connection(PyObject *o)
{
 return  (connectionObject *) o;
}
/* forget the function; use a macro instead */
#define connection(o) ((connectionObject*)(o))

/* === */

static int setConnection(connectionObject*);
static void ifxdbPrintError(const char *);
static int sqlCheck(const char *);

/* Define and handle cursor object
*/
typedef struct
{
  PyObject_HEAD
  PyObject *my_conx;
  PyObject *description;  
  int state; /* 0=free, 1=prepared, 2=declared, 3=opened, 4=closed */
  char cursorName[20];
  char queryName[20];
  struct sqlda daIn;
  struct sqlda *daOut;
  int *originalType;
  char *outputBuffer;
  short *indIn;
  short *indOut;
  int *parmIdx;
  int stype; /* statement type */
  PyObject *op; /* last executed operation */
  long sqlerrd[6];
} cursorObject;

/* pointer cast */
static cursorObject *cursor(PyObject *o)
{
 return  (cursorObject *) o;
}
/* forget the function; use a macro instead */
#define cursor(o) ((cursorObject*)(o))

/* === */

static void cleanInputBinding(cursorObject *cur)
{
  struct sqlda *da = &cur->daIn;
  if (da->sqlvar) {
    int i;
    for (i=0; i<da->sqld; i++) {
      /* may not actually exist in some err cases */
      if ( da->sqlvar[i].sqldata) {
	if (da->sqlvar[i].sqltype == CLOCATORTYPE) {
	  loc_t *loc = (loc_t*) da->sqlvar[i].sqldata;
	  if (loc->loc_buffer) {
	    free(loc->loc_buffer);
	  }
	}
	free(da->sqlvar[i].sqldata);
      }
    }
  }
}

static void deleteInputBinding(cursorObject *cur)
{
  cleanInputBinding(cur);
  if (cur->daIn.sqlvar) {
    free(cur->daIn.sqlvar);
    cur->daIn.sqlvar = 0;
    cur->daIn.sqld = 0;
  }
  if (cur->indIn) {
    free(cur->indIn);
    cur->indIn = 0;
  }
  if (cur->parmIdx) {
    free(cur->parmIdx);
    cur->parmIdx = 0;
  }
}

static void deleteOutputBinding(cursorObject *cur)
{
  struct sqlda *da = cur->daOut;
  if (da && da->sqlvar) {
    int i;
    for (i=0; i<da->sqld; i++)
      if (da->sqlvar[i].sqldata &&
	  (da->sqlvar[i].sqltype == CLOCATORTYPE)) {
	loc_t *loc = (loc_t*) da->sqlvar[i].sqldata;
	if (loc->loc_buffer)
	  free(loc->loc_buffer);
      }
    free(cur->daOut);
    cur->daOut = 0;
  }
  if (cur->indOut) {
    free(cur->indOut);
    cur->indOut = 0;
  }
  if (cur->originalType) {
    free(cur->originalType);
    cur->originalType = 0;
  }
  if (cur->outputBuffer) {
    free(cur->outputBuffer);
    cur->outputBuffer = 0;
  }

  Py_XDECREF(cur->description);
  cur->description = NULL;
}

/* Datastructure and methods for cursors.
*/
static void cursorDealloc(PyObject *self);
static PyObject * cursorGetAttr(PyObject *self, char *name);

static PyTypeObject Cursor_Type =
{
#ifdef WIN32 /* Whacky MS Compiler refuse to resolve. */
  PyObject_HEAD_INIT (0)
#else
  PyObject_HEAD_INIT (&PyType_Type)
#endif
  0,			/*ob_size */
  "ifxdbcur",		/*tp_name */
  sizeof(cursorObject),	/*tp_basicsize */
  0,			/*tp_itemsize */
  cursorDealloc,	/*tp_dealloc */
  0,			/*tp_print */
  cursorGetAttr,	/*tp_getattr */
  /* drop the rest */
};

static void cursorError(cursorObject *cur, const char *action)
{
  ifxdbPrintError(action);
}

static char
ifxdbCursorDoc[] =  "Create a new cursor object and associate it with db object";

static PyObject *ifxdbCursor(PyObject *self, PyObject *args)
{
  cursorObject *cur = PyObject_NEW(cursorObject, &Cursor_Type);
  if (cur) {
    cur->description = 0;
    cur->my_conx = self;	/* Reference to db object. */
    Py_INCREF(self); /* the cursors owns a reference to the connection */
    cur->state = 0;
    cur->daIn.sqld = 0; cur->daIn.sqlvar = 0;
    cur->daOut = 0;
    cur->originalType = 0;
    cur->outputBuffer = 0;
    cur->indIn = 0;
    cur->indOut = 0;
    cur->parmIdx = 0;
    cur->stype = -1;
    cur->op = 0;
    sprintf(cur->cursorName, "CUR%lX", (unsigned long) cur);
    sprintf(cur->queryName, "QRY%lX", (unsigned long) cur);
  }
  return (PyObject*) cur;
}

/* XXX assumes that caller has already called setConnection */
static void doCloseCursor(cursorObject *cur, int doFree)
{
  EXEC SQL BEGIN DECLARE SECTION;
  char *cursorName = cur->cursorName;
  char *queryName = cur->queryName;
  EXEC SQL END DECLARE SECTION;

  if (cur->stype == 0) {
    /* if cursor is opened, close it */
    if (cur->state == 4) {
      EXEC SQL CLOSE :cursorName;
      cur->state = 5;
    }

    if (doFree) { 
      /* if cursor is prepared but not declared, free the statement */
      if (cur->state == 1)
	EXEC SQL FREE :queryName;

      /* if cursor is at least declared, free it */
      if (cur->state >= 2)
	EXEC SQL FREE :cursorName;

      cur->state = 0;
    }
  } else {
    if (doFree && cur->state == 1) {
      /* if statement is prepared, free it */
      EXEC SQL FREE :queryName;
      cur->state = 0;
    }
  }
}

static void cursorDealloc(PyObject *self)
{
  cursorObject *cur = cursor(self);

#ifdef STEP1
  /* Make sure we talk to the right database. */
  if (setConnection(connection(cur->my_conx)))
    PyErr_Clear();
#endif

  doCloseCursor(cur, 1);
  deleteInputBinding(cur);
  deleteOutputBinding(cur);

  Py_DECREF(cur->my_conx);
  PyMem_DEL(self);
}


static PyObject *ifxdbCurClose(PyObject *self, PyObject *args)
{
#ifdef STEP1
  /* Make sure we talk to the right database. */
  if (setConnection(connection(cursor(self)->my_conx))) return NULL;
#endif

  /* per the spec, the cursor should not be useable after calling `close',
   * so go ahead and free the cursor */
  doCloseCursor(cursor(self), 1);

  Py_INCREF(Py_None);
  return Py_None;
}

/* End cursors === */

/* Datastructure and methods for connections.
*/
static void connectionDealloc(PyObject *self);
static PyObject * connectionGetAttr(PyObject *self, char *name);

static PyTypeObject Connection_Type =
{
#ifdef WIN32 /* Whacky MS Compiler refuse to resolve. */
  PyObject_HEAD_INIT (0)
#else
  PyObject_HEAD_INIT (&PyType_Type)
#endif
  0,				/*ob_size */
  "ifxdbconn",			/*tp_name */
  sizeof (connectionObject),	/*tp_basicsize */
  0,				/*tp_itemsize */
  connectionDealloc,		/*tp_dealloc */
  0,				/*tp_print */
  connectionGetAttr,		/*tp_getattr */
  /* drop the rest */
};

static void connectionError(connectionObject *conn, const char *action)
{
  ifxdbPrintError(action);
}
/* forget the function; use a macro instead */
#define connectionError(conn, action) (ifxdbPrintError(action))

#ifdef IFXDB_MT
#define KEY "ifxdbConn"
static PyObject* ifxdbConnKey;
static char* getConnectionName(void)
{
  PyObject *dict, *conn;
  char* name;

  if (!ifxdbConnKey)
    ifxdbConnKey = PyString_FromString(KEY);

  dict = PyThreadState_GetDict();
  conn = PyDict_GetItem(dict, ifxdbConnKey);
  if (!conn) return NULL;

  name = PyString_AS_STRING(conn);
  return name;
}
static void setConnectionName(const char* name)
{
  PyObject *dict, *conn;

  if (!ifxdbConnKey)
    ifxdbConnKey = PyString_FromString(KEY);

  dict = PyThreadState_GetDict();
  if (!name)
    PyDict_DelItem(dict, ifxdbConnKey);
  else {
    conn = PyString_FromString(name);
    PyDict_SetItem(dict, ifxdbConnKey, conn);
    Py_DECREF(conn);
  }
}
#undef KEY
#else
static char* ifxdbConnName;
#define getConnectionName() (ifxdbConnName)
#define setConnectionName(s) (ifxdbConnName = (s))
#endif

/* returns zero on success, nonzero on failure (as with sqlCheck) */
static int setConnection(connectionObject *conn)
{
  EXEC SQL BEGIN DECLARE SECTION;
  char *connectionName;
  EXEC SQL END DECLARE SECTION;

  /* No need to swap connection if correctly set. */
  connectionName = getConnectionName();
  if (!connectionName || strcmp(connectionName, conn->name)) {
    connectionName = conn->name;
    EXEC SQL SET CONNECTION :connectionName;
    if (sqlCheck("SET-CONNECTION")) return 1;
    setConnectionName(conn->name);
  }
  return 0;
}

/* End connections === */

static int unsuccessful()
{
  return SQLCODE;
}
/* forget the function; use a macro instead */
#define unsuccessful() (SQLCODE)

/* Message generator to return human readable
   error message.
 */
static void ifxdbPrintError(const char *action)
{
  char message[512];
  EXEC SQL BEGIN DECLARE SECTION;
  char message1[255];
  char message2[255];
  int messlen1;
  int messlen2;
  int exc_cnt;
  char sqlstate_2[6];
  EXEC SQL END DECLARE SECTION;

  EXEC SQL get diagnostics  exception 1
      :message1 = MESSAGE_TEXT, :messlen1 = MESSAGE_LENGTH;

#ifdef EXTENDED_ERROR_HANDLING
  EXEC SQL get diagnostics :exc_cnt = NUMBER ;

  if (exc_cnt > 1) {
    EXEC SQL get diagnostics  exception 2
        :sqlstate_2 = RETURNED_SQLSTATE,
        :message2 = MESSAGE_TEXT, :messlen2 = MESSAGE_LENGTH;
  }
#else
  exc_cnt = 1;
#endif /* EXTENDED_ERROR_HANDLING */

  /* In some cases, message1 is null and messlen1 undefined... */
  if ( messlen1 > 0 && messlen1 <= 255 ) {
    /* messlen1 value is often incorrect, so recalculate it */
    messlen1 = byleng(message1, 254) - 1;
    if (message1[messlen1] != '\n')
      messlen1++;
    message1[messlen1] = 0;
  } else
    strcpy(message1, "<NULL diagnostic message 1>");

  if (exc_cnt > 1) {
    /* In some cases, message2 is null and messlen2 undefined... */
    if ( messlen2 > 0 && messlen2 <= 255 ) {
      /* messlen2 value is often incorrect, so recalculate it */
      messlen2 = byleng(message2, 254) - 1;
      if (message2[messlen2] != '\n')
	messlen2++;
      message2[messlen2] = 0;
    } else
      strcpy(message2, "<NULL diagnostic message 2>");
  }

  if (exc_cnt > 1) {
      sprintf(message,
	      "Error %d performing %s: %s (%s:%s)",
	      SQLCODE, action, message1, sqlstate_2, message2);
      PyErr_SetObject(ifxdbError, 
          Py_BuildValue("(sissss)", message, SQLCODE, action, message1,
                                sqlstate_2, message2));
  } else {
      sprintf(message,
	      "Error %d performing %s: %s",
	      SQLCODE, action, message1);
      PyErr_SetObject(ifxdbError, 
          Py_BuildValue("(siss)", message, SQLCODE, action, message1));
  }
}

/* returns 0 on success, 1 on failure */
static int sqlCheck(const char *action)
{
  if (unsuccessful()) {
    ifxdbPrintError(action);
    return 1;
  }
  return 0; 
}
/* forget the function; use a macro instead */
#define sqlCheck(action) (unsuccessful() ? ifxdbPrintError(action), 1 : 0)
    
/* End error reporter */

/* Begin section for transaction management.
*/

#ifdef STEP2
/* Experimental code.
   Idea is to allow for user to run without transactions at will.
*/
static PyObject *ifxdbBegin(PyObject *self, PyObject *args)
{
  connectionObject *conn = connection(self);
  if (conn->has_commit) {
    if (setConnection(conn)) return NULL;
    EXEC SQL BEGIN WORK;

    if (unsuccessful()) {
      connectionError(conn, "COMMIT");
      return 0;
    }
  /* success */
  Py_INCREF(Py_None);
  return Py_None;
}
#endif

static PyObject *ifxdbClose(PyObject *self, PyObject *args)
{
  connectionObject *conn = connection(self);
  if (conn->has_commit) {
    if (setConnection(conn)) return NULL;
    EXEC SQL COMMIT WORK;

    if (unsuccessful()) {
      connectionError(conn, "COMMIT");
      return 0;
    }

    /*
    EXEC SQL DISCONNECT :connectionName ;
    */
  }
  /* success */
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *ifxdbRollback(PyObject *, PyObject *);
static PyObject *ifxdbCommit(PyObject *, PyObject *);

static PyMethodDef connectionMethods[] = {
  { "cursor", ifxdbCursor, 1 } ,
#ifdef STEP2
  { "begin", ifxdbBegin, 1 } ,
#endif
  { "commit", ifxdbCommit, 1 } ,
  { "rollback", ifxdbRollback, 1 } ,
#ifdef STEP2
  { "execute", ifxdbExec, 1} ,
  { "fetchone", ifxdbFetchOne, 1} ,
  { "fetchmany", ifxdbFetchMany, 1} ,
  { "fetchall", ifxdbFetchAll, 1} ,
  { "setinputsizes", ifxdbSetInputSizes, 1} ,
  { "setoutputsize", ifxdbSetOutputSize, 1} ,
#endif
  { "close", ifxdbClose, 1 } ,
  {0,     0}        /* Sentinel */
};

static PyObject *connectionGetAttr(PyObject *self,
			    char *name)
{
  if (!strcmp(name, "Error")) {
    Py_INCREF(ifxdbError);
    return ifxdbError;
  }
  return Py_FindMethod (connectionMethods, self, name);
}

static void connectionDealloc(PyObject *self)
{
  EXEC SQL BEGIN DECLARE SECTION;
  char *connectionName;
  EXEC SQL END DECLARE SECTION;

  connectionName = connection(self)->name;
  EXEC SQL DISCONNECT :connectionName ;

  /* just in case the connection being destroyed is no longer current... */
  if (getConnectionName() == connectionName)
    setConnectionName(NULL);

  PyMem_DEL(self);
}

/* If this connection has transactions, commit and
   restart transaction.
*/
static PyObject *ifxdbCommit(PyObject *self, PyObject *args)
{
  connectionObject *conn = connection(self);
  if (conn->has_commit) {
    if (setConnection(conn)) return NULL;
    EXEC SQL COMMIT WORK;

    if (unsuccessful()) {
      connectionError(conn, "COMMIT");
      return 0;
    }
    else {
      EXEC SQL BEGIN WORK;
      if (unsuccessful()) {
	connectionError(conn, "BEGIN");
	return 0;
      }
    }
  }
  /* success */
  Py_INCREF(Py_None);
  return Py_None;
}

/*
 Pretty much the same as COMMIT operation.
 We ROLLBACK and restart transaction but this has only meaning if there
 is support for transactions.
 */
static PyObject *ifxdbRollback(PyObject *self, PyObject *args)
{
  connectionObject *conn = connection(self);
  if (conn->has_commit) {
    if (setConnection(conn)) return NULL;
    EXEC SQL ROLLBACK WORK;

    if (unsuccessful()) {
      connectionError(conn, "ROLLBACK");
      return 0;
    }
    else {
      EXEC SQL BEGIN WORK;
      if (unsuccessful()) {
	connectionError(conn, "BEGIN");
	return 0;
      }
    }
  Py_INCREF(Py_None);
  return Py_None;
  }
}

/* End sections transaction support === */

/* Begin section for parser of sql statements w.r.t.
   dynamic variable binding.
*/
typedef struct {
  const char *ptr;
  char *out;
  int parmCount;
  int parmIdx;
  int isParm;
  char state;
  char prev;
} parseContext;

static void initParseContext(parseContext *ct, const char *c, char *out)
{
  ct->state = 0;
  ct->ptr = c;
  ct->out = out;
  ct->parmCount = 0;
}

static int doParse(parseContext *ct) 
{
  int rc = 0;
  register const char *in = ct->ptr;
  register char *out = ct->out;
  register char ch;

  ct->isParm = 0;
  while (ch = *in++) {
    if (ct->state == ch) {
      ct->state = 0;
    } else if (ct->state == 0){
      if ((ch == '\'') || (ch == '"')) {
	ct->state = ch;
      } else if (ch == '?') {
	ct->parmIdx = ct->parmCount;
	ct->parmCount++;
	ct->isParm = 1;
	rc = 1;
	break;
      } else if ((ch == ':') && !isalnum(ct->prev)) {
	const char *m = in;
	int n = 0;
	while (isdigit(*m)) {
	  n *= 10;
	  n += *m - '0';
	  m++;
	}
	if (n) {
	  ct->parmIdx = n-1;
	  ct->parmCount++;
	  in = m;
	  ct->isParm = 1;
	  ct->prev = '0';
	  rc = 1;
	  break;
	}
      }
    }
    ct->prev = *out++ = ch;
  }

  *out++ = rc ? '?' : 0;
  ct->ptr = in;
  ct->out = out;
  return rc;
}


static int ibindRaw(struct sqlvar_struct *var, PyObject *item)
{
  PyObject *nitem = dbiValue(item);
  PyObject *sitem = PyObject_Str(nitem);
#ifdef PyString_GET_SIZE
  int n = PyString_GET_SIZE(sitem);
#else
  int n = PyString_Size(sitem);
#endif
  loc_t *loc = (loc_t*) malloc(sizeof(loc_t)) ;
  loc->loc_loctype = LOCMEMORY;
  loc->loc_buffer = malloc(n);
  loc->loc_bufsize = n;
  loc->loc_size = n;
  loc->loc_oflags = 0;
  loc->loc_mflags = 0;
  loc->loc_indicator = 0;
  memcpy(loc->loc_buffer,
	 PyString_AS_STRING((PyStringObject*)sitem),
	 n);
    
  var->sqldata = (char *) loc;
  var->sqllen = sizeof(loc_t);
  var->sqltype = CLOCATORTYPE;
  *var->sqlind = 0;
  Py_DECREF(sitem);
  return 1;
}

static int ibindDate(struct sqlvar_struct *var, PyObject *item)
{
  PyObject *sitem = PyNumber_Long(dbiValue(item));
  if (sitem) {
    long n = PyLong_AsLong(sitem);
    dtime_t *dt = (dtime_t*) malloc(sizeof(dtime_t)) ;
    char buf[20];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&n));
    dt->dt_qual = TU_DTENCODE(TU_YEAR, TU_SECOND);
    dtcvasc(buf, dt);
    var->sqldata = (char *) dt;
    var->sqllen = sizeof(dtime_t);
    var->sqltype = CDTIMETYPE;
    *var->sqlind = 0;
    Py_DECREF(sitem);
  }
  else {
    PyErr_SetString(ifxdbError, "type error in date object");
    return 0;
  }
  return 1;
}

static int ibindString(struct sqlvar_struct *var, PyObject *item)
{
  PyObject *sitem = PyObject_Str(item);
  const char *val = PyString_AS_STRING((PyStringObject*)sitem);
  int n = strlen(val);
  var->sqltype = CSTRINGTYPE;
  var->sqldata = malloc(n+1);
  var->sqllen = n;
  *var->sqlind = 0;
  memcpy(var->sqldata, val, n+1);
  if (PyLong_Check(item)) {
    var->sqldata[n-1] = 0; /* erase 'L' suffix */
  }
  Py_DECREF(sitem);
  return 1;
}

static int ibindNone(struct sqlvar_struct *var, PyObject *item)
{
  var->sqltype = CSTRINGTYPE;
  var->sqldata = NULL;
  var->sqllen = 0;
  *var->sqlind = -1;
  return 1;
}

typedef int (*ibindFptr)(struct sqlvar_struct *, PyObject*);

static ibindFptr ibindFcn(PyObject* item)
{
  if (dbiIsRaw(item)) {
    return ibindRaw;
  }
  else if(dbiIsDate(item)) {
    return ibindDate;
  }
  else if(item == Py_None) {
    return ibindNone;
  }
  else {
    return ibindString;
  }
}

static void allocSlots(cursorObject *cur, int n_slots)
{
  cur->daIn.sqld = n_slots;
  cur->daIn.sqlvar = calloc(n_slots, sizeof(struct sqlvar_struct));
  cur->indIn = calloc(n_slots, sizeof(short));
}

#define PARM_COUNT_GUESS 4
#define PARM_COUNT_INCREMENT 4
static int parseSql(cursorObject *cur, register char *out, const char *in)
{
  parseContext ctx;
  int i, n_slots;
  struct sqlvar_struct *var;
  short *ind;

  n_slots = PARM_COUNT_GUESS;
  cur->parmIdx = malloc(PARM_COUNT_GUESS * sizeof(int));

  initParseContext(&ctx, in, out);
  while (doParse(&ctx)) {
    cur->parmIdx[ctx.parmCount-1] = ctx.parmIdx;
    if (ctx.parmCount == n_slots) {
      n_slots += PARM_COUNT_INCREMENT;
      cur->parmIdx = realloc(cur->parmIdx, n_slots * sizeof(int));
    }
  }

  if (ctx.parmCount == 0) {
    free(cur->parmIdx);
    cur->parmIdx = NULL;
  } else if (ctx.parmCount < n_slots)
    cur->parmIdx = realloc(cur->parmIdx, ctx.parmCount * sizeof(int));

  allocSlots(cur, ctx.parmCount);
  var = cur->daIn.sqlvar;
  ind = cur->indIn;
  for (i = 0; i < ctx.parmCount; ++i)
    (var++)->sqlind = ind++;

  return 1;
}

static int bindInput(cursorObject *cur, PyObject *vars)
{
  struct sqlvar_struct *var = cur->daIn.sqlvar;
  int n_vars = vars ? PyObject_Length(vars) : 0;
  int i;

  for (i = 0; i < cur->daIn.sqld; ++i)
    if (cur->parmIdx[i] < n_vars) {
      int success;
      PyObject *item = PySequence_GetItem(vars, cur->parmIdx[i]);

      success = (*ibindFcn(item))(var++, item);
      Py_DECREF(item);	/* PySequence_GetItem increments it */
      if (!success)
	return 0;
    } else {
      PyErr_SetString(ifxdbError, " too few actual parameters");
      return 0;
    }

  return 1;
}

static PyObject *typeOf(int type)
{
  switch(type & SQLTYPE){
  case SQLBYTES:
  case SQLTEXT:
    return DbiRaw;
  case SQLDATE:
  case SQLDTIME:
    return DbiDate;
  case SQLFLOAT:
  case SQLSMFLOAT:
  case SQLDECIMAL:
  case SQLINT:
  case SQLSMINT:
  case SQLSERIAL:
  case SQLMONEY:
    return DbiNumber;
  default:
    return DbiString;
  }
}

static void bindOutput(cursorObject *cur)
{
  char * bufp;
  int pos;
  int count = 0;
  struct sqlvar_struct *var;

  cur->indOut = calloc(cur->daOut->sqld, sizeof(short));
  cur->originalType = calloc(cur->daOut->sqld, sizeof(int));

  cur->description = PyTuple_New(cur->daOut->sqld);
  for (pos = 0, var = cur->daOut->sqlvar;
       pos < cur->daOut->sqld;
       pos++, var++) {
    PyObject *new_tuple = Py_BuildValue("(sOiiiii)",
					var->sqlname, 
					typeOf(var->sqltype),
					var->sqllen,
					var->sqllen,
					0, 0, !(var->sqltype & SQLNONULL));
    PyTuple_SET_ITEM(cur->description, pos, new_tuple);

    var->sqlind = &cur->indOut[pos];
    cur->originalType[pos] = var->sqltype;

    switch(var->sqltype & SQLTYPE){
    case SQLBYTES:
    case SQLTEXT:
      var->sqllen  = sizeof(loc_t);
      var->sqltype = CLOCATORTYPE;
      break;
    case SQLSMFLOAT:
      var->sqltype = CFLOATTYPE;
      break;
    case SQLFLOAT:
      var->sqltype = CDOUBLETYPE;
      break;
    case SQLDECIMAL:
      var->sqltype = CDECIMALTYPE;
      break;
    case SQLMONEY:
      var->sqltype = CMONEYTYPE;
      break;
    case SQLDATE:
      var->sqltype = CDATETYPE;
      break;
    case SQLSMINT:
      var->sqltype = CSHORTTYPE;
      break;
    case SQLINT:
    case SQLSERIAL:
      var->sqltype = CLONGTYPE;
      break;
    case SQLDTIME:
      var->sqllen = 20; /* big enough */
      /* fall through */
    default:
      var->sqltype = CCHARTYPE;
      var->sqllen = rtypmsize(var->sqltype, var->sqllen);
      break;
      
    }
    var->sqllen = rtypmsize(var->sqltype, var->sqllen);
    count = rtypalign(count, var->sqltype) + var->sqllen;
  }

  bufp = cur->outputBuffer = malloc(count);

  for (pos = 0, var = cur->daOut->sqlvar;
       pos < cur->daOut->sqld;
       pos++, var++) {
    bufp = (char *) rtypalign( (int) bufp, var->sqltype);
    
    if (var->sqltype == CLOCATORTYPE) {
      loc_t *loc = (loc_t*) bufp;
      loc->loc_loctype = LOCMEMORY;
      loc->loc_bufsize = -1;
      loc->loc_oflags = 0;
      loc->loc_mflags = 0;
    }
    var->sqldata = bufp;
    bufp += var->sqllen;
  }
}

#define returnOnError(action) if (sqlCheck(action)) return 0

static PyObject *ifxdbCurGetSqlerrd(PyObject *self, PyObject *args)
{
  cursorObject *cur = cursor(self);
  return Py_BuildValue("(iiiiii)",
    cur->sqlerrd[0], cur->sqlerrd[1], cur->sqlerrd[2],
    cur->sqlerrd[3], cur->sqlerrd[4], cur->sqlerrd[5]);
}

static PyObject *ifxdbCurExec(PyObject *self, PyObject *args)
{
  cursorObject *cur = cursor(self);
  struct sqlda *tdaIn = &cur->daIn;
  struct sqlda *tdaOut = cur->daOut;
  PyObject *op;
  const char *sql;
  int i;
  EXEC SQL BEGIN DECLARE SECTION;
  char *queryName = cur->queryName;
  char *cursorName = cur->cursorName;
  char *newSql;
  EXEC SQL END DECLARE SECTION;
  
  PyObject *inputvars = 0;
  if (!PyArg_ParseTuple(args, "s|O", &sql, &inputvars))
    return 0;
  op = PyTuple_GET_ITEM(args, 0);

#ifdef STEP1
  /* Make sure we talk to the right database. */
  if (setConnection(connection(cur->my_conx))) return NULL;
#endif

  if (op == cur->op) {
    doCloseCursor(cur, 0);
    cleanInputBinding(cur);
  } else {
    doCloseCursor(cur, 1);
    deleteInputBinding(cur);
    deleteOutputBinding(cur);

    /* `newSql' may be shorter than but will never exceed length of `sql' */
    newSql = malloc(strlen(sql) + 1);
    if (!parseSql(cur, newSql, sql)) {
      free(newSql);
      return 0;
    }
    EXEC SQL PREPARE :queryName FROM :newSql;
    free(newSql);
    returnOnError("PREPARE");
    cur->state = 1;

    EXEC SQL DESCRIBE :queryName INTO tdaOut;
    cur->daOut = tdaOut;
    cur->stype = SQLCODE;

    if (cur->stype == 0) {
      bindOutput(cur);

      EXEC SQL DECLARE :cursorName CURSOR FOR :queryName;
      returnOnError("DECLARE");
      EXEC SQL FREE :queryName;
      returnOnError("FREE");
      cur->state = 2;
    } else {
      free(cur->daOut);
      cur->daOut = NULL;
    }

    /* cache operation reference */
    cur->op = op;
    Py_INCREF(op);
  }

  if (!bindInput(cur, inputvars))
    return 0;

  if (cur->stype == 0) {
    EXEC SQL OPEN :cursorName USING DESCRIPTOR tdaIn;
    returnOnError("OPEN");
    cur->state = 3;

    Py_INCREF(Py_None);
    return Py_None;
  } else {
    Py_BEGIN_ALLOW_THREADS;
    EXEC SQL EXECUTE :queryName USING DESCRIPTOR tdaIn;
    Py_END_ALLOW_THREADS;
    if (unsuccessful())
      cursorError(cur, "EXEC");
    else {
      for (i=0; i<6; i++) cur->sqlerrd[i] = sqlca.sqlerrd[i];
      return Py_BuildValue("i", sqlca.sqlerrd[2]); /* number of row */
    }
  }
  /* error return */
  return 0;
}

/* Routines for manipulations of datetime
*/
static int convertToInt(dtime_t *d, const char *fmt)
{
  char buf[20];
  int x = dttofmtasc(d, buf, 20, (char *)fmt);
  return atoi(buf);
}

static time_t convertDtToUnix(dtime_t *d)
{
  struct tm gt;
    
  gt.tm_isdst = -1;
  
  gt.tm_year = convertToInt(d, "%Y") - 1900;
  gt.tm_mon = convertToInt(d, "%m") - 1;   /* month */
  gt.tm_mday = convertToInt(d, "%d");   /* day */
  gt.tm_hour = convertToInt(d, "%H");   /* hour */
  gt.tm_min = convertToInt(d, "%M");   /* minute */
  gt.tm_sec = convertToInt(d, "%S");   /* second */
  return mktime(&gt);
}

static time_t convertAscToUnix(const char *d)
{
  struct tm gt;
  memset(&gt, '\0', sizeof(gt));
  sscanf(d, "%04d-%02d-%02d %02d:%02d:%02d",
	 &gt.tm_year,
	 &gt.tm_mon,
	 &gt.tm_mday,
	 &gt.tm_hour,
	 &gt.tm_min,
	 &gt.tm_sec);
  gt.tm_year -= 1900;
  gt.tm_mon -= 1;
  gt.tm_isdst = -1;

  return mktime(&gt);
}

/* End datetime === */

static PyObject *doCopy(/* const */ void *data, int type)
{
  switch(type){
  case SQLDATE:
  {
    char buf[11];
    rfmtdate(*(long*)data, "yyyy-mm-dd", buf);
    return dbiMakeDate(PyInt_FromLong(convertAscToUnix(buf)));
  }  
  case SQLDTIME:
    return dbiMakeDate(PyInt_FromLong(convertAscToUnix((char*)data)));
  case SQLCHAR:
  case SQLVCHAR:
  case SQLNCHAR:
  case SQLNVCHAR:
  {
      /* NOTE: we must axe trailing spaces in Informix (boggle) */
      register size_t len = strlen((char*)data);
      register char * p = (char*)data + len - 1;

      while ( len > 1 && *p == ' ' )
      {
	  *p-- = '\0';
	  --len;
      }
      return Py_BuildValue("s", (char*)data);
  }
  case SQLFLOAT:
    return PyFloat_FromDouble(*(double*)data);
  case SQLSMFLOAT:
    return PyFloat_FromDouble(*(float*)data);
  case SQLDECIMAL:
  case SQLMONEY:
  {
    double dval;
    dectodbl((dec_t*)data, &dval);
    return PyFloat_FromDouble(dval);
  }
  case SQLSMINT:
    return PyInt_FromLong(*(short*)data);
  case SQLINT:
  case SQLSERIAL:
    return PyInt_FromLong(*(long*)data);
  case SQLBYTES:
  case SQLTEXT:
    ((loc_t*)data)->loc_mflags |= LOC_ALLOC;
    return dbiMakeRaw
      (PyString_FromStringAndSize(((loc_t*)data)->loc_buffer,
				  ((loc_t*)data)->loc_size));
  }
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *processOutput(cursorObject *cur)
{
  PyObject *row = PyTuple_New(cur->daOut->sqld);
  int pos;
  struct sqlvar_struct *var;

#ifdef AIX
  /* ### deal with some wacky bug in AIX's mktime() */
  printf("");
#endif

  for (pos = 0, var = cur->daOut->sqlvar;
       pos < cur->daOut->sqld;
       pos++, var++) {

    PyObject *v;
    if (*var->sqlind < 0 && (!ifxdbMaskNulls)) {
      v = Py_None;
      Py_INCREF(v);
    }
    else {
      v = doCopy(var->sqldata, cur->originalType[pos]);
    }
      
    PyTuple_SET_ITEM(row, pos, v);
  }
  return row;
}



static PyObject *ifxdbCurFetchOne(PyObject *self, PyObject *args)
{
  cursorObject *cur = cursor(self);
  struct sqlda *tdaOut = cur->daOut;

  EXEC SQL BEGIN DECLARE SECTION;
  char *cursorName;
  EXEC SQL END DECLARE SECTION;

  cursorName = cur->cursorName;

#ifdef STEP1
  /* Make sure we talk to the right database. */
  if (setConnection(connection(cur->my_conx))) return NULL;
#endif

  Py_BEGIN_ALLOW_THREADS;
  EXEC SQL FETCH :cursorName USING DESCRIPTOR tdaOut;
  Py_END_ALLOW_THREADS;
  if (!strncmp(SQLSTATE, "02", 2)) {
    Py_INCREF(Py_None);
    return Py_None;
  }
  else if (strncmp(SQLSTATE, "00", 2)) {
    cursorError(cur, "FETCH");
    return 0;
  }
  return processOutput(cur);
}

static PyObject *ifxdbFetchCounted(PyObject *self, int count)
{
  PyObject *list = PyList_New(0);

  while ( count-- > 0 )
  {
      PyObject *entry = ifxdbCurFetchOne(self, 0);

      if ( entry == NULL )
      {
	  Py_DECREF(list);
	  return NULL;
      }
      if ( entry == Py_None )
      {
	  Py_DECREF(entry);
	  break;
      }

      if ( PyList_Append(list, entry) == -1 )
      {
	  Py_DECREF(entry);
	  Py_DECREF(list);
	  return NULL;
      }

      Py_DECREF(entry);
  }

  return list;
}

static PyObject *ifxdbCurFetchMany(PyObject *self, PyObject *args)
{
  int n_rows = 1;

  if (!PyArg_ParseTuple(args, "|i", &n_rows))
      return NULL;

  return ifxdbFetchCounted(self, n_rows);
}

static PyObject *ifxdbCurFetchAll(PyObject *self, PyObject *args)
{
  return ifxdbFetchCounted(self, INT_MAX);
}

static PyObject *ifxdbCurSetInputSizes(PyObject *self, PyObject *args)
{
  Py_INCREF(Py_None);
  return Py_None;
}
static PyObject *ifxdbCurSetOutputSize(PyObject *self, PyObject *args)
{
  Py_INCREF(Py_None);
  return Py_None;
}


static PyMethodDef cursorMethods[] = {
  { "close", ifxdbCurClose, 1} ,
  { "execute", ifxdbCurExec, 1} ,
  { "fetchone", ifxdbCurFetchOne, 1} ,
  { "fetchmany", ifxdbCurFetchMany, 1} ,
  { "fetchall", ifxdbCurFetchAll, 1} ,
  { "setinputsizes", ifxdbCurSetInputSizes, 1} ,
  { "setoutputsize", ifxdbCurSetOutputSize, 1} ,
  {0,     0}        /* Sentinel */
};

static PyObject *cursorGetAttr(PyObject *self,
			     char *name)
{
  if (!strcmp(name, "description")) {
    if (cursor(self)->description) {
      Py_INCREF(cursor(self)->description);
      return cursor(self)->description;
    } else {
      Py_INCREF(Py_None);
      return Py_None;
    }
  }
  if (!strcmp(name, "Error")) {
    Py_INCREF(ifxdbError);
    return ifxdbError;
  }
  if (!strcmp(name, "sqlerrd")) {
    Py_INCREF(Py_None);
    return ifxdbCurGetSqlerrd(self, Py_None);
  }
  return Py_FindMethod (cursorMethods, self, name);
}

static PyObject *ifxdbLogon(PyObject *self, PyObject *args)
{
  EXEC SQL BEGIN DECLARE SECTION;
  char *connectionName;
  char *connectionString;
  char *dbUser;
  char *dbPass;
  EXEC SQL END DECLARE SECTION;
  
  connectionObject *conn = 0;
  if (PyArg_ParseTuple(args, "sss", &connectionString, &dbUser, &dbPass)) {
    conn = PyObject_NEW (connectionObject, &Connection_Type);
    if (conn) {
      sprintf(conn->name, "CONN%lX", (unsigned long) conn);
      connectionName = conn->name;

      Py_BEGIN_ALLOW_THREADS;
      if (*dbUser && *dbPass) {
        EXEC SQL CONNECT TO :connectionString AS :connectionName USER :dbUser USING :dbPass WITH CONCURRENT TRANSACTION;
      }
      else {
        EXEC SQL CONNECT TO :connectionString AS :connectionName WITH CONCURRENT TRANSACTION;
      }
      Py_END_ALLOW_THREADS;

      if (unsuccessful()) {
	connectionError(conn, "LOGON");
	PyMem_DEL(conn);
	conn = 0;
      } else {
	conn->has_commit = (sqlca.sqlwarn.sqlwarn1 == 'W') ;
	if (conn->has_commit) {
	  EXEC SQL BEGIN WORK;
	  if (unsuccessful()) {
	    connectionError(conn, "BEGIN");
	    PyMem_DEL(conn);
	    conn = 0;
	  }
	}
      }
    }
  }

  if (conn)
    setConnectionName(conn->name);

  return (PyObject*) conn;
}

static PyMethodDef globalMethods[] = {
  { "informixdb", ifxdbLogon, 1} ,
  {0,     0}        /* Sentinel */
};

static PyObject *InformixError__init__(PyObject *self, PyObject *args);
static PyObject *InformixError__str__(PyObject *self, PyObject *args);
static PyMethodDef
InformixError_methods[] = {
    { "__str__",    InformixError__str__, METH_VARARGS},
    { "__init__",    InformixError__init__, METH_VARARGS},
    { NULL, NULL }
};

static PyObject *InformixError__str__(PyObject *self, PyObject *args) {
    self = PyTuple_GetItem(args, 0);
    return PyObject_GetAttrString(self, "message");
}

static PyObject *InformixError__init__(PyObject *self, PyObject *args) {
    int status;
    PyObject *val;
    int i,n;
    char **attr_name;
    char *attr_names[] = {
      "message",
      "sqlcode",
      "action",
      "message1",
#ifdef EXTENDED_ERROR_HANDLING
      "sqlstate",
      "message2",
#endif
      NULL
    };

    self = PyTuple_GetItem(args, 0);

    if (!(args = PySequence_GetSlice(args, 1, PySequence_Size(args))))
        return NULL;

    status = PyObject_SetAttrString(self, "args", args);
    if (status < 0) {
        Py_DECREF(args);
        return NULL;
    }

    n = PySequence_Size(args);
    for (i=0, attr_name=attr_names; *attr_name; attr_name++, i++) {
      if (i>=n) break;
      val = PySequence_GetItem(args, i);
      status = PyObject_SetAttrString(self, *attr_name, val);
      if (status < 0) {
          Py_DECREF(val);
          Py_DECREF(args);
          return NULL;
      }
      Py_DECREF(val);
    }

    Py_DECREF(args);
    Py_INCREF(Py_None);
    return Py_None;
}

/* void initinformixdb() */
void init_informixdb()
{
  char *env;
  extern void initdbi();
  int threadsafety = 0;
  PyObject *dict = PyDict_New();
  PyMethodDef *def;

#ifdef WIN32
  PyObject *m;
  Cursor_Type.ob_type = &PyType_Type;
  Connection_Type.ob_type = &PyType_Type;
  m = Py_InitModule("_informixdb", globalMethods);
#else
  PyObject *m = Py_InitModule("_informixdb", globalMethods);
#endif

  ifxdbError = PyErr_NewException("informixdb.Error", NULL, dict);
  for (def = InformixError_methods; def->ml_name != NULL; def++) {
    PyObject *func = PyCFunction_New(def, NULL);
    PyObject *method = PyMethod_New(func, NULL, ifxdbError);
    PyDict_SetItemString(dict, def->ml_name, method);
    Py_DECREF(func);
    Py_DECREF(method);
  }

  PyDict_SetItemString (PyModule_GetDict (m), "Error", ifxdbError);

#ifdef IFX_THREAD
  threadsafety = 1;
#endif
  PyDict_SetItemString (PyModule_GetDict (m), "threadsafety",
                        Py_BuildValue("i", threadsafety) );

  /* Setting this environment variable to a non-zero value restores
   * the module's former erroneous handling of NULL outputs. */
  if (env = getenv("IFXDB_MASK_NULLS"))
    ifxdbMaskNulls = atoi(env);

  initdbi();
  /* PyImport_ImportModule("dbi"); */
}
