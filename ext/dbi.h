/*
  dbi.h

  This is the general interface to COPPERMAN-compliant databases.

  In particular, types and type numbers are defined
*/
#ifndef DBI_H
#define DBI_H

int dbiIsDate(const PyObject *o);
int dbiIsRaw(const PyObject *o);
int dbiIsRowId(const PyObject *o);

/* These do not INCREF */
PyObject *dbiValue(PyObject *o);  
PyObject *dbiMakeDate(PyObject *contents);
PyObject *dbiMakeRaw(PyObject *contents);
PyObject *dbiMakeRowId(PyObject *contents);

#ifdef WINDOWS
    #ifdef DBI_EXPORT
	#define CALLCONV DL_EXPORT
    #else
	#define CALLCONV DL_IMPORT
    #endif
    #define DL_EXPORT(RTYPE) __declspec(dllexport) RTYPE
#else
#  define CALLCONV(X) X
#endif


CALLCONV(PyObject) * DbiString;
CALLCONV(PyObject) * DbiRaw;
CALLCONV(PyObject) * DbiRowId;
CALLCONV(PyObject) * DbiNumber;
CALLCONV(PyObject) * DbiDate;

CALLCONV(PyObject) * DbiNoError;
CALLCONV(PyObject) * DbiOpError;
CALLCONV(PyObject) * DbiProgError;
CALLCONV(PyObject) * DbiIntegrityError;
CALLCONV(PyObject) * DbiDataError;
CALLCONV(PyObject) * DbiInternalError;


#endif
