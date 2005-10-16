#ifndef DATETIME_COMPAT_H
#define DATETIME_COMPAT_H

#ifdef PyDateTime_IMPORT
  /* The datetime-API is available, we shouldn't even be including this! */
  #error "Oops, the datetime-compatibility wrappers were included " \
	 "even though the datetime C-API seems to be available!"
#endif

/* This implements part of the Python >= 2.4 datetime C API in earlier
 * versions by calling into a Python 'datetime' module.
 * 
 * When using Python <= 2.2 the 'datetime' module must be installed 
 * separately as pure python module. It should be available from the
 * Python CVS sandbox and/or come with the Informix DB distribution.
 *
 * This should only be used to conditionally define datetime-functions
 * when needed. It is no (good) header in the traditional sense, since
 * it defines potentially conflicting Py*-symbols and static
 * functions/variables!
 */

typedef struct {
  PyTypeObject* DateType;
  PyTypeObject* DateTimeType;
} PyDateTimeAPI_type;

static PyDateTimeAPI_type PyDateTimeAPI_object;
static PyDateTimeAPI_type *PyDateTimeAPI = &PyDateTimeAPI_object; 

#define PyDateTime_IMPORT CompatPyDateTime_Import()

static PyObject* CompatPyDateTime_Import(void)
{
  PyObject *module, *mdict;
  
  module = PyImport_ImportModule("datetime");
  if (!module) {
    return NULL;
  }

  mdict = PyModule_GetDict(module); 

  PyDateTimeAPI->DateType =
    (PyTypeObject*)PyDict_GetItemString(mdict, "date");
  PyDateTimeAPI->DateTimeType =
    (PyTypeObject*)PyDict_GetItemString(mdict, "datetime");

  return module;
}

static int CompatPyDateTime_Get(PyObject *datetime, char* attr)
{
  int i;
  PyObject *o = PyObject_GetAttrString(datetime, attr);
  if (!o)
    return 0;
  i = PyInt_AsLong(o);
  Py_DECREF(o);
  return i;
}

#define PyDateTime_GET_YEAR(datetime) CompatPyDateTime_Get(datetime, "year")
#define PyDateTime_GET_MONTH(datetime) CompatPyDateTime_Get(datetime, "month")
#define PyDateTime_GET_DAY(datetime) CompatPyDateTime_Get(datetime, "day")

#define PyDateTime_DATE_GET_HOUR(datetime) \
          CompatPyDateTime_Get(datetime, "hour")
#define PyDateTime_DATE_GET_MINUTE(datetime) \
          CompatPyDateTime_Get(datetime, "minute")
#define PyDateTime_DATE_GET_SECOND(datetime) \
          CompatPyDateTime_Get(datetime, "second")
#define PyDateTime_DATE_GET_MICROSECOND(datetime) \
          CompatPyDateTime_Get(datetime, "microsecond")

#define PyDateTime_Check(op) PyObject_TypeCheck(op, PyDateTimeAPI->DateTimeType)
#define PyDate_Check(op) PyObject_TypeCheck(op, PyDateTimeAPI->DateType)

static PyObject* PyDate_FromDate(int year, int month, int day)
{
  PyObject *args, *o;
  args = Py_BuildValue("(iii)", year, month, day);
  o = PyObject_CallObject((PyObject*)PyDateTimeAPI->DateType, args);
  Py_DECREF(args);
  return o;
}

static PyObject* PyDateTime_FromDateAndTime(int year, int month, int day,
                                            int hour, int minute, int second,
                                            int microsecond)
{
  PyObject *args, *o;
  args = Py_BuildValue("(iiiiiii)", year, month, day, hour, minute, second,
                                    microsecond);
  o = PyObject_CallObject((PyObject*)PyDateTimeAPI->DateTimeType, args);
  Py_DECREF(args);
  return o;
}

static PyObject* PyDate_FromTimestamp(PyObject *args)
{
  return PyObject_CallMethod((PyObject*)PyDateTimeAPI->DateType,
                             "fromtimestamp", "O", args);
}

static PyObject* PyDateTime_FromTimestamp(PyObject *args)
{
  return PyObject_CallMethod((PyObject*)PyDateTimeAPI->DateTimeType,
                             "fromtimestamp", "O", args);
}

#endif
