/* Generated by Cython 0.29.37 */

#ifndef __PYX_HAVE_API__pycylon__data__compute
#define __PYX_HAVE_API__pycylon__data__compute
#ifdef __MINGW64__
#define MS_WIN64
#endif
#include "Python.h"

static PyObject *(*__pyx_api_f_7pycylon_4data_7compute_c_filter)(PyObject *, PyObject *) = 0;
#define c_filter __pyx_api_f_7pycylon_4data_7compute_c_filter
#if !defined(__Pyx_PyIdentifier_FromString)
#if PY_MAJOR_VERSION < 3
  #define __Pyx_PyIdentifier_FromString(s) PyString_FromString(s)
#else
  #define __Pyx_PyIdentifier_FromString(s) PyUnicode_FromString(s)
#endif
#endif

#ifndef __PYX_HAVE_RT_ImportFunction_0_29_37
#define __PYX_HAVE_RT_ImportFunction_0_29_37
static int __Pyx_ImportFunction_0_29_37(PyObject *module, const char *funcname, void (**f)(void), const char *sig) {
    PyObject *d = 0;
    PyObject *cobj = 0;
    union {
        void (*fp)(void);
        void *p;
    } tmp;
    d = PyObject_GetAttrString(module, (char *)"__pyx_capi__");
    if (!d)
        goto bad;
    cobj = PyDict_GetItemString(d, funcname);
    if (!cobj) {
        PyErr_Format(PyExc_ImportError,
            "%.200s does not export expected C function %.200s",
                PyModule_GetName(module), funcname);
        goto bad;
    }
#if PY_VERSION_HEX >= 0x02070000
    if (!PyCapsule_IsValid(cobj, sig)) {
        PyErr_Format(PyExc_TypeError,
            "C function %.200s.%.200s has wrong signature (expected %.500s, got %.500s)",
             PyModule_GetName(module), funcname, sig, PyCapsule_GetName(cobj));
        goto bad;
    }
    tmp.p = PyCapsule_GetPointer(cobj, sig);
#else
    {const char *desc, *s1, *s2;
    desc = (const char *)PyCObject_GetDesc(cobj);
    if (!desc)
        goto bad;
    s1 = desc; s2 = sig;
    while (*s1 != '\0' && *s1 == *s2) { s1++; s2++; }
    if (*s1 != *s2) {
        PyErr_Format(PyExc_TypeError,
            "C function %.200s.%.200s has wrong signature (expected %.500s, got %.500s)",
             PyModule_GetName(module), funcname, sig, desc);
        goto bad;
    }
    tmp.p = PyCObject_AsVoidPtr(cobj);}
#endif
    *f = tmp.fp;
    if (!(*f))
        goto bad;
    Py_DECREF(d);
    return 0;
bad:
    Py_XDECREF(d);
    return -1;
}
#endif


static int import_pycylon__data__compute(void) {
  PyObject *module = 0;
  module = PyImport_ImportModule("pycylon.data.compute");
  if (!module) goto bad;
  if (__Pyx_ImportFunction_0_29_37(module, "c_filter", (void (**)(void))&__pyx_api_f_7pycylon_4data_7compute_c_filter, "PyObject *(PyObject *, PyObject *)") < 0) goto bad;
  Py_DECREF(module); module = 0;
  return 0;
  bad:
  Py_XDECREF(module);
  return -1;
}

#endif /* !__PYX_HAVE_API__pycylon__data__compute */
