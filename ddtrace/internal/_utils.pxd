cdef extern from "_utils.h":
    cdef inline int PyBytesLike_Check(object o)
    cdef inline char* PyObject_Copy_Str(object o)
