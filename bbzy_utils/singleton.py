from functools import wraps


def singleton(class_name, bases=None, fields=None):
    original_init_method = fields.get('__init__')
    if original_init_method is None:
        if bases:
            original_init_method = bases[0].__init__
        else:
            original_init_method = object.__init__
    original_new_method = fields.get('__new__')
    if original_new_method is None:
        if bases:
            original_new_method = bases[0].__new__
        else:
            original_new_method = object.__new__

    @wraps(original_init_method)
    def wrap_init(self, *args, **kwargs):
        if self._instance is None:
            # noinspection PyArgumentList
            original_init_method(self, *args, **kwargs)
            self.__class__._instance = self

    @wraps(original_new_method)
    def wrap_new(cls, *args, **kwargs):
        if cls._instance is None:
            # noinspection PyArgumentList
            return original_new_method(cls, *args, **kwargs)
        return cls._instance

    fields['_instance'] = None
    fields['__init__'] = wrap_init
    fields['__new__'] = wrap_new
    return type(class_name, bases, fields)
