def parse(o, prefix=""):
    def flatten(lis):
        new_lis = []
        for item in lis:
            if isinstance(item, list):
                new_lis.extend(flatten(item))
            else:
                new_lis.append(item)
        return new_lis

    try:
        return {
            "str": lambda: (prefix, o),
            "int": lambda: parse(str(o), prefix=prefix),
            "float": lambda: parse(str(o), prefix=prefix),
            "bool": lambda: parse(1 if o else 0, prefix=prefix),
            "NoneType": lambda: parse("", prefix=prefix),
            "list": lambda: flatten([parse(io, "{}{}{}".format(prefix, "_" if prefix else "", ik).upper()) for ik, io in enumerate(o)]),
            "dict": lambda: flatten([parse(io, "{}{}{}".format(prefix, "_" if prefix else "", ik).upper()) for ik, io in o.items()]),
        }[type(o).__name__]()
    except KeyError:
        raise ValueError("type '{}' not supported".format(type(o).__name__))
