from .error import TypeMergeError


class TypeResolveStrategy:
    @staticmethod
    def resolve(type1, type2):
        """
        This method will
        return PySpark Type Object when types are resolved
        or return None when types cannot be resolved
        """
        raise RuntimeError("Not implemented error")


class TypeResolver:
    def __init__(self, resolvers=None):
        self.resolvers = resolvers if resolvers else []

    def register(self, resolver: TypeResolveStrategy):
        self.resolvers.append(resolver)

    def resolve(self, type1, type2):
        for resolver in self.resolvers:
            new_type = resolver.resolve(type1, type2)
            if new_type:
                return new_type

        raise TypeMergeError(f"Schema {type1} and {type2} cannot be merged")


def is_null(t):
    return t.typeName() in ["null", "void"]


def is_number(t):
    return t.typeName() in ["integer", "double"]


def is_string(t):
    return t.typeName() in ["string"]


class NullTypeResolveStrategy(TypeResolveStrategy):
    @staticmethod
    def resolve(type1, type2):
        if is_null(type1) and not is_null(type2):
            return type2
        if not is_null(type1) and is_null(type2):
            return type1


class NumberTypeResolveStrategy(TypeResolveStrategy):
    @staticmethod
    def resolve(type1, type2):
        if is_string(type1) and is_number(type2):
            return type1
        if is_number(type1) and is_string(type2):
            return type2
