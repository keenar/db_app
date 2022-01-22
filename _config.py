class Struct(object):

    def __init__(self, *args):
        self.__header__ = str(args[0]) if args else None
        self._database = None
        self._dbms = None
        self._password = None

    def __repr__(self):
        if self.__header__ is None:
            return super(Struct, self).__repr__()
        return self.__header__

    def next(self):
        """Fake iteration functionality"""
        raise StopIteration

    def __iter__(self):
        """Fake iteration functionality,
        skip magic attributes and structs and return the rest"""
        ks = self.__dict__.keys()
        for k in ks:
            if not k.startswith('__') and not isinstance(k, Struct):
                yield getattr(self, k)

    def __len__(self):
        """Don't count magic attributes or structs"""
        ks = self.__dict__.keys()
        return len([k for k in ks if not k.startswith('__') \
                    and not isinstance(k, Struct)])

    def get_dbms_var(self):
        return self._dbms

    def set_dbms_var(self, dbms_val):
        self._dbms = dbms_val

    def get_password(self):
        return self._password

    def set_password(self, password_val):
        self._password = password_val

    def get_database(self):
        return self._database

    def set_database(self, db_val):
        self._database = db_val

    # properties
    dbms = property(fget=get_dbms_var, fset=set_dbms_var)
    password = property(fget=get_password, fset=set_password)
    database = property(fget=get_database, fset=set_database)

    # @database.setter
    # def database(self, value):
    #     self._database = value

