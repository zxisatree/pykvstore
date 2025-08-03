class SingletonMeta(type):
    _instances = {}

    # single threaded at this point, no need for a lock
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
