from utils import timestamp_helper


class Test:

    _instance = None

    def __init__(self):
        raise RuntimeError('init Call instance() instead')

    def __new__(self):
        raise RuntimeError('new Call instance() instead')



    def __new__(cls):
        if cls._instance is None:
            print('Creating the object')
            cls._instance = super(Test, cls).__new__(cls)
            cls.name = "_" + timestamp_helper.get_timestamp()
        return cls._instance


    @classmethod
    def instance(cls):
        if cls._instance is None:
            print('Creating new instance of Cosmos Factory')
            cls._instance = cls.__new__(cls)
            # Put any initialization here.
        return cls._instance


    def get_value(self):
        return self.name


if __name__ == '__main__':
    t1 = Test.instance()
    print(t1.get_value())

    t2 = Test.instance()
    print(t2.get_value())


    t3 = Test.instance()
    print(t3.get_value())

    print('Are they the same object?', t1 is t2)