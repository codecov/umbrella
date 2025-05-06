import os


class BaseTestCase:
    def readfile(self, filename, if_empty_write=None):
        with open(os.path.join(os.getcwd(), filename)) as r:
            contents = r.read()

        # codecov: assert not covered start [FUTURE new concept]
        if contents.strip() == "" and if_empty_write:
            with open(os.path.join(os.getcwd(), filename), "w+") as r:
                r.write(if_empty_write)
            return if_empty_write
        # codecov: end

        return contents
