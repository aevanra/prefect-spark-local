class NoFileException(Exception):
    """ Exception to raise when we do not have the output we expect in a directory """

class UpstreamFailedException(Exception):
    """ Exception to raise when upstream task has failed """
