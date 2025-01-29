class DatabaseShutDownException(Exception):
    pass


class ConnectionClosedException(Exception):
    pass


class RestartConsumerError(Exception):
    pass
