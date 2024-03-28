from pyspark.sql.streaming.listener import QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent, StreamingQueryListener

class StreamingListener(StreamingQueryListener):
    def __log(self, msg):
        print(f"[Streaming Query Listener] {msg}")

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        self.__log("Query Started")
        return super().onQueryStarted(event)

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        self.__log("Query Progress")
        return super().onQueryProgress(event)

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        self.__log("Query Idle")
        return super().onQueryIdle(event)

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        self.__log("Query Terminated")
        return super().onQueryTerminated(event)