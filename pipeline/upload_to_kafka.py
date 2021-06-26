import json

from kafka import KafkaProducer
from luigi import Parameter, WrapperTask

from pipeline.fighter.aggregator import FighterAggregatorTask


class UploadToKafkaTask(WrapperTask):
    tasks = [FighterAggregatorTask]
    kafka_host = Parameter()  # type: str
    kafka_port = Parameter()  # type: str
    instance_name = Parameter()  # type: str
    project_name = Parameter()  # type: str
    topic = Parameter()  # type: str

    kafka_client = KafkaProducer(
        bootstrap_servers=f"{instance_name}-{project_name}.{kafka_host}:{kafka_port}",
        value_serializer=lambda v: json.dumps(v).encode('ascii'),
        key_serializer=lambda v: json.dumps(v).encode('ascii')
    )

    def requires(self):
        for task in self.tasks:
            result = task()
            yield result
            self.kafka_client.send(topic=self.topic, value=json.load(result.output().open("r")))
