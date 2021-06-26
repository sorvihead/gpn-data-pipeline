import json

from luigi import LocalTarget, Task

from pipeline.fighter.resource import FighterDocumentsResource


def aggregate_data(data):
    pass


class FighterAggregatorTask(Task):
    def requires(self):
        return FighterDocumentsResource()

    def run(self):
        with self.input().open() as f:
            data = json.load(f)
        processed_data = aggregate_data(data)

        with self.output().open("w") as f:
            json.dump(processed_data, f)

    def output(self):
        return LocalTarget(path="aggregated-fighter-data.json")
