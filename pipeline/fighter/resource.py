from luigi import ExternalTask, LocalTarget


class FighterDocumentsResource(ExternalTask):
    def output(self):
        return LocalTarget(path="mock_data/fighter.json")
