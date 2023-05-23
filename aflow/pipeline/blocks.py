from aflow.pipeline.pipes import Block


class AggregateResultsBlock(Block):
    def __init__(self, results=None):
        super().__init__()
        self.results = results
        self.aggregated_results = []

    def run(self, *args):
        self.setup(*args)

        for res in self.results:
            self.aggregated_results.append(res)

        return self

    # def __getstate__(self):
    #     attributes = self.__dict__.copy()
    #     attributes["results"] = None
    #     attributes["aggregated_results"] = None
    #     return attributes
