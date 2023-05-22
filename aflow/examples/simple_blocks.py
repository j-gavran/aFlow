import time

from aflow.pipeline.pipes import Block


class IncBlock(Block):
    def __init__(self, x_in):
        """Incerment block."""
        super().__init__()
        self.x_in = x_in
        self.x = None

    def run(self, *args):
        self.setup(*args)

        time.sleep(4)
        self.x = self.x_in + 1

        return self


class DoubleBlock(Block):
    def __init__(self, x_in):
        """Double block."""
        super().__init__()
        self.x_in = x_in
        self.y = None

    def run(self, *args):
        self.setup(*args)

        time.sleep(5)
        self.y = self.x_in * 2

        return self


class SquareBlock(Block):
    def __init__(self, x=None, y=None):
        """Square x or y block."""
        super().__init__()
        self.x = x
        self.y = y

    def run(self, *args):
        self.setup(*args)

        time.sleep(3)

        if self.x:
            self.x = self.x**2
            self.y = False
        if self.y:
            self.y = self.y**2
            self.x = False

        return self


class AddBlock(Block):
    def __init__(self, x=None, y=None):
        """Addition block."""
        super().__init__()
        self.x = x
        self.y = y

        self.results = None

    def run(self, *args):
        self.setup(*args)

        time.sleep(2)
        self.results = self.x + self.y

        return self


class ResultsBlock(Block):
    def __init__(self, x=None):
        """Renames x to results."""
        super().__init__()
        self.x = x
        self.results = None

    def run(self, *args):
        self.setup(*args)
        self.results = self.x
        return self


class AggregateAddBlock(Block):
    def __init__(self, results=None):
        """Join and add block. Adds results from AddBlocks."""
        super().__init__()
        self.results = results
        self.sum_results = None

    def run(self, *args):
        self.setup(*args)

        time.sleep(1)
        self.sum_results = sum(self.results)

        return self
