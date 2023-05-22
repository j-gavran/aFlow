import unittest

from dask.distributed import Client

from aflow.examples.simple_blocks import (
    AddBlock,
    AggregateAddBlock,
    DoubleBlock,
    IncBlock,
    ResultsBlock,
    SquareBlock,
)
from aflow.pipeline.pipes import Pipeline


class TestDaskSimpleBlockPipeline(unittest.TestCase):
    def test_dask_basic_pipeline(self):
        """
        x1----x3----
                    |----x5
        x2----x4----
        """
        client = Client(dashboard_address=":8787")

        p = Pipeline()

        x1 = IncBlock(1)

        x2 = DoubleBlock(2)

        x3 = SquareBlock()
        x3.take(x1)

        x4 = SquareBlock()
        x4.take(x2)

        x5 = AddBlock()
        x5.take(x3, x4)

        p.compose(x1, x2, x3, x4, x5)
        p.fit(visualize=True, rankdir="LR")

        res = p.computed

        client.close()

        self.assertEqual(res.results, 16)

    def test_dask_basic_multi_pipeline(self):
        """
        x1----
              |
        x2----|----x5
              |     |----x7
        x3----|----x6
              |
        x4----
        """
        client = Client(dashboard_address=":8787")

        p = Pipeline()

        x1 = IncBlock(1)
        x2 = IncBlock(2)

        x3 = DoubleBlock(1)
        x4 = DoubleBlock(2)

        x5 = AddBlock().take(x1, x3)
        x6 = AddBlock().take(x2, x4)

        x7 = AggregateAddBlock().take(x5, x6)

        p.compose(x1, x2, x3, x4, x5, x6, x7)

        p.fit(visualize=True, rankdir="LR")

        client.close()

    def test_dask_basic_multi_results_pipeline(self):
        """
        x1----
        .     |
        .     |----y
        .     |
        xn----
        """
        client = Client(dashboard_address=":8787")

        p = Pipeline()

        xs1 = [IncBlock(1) for _ in range(10)]
        xs2 = [ResultsBlock().take(x) for x in xs1]
        y = AggregateAddBlock().take(*xs2)

        p.compose(*xs1, *xs2, y)
        p.fit(visualize=True, rankdir="LR")

        res = p.computed

        self.assertEqual(res.sum_results, 20)  # add +1 to xs1 and sum list

        client.close()


if __name__ == "__main__":
    unittest.main()
