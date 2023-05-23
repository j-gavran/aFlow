import unittest

from dask.distributed import Client

from aflow.examples.loremipsum import CountCharsBlock, CountSingleChars, DownloadBlock
from aflow.pipeline.blocks import AggregateResultsBlock
from aflow.pipeline.pipes import Pipeline


class TestLoremipsum(unittest.TestCase):
    def test_download(self):
        client = Client(dashboard_address=":8787")

        p = Pipeline()

        b_download = DownloadBlock(version=1)  # [DownloadBlock(version=i) for i in range(1, 6)]

        p.compose(b_download)
        p.fit(visualize=True, rankdir="LR")

        client.close()

    def test_count_chars(self):
        client = Client(dashboard_address=":8787")

        p = Pipeline()

        b_download = DownloadBlock(version=1)
        b_count_chars = CountCharsBlock().take(b_download)

        p.compose(b_download, b_count_chars)
        p.fit(visualize=True, rankdir="LR")

        client.close()

    def test_parallel_count_chars(self):
        client = Client(dashboard_address=":8787")

        p = Pipeline()

        b_download = [DownloadBlock(version=i) for i in range(1, 6)]
        b_count_chars = [CountCharsBlock().take(b) for b in b_download]
        b_aggregate = AggregateResultsBlock().take(*b_count_chars)

        p.compose(b_download, b_count_chars, b_aggregate)
        p.fit(visualize=True, rankdir="LR")

        client.close()

    def test_parallel_single_chars(self):
        client = Client(dashboard_address=":8787")

        # map
        p1 = Pipeline()

        b_download = [DownloadBlock(version=i) for i in range(1, 6)]
        b_count_chars = [CountCharsBlock().take(b) for b in b_download]
        b_aggregate1 = AggregateResultsBlock().take(*b_count_chars)

        p1.compose(b_download, b_count_chars, b_aggregate1)
        p1.fit(visualize=True)

        results = p1.computed.aggregated_results

        # reduce
        p2 = Pipeline()

        all_chars = []
        for r in results:
            all_chars += r[1]

        all_chars = set(all_chars)

        b_single_chars = [CountSingleChars(c).take(b_aggregate1) for c in all_chars]
        b_aggregate2 = AggregateResultsBlock().take(*b_single_chars)

        p2.compose(b_aggregate1, *b_single_chars, b_aggregate2)
        p2.fit(visualize=True)

        loremipsum_counts = p2.computed.aggregated_results

        client.close()


if __name__ == "__main__":
    unittest.main()
