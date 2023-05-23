import urllib

from aflow.pipeline.pipes import Block


class DownloadBlock(Block):
    def __init__(self, version=None):
        super().__init__()
        self.version = version
        self.url = f"http://www.loremipsum.de/downloads/version{self.version}.txt"
        self.content = None

    def _download(self):
        with urllib.request.urlopen(self.url) as f:
            content = f.read().decode("utf-8", "ignore")
        return content

    def run(self, *args):
        self.setup(*args)

        self.content = self._download()

        return self


class CountCharsBlock(Block):
    def __init__(self, content=None):
        super().__init__()
        self.content = content
        self.results = None

    def _count_chars(self):
        char_dict = dict()

        for char in self.content:
            if char not in char_dict:
                char_dict[char] = 1
            else:
                char_dict[char] += 1

        return [char_dict, list(char_dict.keys())]

    def run(self, *args):
        self.setup(*args)

        self.results = self._count_chars()

        return self


class CountSingleChars(Block):
    def __init__(self, char, aggregated_results=None):
        super().__init__()
        self.char = char
        self.aggregated_results = aggregated_results

    def _count_single_char(self):
        char_count = 0
        for res in self.aggregated_results:
            for k, v in res[0].items():
                if k == self.char:
                    char_count += v
        return [self.char, char_count]

    def run(self, *args):
        self.setup(*args)

        self.results = self._count_single_char()

        return self
