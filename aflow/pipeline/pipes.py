import logging
import os

import dask

from aflow.pipeline.utils import pickle_load, pickle_save


class Pipeline:
    def __init__(self, pipeline_name="pipe.p", pipeline_path="pipelines/"):
        if pipeline_name[:-2] != ".p":
            pipeline_name += ".p"

        self.pipeline_name, self.pipeline_path = pipeline_name, pipeline_path

        if not os.path.exists(self.pipeline_path):
            os.makedirs(self.pipeline_path)

        self.pipes = []
        self.computed = None

    def compose(self, *blocks):
        for b in blocks:
            if isinstance(b, self.__class__):
                self.pipes += b.pipes
            elif isinstance(b, list):
                self.pipes += b
            else:
                self.pipes.append(b)

        return self

    def _get_priors_recursively(self, block):
        priors = block.priors
        save = [block]

        if len(priors) == 0:
            return save

        for prior in priors:
            inner_priors = self._get_priors_recursively(prior)
            save.append(inner_priors)

        return save

    def _get_prior_map(self):
        prior_map = dict()
        for b in self.pipes:
            prior_map[b] = list(b.priors)

        return prior_map

    def _get_prior_map_recursivly(self, block):
        rec = self._get_priors_recursively(block)

        root, *tail = rec
        node = root
        q = [[node, *tail]]

        prior_map = dict()
        while q:
            parent, *children = q.pop()

            prior_map[parent] = []

            for child in children:
                if isinstance(child, list):
                    head, *tail = child
                    node = head
                    q.append([node, *tail])
                else:
                    node = head

                prior_map[parent].append(node)

        return prior_map

    def fit(self, idx=-1, visualize=False, **vis_kwargs):
        for b in self.pipes:
            b._run()

        prior_map = self._get_prior_map()

        for i, b in enumerate(self.pipes):
            b.block_idx = i

            key = b
            nodes = prior_map[key]

            nodes = [n.delayed() for n in nodes]
            b.delayed = b.delayed(
                *nodes,
                dask_key_name=b.__class__.__name__ + "\n" + str(b.__repr__)[-15:-1]
                if b.block_name is None
                else b.block_name,
            )

            for k, v_lst in prior_map.items():
                for i, v in enumerate(v_lst):
                    if str(key) == str(v):
                        prior_map[k][i] = b.delayed

        lazy = self.pipes[idx].delayed

        if visualize:
            lazy.visualize(filename=self.pipeline_path + self.pipeline_name[:-4] + ".pdf", **vis_kwargs)

        self.computed = lazy.compute()

        return self

    def save(self):
        prior_map = self._get_prior_map_recursivly(self.computed)
        prior_map_lst = list(prior_map.keys())
        prior_map_idx = [b.block_idx for b in prior_map_lst]

        sort_zip = sorted(zip(prior_map_idx, prior_map_lst))
        sorted_blocks = [pair[1] for pair in sort_zip]

        self.pipes = sorted_blocks

        pickle_save(
            self.pipeline_path,
            self.pipeline_name,
            self.__dict__,
        )
        logging.info(f"Saving pipe to {self.pipeline_path + self.pipeline_name}")

        return self

    def load(self):
        saved_pipeline = pickle_load(
            self.pipeline_path,
            self.pipeline_name,
        )
        logging.info(f"Loading pipe from {self.pipeline_path + self.pipeline_name}")

        loaded_pipes = saved_pipeline["pipes"]

        if len(self.pipes) == len(loaded_pipes):
            for i in range(len(self.pipes)):
                self.pipes[i].__dict__.update(loaded_pipes[i].__dict__)
        else:
            logging.warning("Number of composed and loaded pipes did not match! Loading anyway...")
            self.__dict__.update(saved_pipeline)

        return self

    def __add__(self, prior_pipeline):
        self.pipes += prior_pipeline.pipes
        return self


class Block:
    def __init__(self):
        self.priors = []  # initialized by take method
        self.delayed = None
        self.block_idx = None
        self.fit_block = True
        self.block_name = None

    def set_name(self, name):
        self.block_name = name
        return self

    def _get_prior_blocks(self, *args):
        prior_blocks = []

        for i, arg in enumerate(args):
            for _, v in arg.dask.items():
                if type(v[0]) == type(self.priors[i]):
                    prior_blocks.append(v[0])

        return prior_blocks

    def setup(self, *args):
        """Method for parameter sharing between this (current) block and prior blocks.

        The idea is to update the values of None valued parameters in the current block to the values of the same but
        not None parameters in the prior blocks. Changed parameters need to be present in both compared objects.

        Note
        ----
        - Subtle point: prior values should always remain constant and should never be changed because they get overriden
        in the dict update. Only exception is the `results` attrtibute that gets accumulated in a list.
        - Prior blocks are represented by Block objects.
        - Dask delayed objects are passed as args to the setup method from the run method. Args are thus a tuple of delayed
        objects wrapping Block objects implementing run method via __call__.

        Returns
        -------
        self

        """

        self.priors = self._get_prior_blocks(*args)
        prior_dict = dict()

        for d in self.priors:
            for key, val in d.__dict__.items():
                # Acumulate results as result list; results from blocks get transformed to list!
                if key == "results":
                    if key not in prior_dict:
                        prior_dict[key] = list()
                    prior_dict[key].append(val)
                else:
                    prior_dict[key] = val

        for k, v in self.__dict__.items():
            if v is None and k in prior_dict:
                self.__dict__[k] = prior_dict[k]

        self.fit_block = False
        logging.info(f"setup {self} block, fitting...")

        return self

    def take(self, *priors):
        self.priors = priors
        return self

    def run(self, *args):
        pass

    def _run(self):
        self.delayed = dask.delayed(self)
        return self

    def __call__(self, *args, **kwargs):
        if self.fit_block:
            self.run(*args, **kwargs)
        else:
            logging.info(f"block {self} already fitted, skipping...")
        return self
