from tqdm import tqdm
class TqdmFromOne(tqdm):
    def __init__(self, iterable, **kwargs):
        super().__init__(iterable, **kwargs)
        self.n = 1  # Set initial counter to 1

