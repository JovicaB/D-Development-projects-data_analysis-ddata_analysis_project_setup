from abc import ABC, abstractmethod
import pandas as pd

class DataHandler(ABC):
    @abstractmethod
    def load_data(self, *args, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def save_data(self, data: pd.DataFrame, *args, **kwargs) -> None:
        pass