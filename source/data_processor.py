from tqdm import tqdm
import pandas as pd
import numpy as np

from utils.cli import break_
from utils.cli import clear_


class DataProcessor:
    def __init__(self) -> None:
        self.my_norm = None
        self.sample_sheet = None
        self.var_threshold = None
        self.init_cgs_number = None

    def load_data(self, my_norm_path: str, sample_sheet_path: str) -> None:
        self.my_norm = pd.read_csv(my_norm_path, index_col=0, encoding='latin-1').T
        self.init_cgs_number = self.my_norm.shape[1]
        self.sample_sheet = pd.read_csv(sample_sheet_path, index_col=0, encoding='latin-1')
        self.sample_sheet.columns = self.sample_sheet.columns.str.lower()

    def check_data_view(self) -> None:
        print("MyNorm view:")
        print(self.my_norm.head())
        print(f"Mynorm shape [n_rows, n_cols] = {self.my_norm.shape}")

        print("Sample sheet view:")
        print(self.sample_sheet.head())
        print(f"Sample sheet shape [n_rows, n_cols] = {self.sample_sheet.shape}")

        status = input("Is it correct view ? [y/n]: ").lower()
        if status == "n":
            break_(msg="Cli was stopped, because of wrong data form.")

    def get_column_to_split(self) -> str:

        while True:
            features = " | ".join(self.sample_sheet.columns.tolist())
            print(f"Sample sheet features: {features}")
            column_to_split = input("Select column (from above list) to split dataset into groups: ").lower()

            if column_to_split in self.sample_sheet.columns:
                break
            else:
                print("Columns does not exists!")
                clear_()
                continue

        return column_to_split

    def merge(self, column_to_split: str) -> None:
        self.sample_sheet = self.sample_sheet[column_to_split].to_frame()
        self.my_norm = pd.concat((self.my_norm, self.sample_sheet), axis=1, sort=False)

    def split_data(self, column_to_split: str) -> dict:
        frames_of_data = {}
        for unique_class in tqdm(self.my_norm[column_to_split].unique()):
            frame = self.my_norm[self.my_norm[column_to_split] == unique_class]
            frame = frame.drop(column_to_split, axis=1)
            frames_of_data.update({unique_class: frame})

        return frames_of_data

    def universal_merge(self, list_of_frames: list) -> None:
        cgs = [set(data.columns) for data in list_of_frames]
        overlapped_cgs = list(set.intersection(*cgs))
        list_of_frames = [data[overlapped_cgs] for data in list_of_frames]
        self.my_norm = pd.concat(list_of_frames, axis=0, sort=False)

    def save_data(self, output_file_path: str) -> None:
        self.my_norm.T.to_csv(output_file_path)

    @staticmethod
    def reduce(my_norm: pd.DataFrame, selection_type=0) -> pd.DataFrame:
        var_per_cg = my_norm.var()

        if selection_type == 0:
            var_threshold = np.quantile(var_per_cg, 0.5)
            mask = [True if var < var_threshold else False for var in var_per_cg]
        else:
            var_threshold = np.quantile(var_per_cg, 0.5)
            mask = [True if var > var_threshold else False for var in var_per_cg]

        reduced_mynorm = my_norm.loc[:, mask]

        if reduced_mynorm.empty:
            break_("Reduction cause empty matrix!")
        else:
            return reduced_mynorm

    @property
    def stats(self) -> str:
        before_filtering = self.init_cgs_number
        after_filtering = self.my_norm.shape[1]
        difference = 100 - round(after_filtering / before_filtering * 100, 2)

        return (f"Before filtering beta-values matrix has {before_filtering} CGs, "
                f"after filtering {after_filtering} CGs, the size of the matrix "
                f"has been reduced by {difference}%.")
