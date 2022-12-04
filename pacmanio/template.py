import os
import pandas as pd


class Template:

    def __init__(self, path: str = None):

        self.version: str = None
        self.metadata: Sheet = None
        self.environmental: Sheet = None
        self.samples: Sheet = None
        self.vouchers: Sheet = None
        self.images: Sheet = None
        self.sra: Sheet = None
        self.about: Sheet = None

        if path is not None:
            self.path: str = os.path.expanduser(path)
            self.read(self.path)

    def read_metadata(self):
        df = pd.read_excel(self.path, engine="openpyxl", sheet_name="Sampling metadata", header=None, skiprows=1).T
        df.columns = df.iloc[0]
        df.drop(0, inplace=True)
        df = df.iloc[:, 1:]
        self.metadata = Sheet(df=df)

    def read_samples(self):
        df = pd.read_excel(self.path, engine="openpyxl", sheet_name="Samples", header=0, skiprows=1)
        df = df.iloc[1:, 1:]
        df.dropna(how="all", inplace=True)
        self.samples = Sheet(df=df)

    def read_vouchers(self):
        df = pd.read_excel(self.path, engine="openpyxl", sheet_name="Vouchers", header=0, skiprows=1)
        df = df.iloc[1:, 1:]
        df.dropna(how="all", inplace=True)
        self.vouchers = Sheet(df=df)

    def read(self, path: str) -> None:
        if os.path.exists(path) and os.path.exists(path):
            self.read_metadata()
            self.read_samples()
            self.read_vouchers()


class Sheet:

    def __init__(self, df: pd.DataFrame = None):
        self.df = df
