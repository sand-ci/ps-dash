import dask.dataframe as dd
import traceback
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils.helpers import DATE_FORMAT

class Parquet(object):
    
    @staticmethod
    def writeToFile(df, filename):
        # Convert date columns to the desired format
        for col in df.select_dtypes(include=['datetime']):
            df[col] = df[col].dt.strftime(DATE_FORMAT)
        table = pa.Table.from_pandas(df, preserve_index=True)
        pq.write_table(table, filename)

    @staticmethod
    def readSequenceOfFiles(location, prefix):
        try:
            files = glob.glob(f"{location}{prefix}*")
            df = dd.read_parquet(files).compute()
            # Convert date columns to datetime objects
            for col in df.select_dtypes(include=['object']):
                try:
                    df[col] = pd.to_datetime(df[col], format=DATE_FORMAT)
                except ValueError:
                    pass
            return df
        except Exception as e:
            print(traceback.format_exc())
    
    @staticmethod
    def readFile(filename):
        try:
            df = dd.read_parquet(filename).compute()
            # Convert date columns to datetime objects
            for col in df.select_dtypes(include=['object']):
                try:
                    df[col] = pd.to_datetime(df[col], format=DATE_FORMAT)
                except ValueError:
                    pass
            return df
        except FileNotFoundError:
            print(f"{filename} not found.")
            return dd.from_pandas(pd.DataFrame())
        except Exception as e:
            print(traceback.format_exc())
