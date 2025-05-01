import datetime
import hashlib
import os

import pandas as pd


class AbstractDataSource:
    __base_dir__ = 'D:/PycharmProjects/WangAI/data'
    __dir_name__ = None

    @classmethod
    def load(
        cls,
        from_date: datetime.date,
        to_date: datetime.date,
        **kwargs
    ) -> pd.DataFrame:
        raise NotImplementedError

    @classmethod
    def _save_to_csv(cls, data: pd.DataFrame, name: str, *args, **kwargs) -> None:
        path = f"{cls.__base_dir__}/{cls.__dir_name__}/"
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        path += f"{name}.csv"
        data.to_csv(path, *args, **kwargs)

    @classmethod
    def _read_from_csv(cls, name: str, *args, **kwargs) -> pd.DataFrame | None:
        file_path = f"{cls.__base_dir__}/{cls.__dir_name__}/{name}.csv"
        try:
            data = pd.read_csv(
                file_path,
                parse_dates=True,
                *args, **kwargs
            )
        except FileNotFoundError:
            data = None
        except OSError:
            data = None
        return data

    @classmethod
    def _build_cache_key(
        cls,
        from_date: datetime.date,
        to_date: datetime.date,
        *args,
        **kwargs,
    ) -> str:
        s = f"{from_date.strftime('%Y-%m-%d')}--{to_date.strftime('%Y-%m-%d')}--"
        s_ = ''
        if args:
            s_ = f"{s_}--{'-'.join(args)}"
        if kwargs:
            kwargs_str = [f'{key}__{value}' for key, value in kwargs.items()]
            s_ = f"{s_}--{'-'.join(kwargs_str)}"
        md5_hash = hashlib.md5(s_.encode('utf-8')).hexdigest()
        return s+md5_hash


