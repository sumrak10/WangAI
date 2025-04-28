import datetime
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
        try:
            data = pd.read_csv(
                f"{cls.__base_dir__}/{cls.__dir_name__}/{name}.csv",
                parse_dates=True,
                *args, **kwargs
            )
        except FileNotFoundError:
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
        s = f"{from_date.strftime('%Y-%m-%d')}--{to_date.strftime('%Y-%m-%d')}"
        if args:
            args_str = [str(arg) for arg in args]
            s = f"{s}-{''.join(args_str)}"
        if kwargs:
            kwargs_str = [f'{key}_{value}' for key, value in kwargs.items()]
            s = f"{s}-{''.join(args)}"
        return s

