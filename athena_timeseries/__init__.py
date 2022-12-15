from typing import Optional, Dict, List
import pandas as pd

from .sql.resample import resample_query, Expr
from .sql.basic import query

from .uploader import upload


__all__ = ["AthenaTimeSeries"]


class AthenaTimeSeries:
    def __init__(self, boto3_session, glue_db_name: str, s3_path: str):
        self.boto3_session = boto3_session
        self.glue_db_name = glue_db_name
        self.s3_path = s3_path

    def query(
        self,
        *,
        table_name: str,
        field: str,
        symbols: Optional[List[str]] = None,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
    ) -> pd.DataFrame:
        return query(
            boto3_session=self.boto3_session,
            glue_db_name=self.glue_db_name,
            table_name=table_name,
            field=field,
            symbols=symbols,
            start_dt=start_dt,
            end_dt=end_dt,
        )

    def resample_query(
        self,
        *,
        table_name: str,
        field: str,
        symbols: Optional[List[str]] = None,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
        interval: str = "day",
        tz: Optional[str] = None,
        op: str = "last",
        where: Optional[Expr] = None,
        cast: Optional[str] = None,
        verbose: int = 0,
        fast: bool = True,
        offset_repr: Optional[str] = None,
    ) -> pd.DataFrame:
        return resample_query(
            boto3_session=self.boto3_session,
            glue_db_name=self.glue_db_name,
            table_name=table_name,
            field=field,
            symbols=symbols,
            start_dt=start_dt,
            end_dt=end_dt,
            interval=interval,
            tz=tz,
            op=op,
            where=where,
            cast=cast,
            verbose=verbose,
            fast=fast,
            offset_repr=offset_repr,
        )

    def upload(
        self,
        *,
        table_name: str,
        df: pd.DataFrame,
        dtype: Optional[Dict[str, str]] = None,
    ):
        return upload(
            boto3_session=self.boto3_session,
            glue_db_name=self.glue_db_name,
            s3_path=self.s3_path,
            table_name=table_name,
            df=df,
            dtype=dtype,
        )
