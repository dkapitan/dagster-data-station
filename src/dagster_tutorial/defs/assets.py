import yaml

import dagster as dg
from dagster_polars import PolarsParquetIOManager
import polars as pl


@dg.asset
def assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...

def build_cbs_job(table_id: str) -> dg.Definitions:
    asset_key = f"_{table_id}"
    
    @dg.asset(name=asset_key, io_manager_key="cbs_polars_parquet_io_manager", )
    def build_cbs_asset(context):
        df = pl.DataFrame(context.cbs.get_data(table_id))
        context.cbs_polars_parquet_io_manager.write_df_to_path(df)
    
    return dg.Definitions(assets=[build_cbs_asset],)


def load_cbs_job_from_yaml(yaml_path: str) -> dg.Definitions:
    config = yaml.safe_load(open(yaml_path))
    defs = []
    for table in config["tables"]:
        defs.append(build_cbs_job(table))
    return dg.Definitions.merge(*defs)


@dg.definitions
def defs():
    return load_cbs_job_from_yaml("cbs_load_job.yaml")


# TO DO:
# - Create asset factory for CBS tables
# - implement ADBC and/or Ducklake
# - join CBS tables for groepen landbouwgebieden

