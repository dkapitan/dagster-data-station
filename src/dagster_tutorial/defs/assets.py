import dagster as dg
from dagster_duckdb import DuckDBResource
import polars as pl


@dg.asset
def assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...

@dg.asset
def cbs80780ned(cbs: dg.ConfigurableResource, duckdb: DuckDBResource):
    table_name = "80780ned"
    return pl.DataFrame(cbs.get_data(table_name))
    # with duckdb.get_connection() as conn:
    #     conn.execute(
    #         f"""
    #         create or replace table cbs{table_name} as (
    #             select * from df
    #         )
    #         """
    #     )

# TO DO:
# - Create asset factory for CBS tables
# - implement ADBC and/or Ducklake
# - join CBS tables for groepen landbouwgebieden

