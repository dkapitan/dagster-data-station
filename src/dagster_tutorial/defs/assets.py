import yaml

import dagster as dg
from dagster_duckdb_polars import DuckDBPolarsIOManager
import polars as pl


@dg.asset
def assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...

def build_cbs_job(table_id: str, cbs: dg.ConfigurableResource, duckdb: DuckDBPolarsIOManager) -> dg.Definitions:
    asset_key = f"_{table_id}"
    
    @dg.asset(name=asset_key)
    def cbs_asset(context):
        pl.DataFrame(cbs.get_data(table_name))
        return dg.Definitions(assets=[cbs_asset],)



def build_cbs_load_job(table_id: str, cbs: dg.ConfigurableResource, duckdb: DuckDBResource) -> dg.Definitions:
    "Builds CBS tables as assets."
    

    @dg.asset(name=asset_key)
    def load_cbs_job_from_yaml(yaml_path: str) -> dg.Definitions:
        config = yaml.safe_load(open(yaml_path))
        defs = []
        for table in config["tables"]:
            defs.append(
                return pl.DataFrame(cbs.get_data(table_name))

def load_etl_jobs_from_yaml(yaml_path: str) -> dg.Definitions:
    config = yaml.safe_load(open(yaml_path))
    s3_resource = s3.S3Resource(
        aws_access_key_id=config["aws"]["access_key_id"],
        aws_secret_access_key=config["aws"]["secret_access_key"],
    )
    defs = []
    for job_config in config["etl_jobs"]:
        defs.append(
            build_etl_job(
                s3_resource=s3_resource,
                bucket=job_config["bucket"],
                source_object=job_config["source"],
                target_object=job_config["target"],
                sql=job_config["sql"],
            )
        )
    return dg.Definitions.merge(*defs)


@dg.definitions
def defs():
    return load_etl_jobs_from_yaml("cbs_load_job.yaml")


@dg.asset
def build_cbsodata_asset(table_name: str, cbs: dg.ConfigurableResource, duckdb: DuckDBResource):
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

