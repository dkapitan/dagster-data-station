from pathlib import Path

import cbsodata
import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_duckdb_polars import DuckDBPolarsIOManager

datalake = Path(__file__).parent.parent.parent.parent
database_resource = DuckDBPolarsIOManager(database= (datalake /"lakehouse.duckdb").as_posix())

class CBSResource(dg.ConfigurableResource):
    get_info = cbsodata.get_info
    get_data = cbsodata.get_data



@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"duckdb": database_resource, "cbs": CBSResource})
