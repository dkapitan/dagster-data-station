import marimo

__generated_with = "0.16.4"
app = marimo.App()


@app.cell
def _():
    import cbsodata
    import polars as pl


    datasets = ("80780ned", "80783ned", "83982NED", "84071NED")
    info = pl.concat([pl.from_dict(cbsodata.get_info(dataset)) for dataset in datasets])
    info
    return cbsodata, datasets, pl


@app.cell
def _(cbsodata, datasets, pl):
    df = pl.DataFrame(cbsodata.get_data(datasets[0]))
    return (df,)


@app.cell
def _(df):
    df
    return


@app.cell
def _(cbsodata, datasets):
    cbsodata.get_meta(datasets[0], "DataProperties")
    return


@app.cell
def _(df):
    df.select("RegioS").unique()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
