

def q05(x):
	return x.quantile(0.05)

def q25(x):
	return x.quantile(0.25)
def q50(x):
	return x.quantile(0.50)
def q75(x):
	return x.quantile(0.75)

def q95(x):
	return x.quantile(0.95)




stat_agg_cols = [
	"min",
	"max",
	"mean",
	"sum",
	"median",
	"std",
	q05,
	q25,
	q50,
	q75,
	q95,
]

def stats_by_dt(df, columns=["value"], date_col="dt", add_date_col=True):
	agg_config = {}

	for c in columns:
		agg_config[c] = stat_agg_cols

	grouped_by_dt = df.groupby(by=date_col).agg(agg_config)

	flattened_cols = ["".join(col).strip() for col in df.columns.values]


	grouped_by_dt.columns = [
		".".join(a) for a in grouped_by_dt.columns.to_flat_index()
	]
	if add_date_col:
		grouped_by_dt["date"] = grouped_by_dt.index

	return grouped_by_dt

