import polars as pl
def add_electricity_prices(df_data,df_electricity):
    return df_data.join(df_electricity.with_columns((pl.col("forecast_date")+pl.duration(days=1)).alias("forecast_date").cast(pl.Datetime))[["forecast_date","euros_per_mwh"]],right_on="forecast_date",left_on="datetime",how="left")

def add_gas_price(df_data,added_data):
    return df_data.join(added_data.with_columns((pl.col("forecast_date")+pl.duration(days=1)).alias("date").cast(pl.Date))[["date","lowest_price_per_mwh","highest_price_per_mwh"]],on="date",how="left")

def add_client(df_data,added_data):
    join_cols=["county", "is_business", "product_type", "date"]
    used_cols=["eic_count","installed_capacity"]
    return df_data.join(added_data.with_columns((pl.col("date")+pl.duration(days=2)))[join_cols+used_cols],on=join_cols,how="left")

def add_client(df_data,added_data):
    join_cols=["county", "is_business", "product_type", "date"]
    used_cols=["eic_count","installed_capacity"]
    return df_data.join(added_data.with_columns((pl.col("date")+pl.duration(days=2)))[join_cols+used_cols],on=join_cols,how="left")
def add_latest_weather(df_data,df_forecast,df_location):
    added_data = df_forecast.join(df_location[["longitude","latitude","county"]],on=["longitude","latitude"],how="left")
    added_data = added_data.sort(by=["county","latitude","longitude","forecast_datetime"]).unique(subset=["county","latitude","longitude","forecast_datetime"],keep="last")
    added_data = added_data.filter(~pl.all_horizontal(added_data["county"].is_null())).drop(columns=["origin_datetime","latitude","longitude","hours_ahead","data_block_id"]).sort(by=["county","forecast_datetime"])
    added_data = added_data.group_by(by=["county","forecast_datetime"]).mean()
    return df_data.join(added_data,how="left",left_on=["county","datetime"],right_on=["county","forecast_datetime"])

def extend_month_weekday(df_data):
    return df_data.with_columns(
        pl.col("datetime").dt.month().alias("month"),
        pl.col("datetime").dt.weekday().alias("weekday"),
        pl.col("datetime").dt.hour().alias("hour"),
    )
def previous_value(df_data,day=7):
    last_week = df_data[["county","is_business","product_type","datetime","is_consumption","target"]].with_columns(
        (pl.col("datetime") + pl.duration(days=day)).alias("datetime"),
    )
    last_week = last_week.rename({"target":"last_week_target"})
    return df_data.join(last_week,how="left",on=["county","is_business","product_type","datetime","is_consumption"])