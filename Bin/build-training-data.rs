use polars::frame::groupby::resample::SampleRule;
use polars::prelude::*;
//use polars::prelude::LazyFrame;
use polars::frame::*;

use ta::indicators::ExponentialMovingAverage;
use ta::Next;


fn get_ohlc_schema() -> Schema {
  Schema::new(vec![
    Field::new("instrument_id", DataType::Utf8),
    Field::new("datetime", DataType::Utf8),
    Field::new("open", DataType::Float64),
    Field::new("high", DataType::Float64),
    Field::new("low", DataType::Float64),
    Field::new("close", DataType::Float64),
    Field::new("volume", DataType::Int32),
  ])
}

fn str_to_date(dates: &Series) -> std::result::Result<Series, PolarsError> {
  let fmt = Some("%Y-%m-%d %H:%M:%S%z");
  Ok(dates.utf8()?.as_date64(fmt)?.into_series())
}

fn find_ema(df: &DataFrame) -> Result<()> {
  let mut ema9 = ExponentialMovingAverage::new(9).unwrap();
  let mut ema21 = ExponentialMovingAverage::new(21).unwrap();

 // let ema9_df = df.into_series().rolling_mean(5,1,true,2);
  println!("ema 9= {:?}", ema9);
  Ok(())
}

fn process_inst_ohlc_1m(df: &DataFrame) -> Result<()> {
  let agg_params = [
    ("instrument_id", &["first"]),
    ("open", &["first"]),
    ("high", &["max"]),
    ("low", &["min"]),
    ("close", &["last"]),
    ("volume", &["sum"]),
  ];
  let df_5m = df
    .downsample("datetime", SampleRule::Minute(5))?
    .agg(&agg_params)?
    .sort("datetime", false);
  let df_15m = df
    .downsample("datetime", SampleRule::Minute(15))?
    .agg(&agg_params)?
    .sort("datetime", false);
  let df_1d = df
    .downsample("datetime", SampleRule::Day(1))?
    .agg(&agg_params)?
    .sort("datetime", false);

  //println!("5 Min OHLC: {:?}", df_5m);
  //println!("15 Min OHLC: {:?}", df_15m);
  //println!("1 Day OHLC: {:?}", df_1d);
 // use LazyFrame::slice;
  let mut df_5m_train = df_5m.slice(1,1);


  println!("slice: {:?}", df_5m_train);


  Ok(())
}

// Box<dyn Error>
fn load_and_parse_csv(path: &str) -> Result<()> {
  let schema = get_ohlc_schema();
  let mut df = CsvReader::from_path(path)?
    .has_header(true)
    //.with_ignore_parser_errors(true)
    .with_columns(Some(
      schema
        .fields()
        .into_iter()
        .map(|s| s.name().to_string())
        .collect(),
    ))
    .with_schema(Arc::new(schema))
    .finish()?;
  df.may_apply("datetime", str_to_date)?;

  let inst_dfs = df
    .groupby("instrument_id")?
    .get_groups()
    .into_iter()
    .map(|t| df.take_iter(t.1.iter().map(|i| *i as usize)))
    .collect::<Vec<DataFrame>>();

  for inst_df in inst_dfs {
    if let Err(e) = process_inst_ohlc_1m(&inst_df) {
      eprintln!("{}", e);
    }
    if let Err(e) = find_ema(&inst_df) {
      eprintln!("{}", e);
    }
    
  }

  Ok(())
}

fn main() {
  let path = "data/algo_trading_dataset.csv";
  if let Err(e) = load_and_parse_csv(&path) {
    eprintln!("{}", e);
  }
}
