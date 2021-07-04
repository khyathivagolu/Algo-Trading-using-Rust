use polars::frame::groupby::resample::SampleRule;
use polars::prelude::*;

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
    .sort("datetime", false)?;
  let df_15m = df
    .downsample("datetime", SampleRule::Minute(15))?
    .agg(&agg_params)?
    .sort("datetime", false)?;
  let df_1d = df
    .downsample("datetime", SampleRule::Day(1))?
    .agg(&agg_params)?
    .sort("datetime", false)?;

  println!("5 Min OHLC: {:?}", df_5m);
  println!("15 Min OHLC: {:?}", df_15m);
  println!("1 Day OHLC: {:?}", df_1d);

  let df_samples = DataFrame::new(vec![
    // train_x
    
    Series::new("ohlc_1d_0_open", &[0]),
    Series::new("ohlc_1m_0_open", &[0]),
    Series::new("ohlc_1m_0_close", &[0]),
    Series::new("ohlc_1d_1_close", &[0]),
    Series::new("ohlc_1d_1_high", &[0]),
    Series::new("ohlc_1d_1_low", &[0]),
    Series::new("ohlc_1d_1_open", &[0]),
    Series::new("ohlc_1d_2_close", &[0]),
    Series::new("ohlc_1d_2_high", &[0]),
    Series::new("ohlc_1d_2_low", &[0]),
    Series::new("ohlc_1d_2_open", &[0]),
    Series::new("ohlc_1d_3_close", &[0]),
    Series::new("ohlc_1d_3_high", &[0]),
    Series::new("ohlc_1d_3_low", &[0]),
    Series::new("ohlc_1d_3_open", &[0]),
    Series::new("ohlc_1d_4_close", &[0]),
    Series::new("ohlc_1d_4_high", &[0]),
    Series::new("ohlc_1d_4_low", &[0]),
    Series::new("ohlc_1d_4_open", &[0]),
    Series::new("ohlc_1d_5_close", &[0]),
    Series::new("ohlc_1d_5_high", &[0]),
    Series::new("ohlc_1d_5_low", &[0]),
    Series::new("ohlc_1d_5_open", &[0]),
    // Series::new("ohlc_1d_6_close", &[0]),
    // Series::new("ohlc_1d_7_close", &[0]),

    // train_y
    Series::new("movement_3_pec", &[0.111]),
    Series::new("movement_7_pec", &[0]),
    Series::new("movement_15_pec", &[0]),
  ])
  .unwrap();

  // let name: str = "movement_3_pec";
  println!("df_samples: {:?}, {:?}", df_samples.column("movement_3_pec")?.get(0), df_samples.shape().0);

  let offset_days = 7;
  let sample_days = df_1d.shape().0;
  let mut sampels = vec![];

  let s_h: &Series = df_1d.column("high_max")?;
  let s_l: &Series = df_1d.column("low_min")?;
  let s_o: &Series = df_1d.column("open_first")?;

  let movement_up = &(s_h - s_o) / s_o;
  let movement_down = &(s_o - s_l) / s_o;

  println!("df_diff::::::: {:?}, {:?}", movement_up, movement_down);

  // let values = df.column("values").unwrap();
  // let mask = values.lt_eq(1) | values.gt_eq(5);

  // df.may_apply("foo", |s| {
  //   s.utf8()?
  //   .set(&mask, Some("not_within_bounds"))
  // });

  for i in (offset_days..sample_days).into_iter(){
    // let h = df_1d.column("high_max")?.get(i);
    // let l = df_1d.column("low_min")?.get(i);
    // let cur_p = df_1d.column("open_first")?.get(i);
    // let pec_3 = 0; // (h - cur_p)/cur_p;
    // let pec_7 = 0; // (h - cur_p)/cur_p;
    // let pec_15 = 0; // (h - cur_p)/cur_p;

    sampels.push(vec![
      // train_x
      df_1d.column("open_first")?.get(i),   // ohlc_1d_0_open
      df_1d.column("open_first")?.get(i),   // ohlc_1m_0_open
      df_1d.column("open_first")?.get(i),   // ohlc_1m_0_close
      df_1d.column("close_last")?.get(i-1), // ohlc_1d_1_close
      df_1d.column("high_max")?.get(i-1),   // ohlc_1d_1_high
      df_1d.column("low_min")?.get(i-1),    // ohlc_1d_1_low
      df_1d.column("open_first")?.get(i-1), // ohlc_1d_1_open
      df_1d.column("close_last")?.get(i-2), // ohlc_1d_2_close
      df_1d.column("high_max")?.get(i-2),   // ohlc_1d_2_high
      df_1d.column("low_min")?.get(i-2),    // ohlc_1d_2_low
      df_1d.column("open_first")?.get(i-2), // ohlc_1d_2_open
      df_1d.column("close_last")?.get(i-3), // ohlc_1d_3_close
      df_1d.column("high_max")?.get(i-3),   // ohlc_1d_3_high
      df_1d.column("low_min")?.get(i-3),    // ohlc_1d_3_low
      df_1d.column("open_first")?.get(i-3), // ohlc_1d_3_open
      df_1d.column("close_last")?.get(i-4), // ohlc_1d_4_close
      df_1d.column("high_max")?.get(i-4),   // ohlc_1d_4_high
      df_1d.column("low_min")?.get(i-4),    // ohlc_1d_4_low
      df_1d.column("open_first")?.get(i-4), // ohlc_1d_4_open
      df_1d.column("close_last")?.get(i-5), // ohlc_1d_5_close
      df_1d.column("high_max")?.get(i-5),   // ohlc_1d_5_high
      df_1d.column("low_min")?.get(i-5),    // ohlc_1d_5_low
      df_1d.column("open_first")?.get(i-5), // ohlc_1d_5_open

      // train_y
      // pec_3,
      // pec_7,
      // pec_15
    ])
  }

  //println!("{:?}", sampels);

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
  }

  Ok(())
}

fn main() {
  let path = "data/algo_trading_dataset.csv"; // "data/algo_trading_dataset_mini.csv";
  if let Err(e) = load_and_parse_csv(&path) {
    eprintln!("{}", e);
  }
}