//Sorting given dataset
//conversion of 1min ohlc to 5min and implementation of xgboost on 5min ohlc to predict the close value of every 5th candle in the infy stock using indicators ema 9 and ema 21
//prediction error is also written to the csv
//crates used:
//xgboost = "0.1.4"
//csv = "1.1"

//use std::io;
//#[derive(Debug)]
use csv::ReaderBuilder;
use std::error::Error;
use std::process;
//use std::fmt::Debug::fmt;

use chrono::{DateTime, FixedOffset};

struct Record {
    id: String,
    datetime: DateTime<FixedOffset>,
    o: f32,
    h: f32,
    l: f32,
    c: f32,
    v: f32,
}

fn example() -> Result<(), Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new().from_path("./algo_trading_dataset.csv")?;
    let mut wtr = csv::Writer::from_path("./sorted_1min_ohlc.csv")?;
    //type Record = (String, String, f32, f32, f32, f32, f32);
    let mut records: Vec<Record> = Vec::new();
    wtr.write_record(&["id", "datetime", "open", "high", "low", "close", "volume"])?;

    for result in rdr.deserialize() {
        type RecordL = (String, String, f32, f32, f32, f32, f32);
        let record: RecordL = result?;
        let datetime = DateTime::parse_from_str(&record.1, "%Y-%m-%d %T%:z")?;
        if record.0 == "nse:infy" {
            let id = record.0;
            //let datetime = record.1;
            let o = record.2;
            let h = record.3;
            let l = record.4;
            let c = record.5;
            let v = record.6;
            records.push(Record {
                id,
                datetime,
                o,
                h,
                l,
                c,
                v,
            });
        }
    }

    //for record in &records {
    //let record = result?;

    // let date_time = DateTime::parse_from_str(&record.datetime, "%Y-%m-%d %T%:z")?; //2021-03-04 09:15:00+05:30
    records.sort_by(|record_a, record_b| record_a.datetime.cmp(&record_b.datetime));
    //println!("{}", date_time);
    //println!("{:?}", record);
    println!("records size: {}", records.len());

    for record in records {
        wtr.write_record(&[
            record.id,
            record.datetime.to_string(),
            record.o.to_string(),
            record.h.to_string(),
            record.l.to_string(),
            record.c.to_string(),
            record.v.to_string(),
        ])?;
    }

    //}
    Ok(())
}

fn frame5() -> Result<(), Box<dyn Error>> {
    type Record = (String, String, f32, f32, f32, f32, f32); //parsing data from csv file
    let mut rdr = csv::Reader::from_path("./algo_trading_dataset.csv")?;
    let mut wtr = csv::Writer::from_path("./5min_ohlc.csv")?;
    wtr.write_record(&["id", "datetime", "open", "high", "low", "close", "volume"])?;

    let mut _count: i32 = 0;
    let mut open5: f32 = 0.0;
    //let mut close15: f64 = 0.0;         //not used as value is written directly
    let mut high5: f32 = 0.0;
    let mut low5: f32 = 0.0;
    let mut volume5: f32 = 0.0;
    let mut datetime: String = "".to_string();

    for result in rdr.deserialize() {
        let record: Record = result?;
        //let mut month: i8 = record.1[5..7].to_string().trim().parse().expect("Not number");
        //let mut year: i32 = record.1[0..4].to_string().trim().parse().expect("Not number");
        let min: i32 = record.1[14..16]
            .to_string()
            .trim()
            .parse()
            .expect("Not number");
        if min % 5 == 0 {
            open5 = record.2; //stores the opening stock value for the 15 minute frame.
            low5 = record.4;
            high5 = record.3;
            volume5 = 0.0;
            datetime = record.1;
        }
        if record.3 >= high5 {
            //records highest value in 15 minute frame.
            high5 = record.3;
        }
        if record.4 <= low5 {
            //records lowest value in 15 mins frame
            low5 = record.4;
        }
        volume5 = volume5 + record.6; //total volume in 15 minutes frame.
        if min % 5 == 4 {
            wtr.serialize((record.0, &datetime, open5, high5, low5, record.5, record.6))?;
        }
    }
    wtr.flush()?;
    Ok(())
}

fn indicators() -> Result<(), Box<dyn Error>> {
    //to calculate indicators for 5min ohlc and storing the data in a csv file

    type Record = (String, String, f32, f32, f32, f32, f32); //parsing data types from csv file
    let mut rdr1 = csv::Reader::from_path("./5min_ohlc.csv")?;
    let mut wtr1 = csv::Writer::from_path("./5min_infy_ohlc_indicators.csv")?;
    wtr1.write_record(&[
        "id", "datetime", "open", "high", "low", "close", "volume", "EMA 9", "EMA 21",
    ])?;

    let mut count1: isize = 0;
    let mut init21: f32 = 0.0;
    let mut ema21: f32 = 0.0;
    let mult21: f32 = 2.0 / 22.0;

    let mut init9: f32 = 0.0;
    let mut ema9: f32 = 0.0;
    let mult9: f32 = 2.0 / 10.0;
    for result in rdr1.deserialize() {
        let record: Record = result?;
        if record.0 == "nse:infy" {
            count1 = count1 + 1;

            if count1 <= 9 {
                //ema 9 calculation 5min ohlc
                init9 = init9 + record.5;
            }
            if count1 == 9 {
                ema9 = init9 / 9.0;
            }
            if count1 > 9 {
                ema9 = ((record.5 - ema9) * mult9) + ema9;
            }

            if count1 <= 21 {
                //ema 21 calculation 5min ohlc
                init21 = init21 + record.5;
            }
            if count1 == 21 {
                ema21 = init21 / 21.0;
            }
            if count1 > 21 {
                ema21 = ((record.5 - ema21) * mult21) + ema21;
            }

            wtr1.serialize((
                record.0, record.1, record.2, record.3, record.4, record.5, record.6, ema9, ema21,
            ))?;
        }
    }
    Ok(())
}

fn main() {
    if let Err(err) = frame5() {
        //running the function along with error handling
        println!("{}", err);
        process::exit(1);
    }
    if let Err(err) = indicators() {
        //running the function along with error handling
        println!("{}", err);
        process::exit(1);
    }
    if let Err(err) = example() {
        //running the function along with error handling
        println!("{}", err);
        process::exit(1);
    }
}
