//conversion of 1min ohlc to 5min and implementation of xgboost on 5min ohlc to predict the close value of every 5th candle in the infy stock using indicators ema 9 and ema 21
//prediction error is also written to the csv
//crates used:
//xgboost = "0.1.4"
//csv = "1.1"

extern crate xgboost;

//use std::io;
use std::error::Error;
use std::process;
use xgboost::{parameters, Booster, DMatrix};

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

fn run() -> Result<(), Box<dyn Error>> {
    // function to implement xgboost on 5 min ohlc data
    type Record = (String, String, f32, f32, f32, f32, f32, f32, f32);

    let mut reader = csv::Reader::from_path("./5min_infy_ohlc_indicators.csv")?; //loading csv file from given dataset
    let mut wtr = csv::Writer::from_path("./output.csv")?;

    //wtr.write_record(&["id","datetime","open","high","low","close","volume","predicted_close"])?;
    wtr.write_record(&["id", "datetime", "close", "predicted_close", "error%"])?;

    let mut x_train = [0.0f32; 3 * 4]; // (n-1)*4 for nth candle prediction
    let mut y_train = [0.0f32; 4]; // n-1
    let mut x_test = [0.0f32; 3];
    let mut y_test = [0.0f32; 1];
    let mut _rms_error: f32;
    let mut perc_error: f32;

    let mut count: usize = 1;
    let num_rows_train = 4; // n-1
    let num_rows_test = 1;

    for result in reader.deserialize() {
        let record: Record = result?;

        if record.0 == "nse:infy" {
            if count % 5 != 0 {
                // count % n for nth candle prediction

                x_train[(count - 1) * 3 + 0] = record.7; //ema9
                x_train[(count - 1) * 3 + 1] = record.8; //ema21
                x_train[(count - 1) * 3 + 2] = record.5; //close
                                                         //x_train[(count-1)*4 + 3] = record.5;
                                                         //x_train[(count-1)*4 + 4] = record.6;

                y_train[count - 1] = record.5; //close
            }
            if count % 5 == 0 {
                // count % n for nth candle prediction
                x_test[0] = record.7; //ema20
                x_test[1] = record.8; //ema21
                x_test[2] = record.5; //close
                                      //x_test[3] = record.5; //close
                                      //x_test[4] = record.6; //volume

                y_test[0] = record.5; //close

                let mut dtrain = DMatrix::from_dense(&x_train, num_rows_train).unwrap();
                dtrain.set_labels(&y_train).unwrap();

                let mut dtest = DMatrix::from_dense(&x_test, num_rows_test).unwrap();
                dtest.set_labels(&y_test).unwrap();

                let evaluation_sets = &[(&dtrain, "train"), (&dtest, "test")];

                let training_params = parameters::TrainingParametersBuilder::default()
                    .dtrain(&dtrain)
                    .evaluation_sets(Some(evaluation_sets))
                    .build()
                    .unwrap();

                let bst = Booster::train(&training_params).unwrap();
                let predict: f32 = bst.predict(&dtest).unwrap()[0];
                //ms_error = (predict - record.5)*(predict - record.5);
                perc_error = (predict - record.5) / record.5 * 100.0; //calculation of percentage error of the predicted value

                //wtr.serialize((record.0,record.1,record.2,record.3,record.4,record.5,record.6, bst.predict(&dtest).unwrap()))?;
                wtr.serialize((
                    record.0,
                    record.1,
                    record.5,
                    bst.predict(&dtest).unwrap(),
                    perc_error,
                ))?;
                //println!("{:?}", bst.predict(&dtest).unwrap());
                //println!("hii");
                count = 0; // to keep track of candle number
            }
            count = count + 1;
        }
    }
    wtr.flush()?;
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
    if let Err(err) = run() {
        //running the function along with error handling
        println!("{}", err);
        process::exit(1);
    }
}
