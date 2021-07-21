//xgboost implementation on 1min ohlc to predict the close value of the 5th candle using open, high, low and volume.
//prediction error is also written to the csv.
//crates used:
//xgboost = "0.1.4"
//csv = "1.1"

extern crate xgboost;
//use std::io;
use std::error::Error;
use std::process;
use xgboost::{parameters, Booster, DMatrix};

type Record = (String, String, f32, f32, f32, f32, f32); //Using serde to parse csv data given in the file into various datatypes.

fn run() -> Result<(), Box<dyn Error>> {
    let mut reader = csv::Reader::from_path("./algo_trading_dataset.csv")?; //loading csv file from given dataset
    let mut wtr = csv::Writer::from_path("./output.csv")?;

    //wtr.write_record(&["id","datetime","open","high","low","close","volume","predicted_close"])?;
    wtr.write_record(&["id", "datetime", "close", "predicted_close", "rms_error"])?;

    let mut x_train = [0.0f32; 20]; // (n-1)*4 for nth candle prediction
    let mut y_train = [0.0f32; 4]; // n-1
    let mut x_test = [0.0f32; 5];
    let mut y_test = [0.0f32; 1];
    let mut _rms_error: f32;
    let mut perc_error: f32;

    let mut count: usize = 1;
    let num_rows_train = 4; // n-1
    let num_rows_test = 1;

    for result in reader.deserialize() {
        let record: Record = result?;
        if record.0 == "nse:britannia" {
            if count % 5 != 0 {
                // count % n for nth candle prediction

                x_train[(count - 1) * 4 + 0] = record.2; //open
                x_train[(count - 1) * 4 + 1] = record.3; //high
                x_train[(count - 1) * 4 + 2] = record.4; //low
                x_train[(count - 1) * 4 + 3] = record.5; //close
                x_train[(count - 1) * 4 + 4] = record.6; //volume

                y_train[count - 1] = record.5; //close
            }
            if count % 5 == 0 {
                x_test[0] = record.2; //open
                x_test[1] = record.3; //high
                x_test[2] = record.4; //low
                x_test[3] = record.5; //close
                x_test[4] = record.6; //volume

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
                perc_error = (predict - record.5) / record.5 * 100.0;

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
                count = 1;
            }
            count = count + 1;
        }
    }
    wtr.flush()?;
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        //running the function along with error handling
        println!("{}", err);
        process::exit(1);
    }
}
