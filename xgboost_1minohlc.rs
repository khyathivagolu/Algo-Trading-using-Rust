//xgboost implementation on 1min ohlc to predict the close value of the 5th candle using open, high, low and volume.
//the prediction is not very accurate.
//crates used: 
//xgboost = "0.1.4"
//csv = "1.1"

extern crate xgboost;
//use std::io;
use std::process;
use std::error::Error;


use xgboost::{parameters, DMatrix, Booster};
//use xgboost::DMatrix;

//let dmat = DMatrix::load("somefile.txt").unwrap();

type Record = (String, String, f32, f32, f32, f32, f32); //Using serde to parse csv data given in the file into various datatypes.



fn run() -> Result<(), Box<dyn Error>> {
    let mut reader = csv::Reader::from_path("./algo_trading_dataset.csv")?; //loading csv file from stdin.
    let mut wtr = csv::Writer::from_path("./output.csv")?;


    wtr.write_record(&["id","datetime","open","high","low","close","volume","predicted_close"])?;
    /*let mut x_train: Vec<f32> = Vec::new();
    let mut y_train: Vec<f32> = Vec::new();
    let mut x_test: Vec<f32> = Vec::new();
    let mut y_test: Vec<f32> = Vec::new(); */


    let mut x_train = [0.0f32; 16];
    let mut y_train = [0.0f32; 4];
    let mut x_test = [0.0f32; 4];
    let mut y_test = [0.0f32; 1];
    //state[0][1] = 42;

    let mut count: usize = 1;
    let num_rows_train = 4;
    let num_rows_test = 1;

    for result in reader.deserialize() {
        let record: Record = result?;
        //println!("{:?}", record); 

        if count % 5 != 0{

        x_train[(count-1)*4 + 0] = record.2;
        x_train[(count-1)*4 + 1] = record.3;
        x_train[(count-1)*4 + 2] = record.4;
        x_train[(count-1)*4 + 3] = record.6;
       /* x_train.push(record.0);
        x_train.push(record.1);
        x_train.push(record.2);
        x_train.push(record.4); */

        y_train[count-1] = record.5;

        }
        if count % 5 == 0 {
            x_test[0] = record.2;
            x_test[1] = record.3;
            x_test[2] = record.4;
            x_test[3] = record.6;

            y_test[0] = record.5;
            //use rustlearn::prelude::*;

            //let mut array_xn = Array::from(x_train);
            //array_xn.reshape(4, 4);

            //let mut array1: = demo(x_train);
            //array.reshape(4, 4);

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
            
            let mut bst = Booster::train(&training_params).unwrap();

            wtr.serialize((record.0,record.1,record.2,record.3,record.4,record.5,record.6, bst.predict(&dtest).unwrap()))?;

            //println!("{:?}", bst.predict(&dtest).unwrap());

            count = 1;

        }
        count = count +1;
    }
    wtr.flush()?;
    Ok(())
}

fn main() {

    // train model, and print evaluation data
    //let bst = Booster::train(&training_params).unwrap();

    //println!("{:?}", bst.predict(&dtest).unwrap());

    if let Err(err) = run() { //error handling
        println!("{}", err); 
        process::exit(1); 
    }
}