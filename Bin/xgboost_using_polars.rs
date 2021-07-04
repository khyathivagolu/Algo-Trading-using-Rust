use polars::prelude::*;
//use std::fmt::Result;
use std::fs::File;
//use std::io::Write;
use std::path::Path;
use std::error::Error;
use std::process;
extern crate xgboost;

use xgboost::{parameters, DMatrix, Booster};



fn read_csv() -> Result<DataFrame> {
    let file = File::open("algo_trading_dataset_.csv").expect("could not read this file");
    CsvReader::new(file)
        .infer_schema(Some(510))
        .has_header(true)
        .with_chunk_size(2)
        .finish()
        //.unwrap()
}
fn run(df: &DataFrame) -> Result<DataFrame> {
    //let mut reader = csv::Reader::from_path("KVDATASET.csv")?; //loading csv file from stdin.
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
    let mut row_used=1;

    while(row_used<=4){
        //let mut de=df.slice(row_used-1,1);
        x_train[(count-1)*4 + 0] = df.column("open")?.get(row_used-1);
        x_train[(count-1)*4 + 1] = df.column("high")?.get(row_used-1);
        x_train[(count-1)*4 + 2] = df.column("low")?.get(row_used-1);
        x_train[(count-1)*4 + 3] = df.column("close")?.get(row_used-1);
        y_train[count-1]=df.column("close")?.get(row_used-1);
        row_used+=1;
        count+=1;

    }

        /*if count % 99 != 0{

        x_train[(count-1)*4 + 0] = record.0;
        x_train[(count-1)*4 + 1] = record.1;
        x_train[(count-1)*4 + 2] = record.2;
        x_train[(count-1)*4 + 3] = record.4;
       /* x_train.push(record.0);
        x_train.push(record.1);
        x_train.push(record.2);
        x_train.push(record.4); */*/

        //y_train[count-1] = ;

        
    
        x_test[0] = df.column("open")?.get(row_used-1);
        x_test[1] = df.column("high")?.get(row_used-1);
        x_test[2] = df.column("low")?.get(row_used-1);
        x_test[3] = df.column("close")?.get(row_used-1);

        y_test[0] = df.column("close")?.get(row_used-1);
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

            println!("{:?}", bst.predict(&dtest).unwrap());


    
        //count = count +1;
    
        let idx = UInt32Chunked::new_from_slice("idx", &[0,1,2,3]);
        
    
        Ok(df.take(&idx))
}


fn main(){
    let mut df=read_csv().expect("could not prepare DataFrame");
    if let Err(err) = run(&df) { //error handling
        println!("{}", err); 
        process::exit(1); 
    }

}