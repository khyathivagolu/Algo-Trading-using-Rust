// Program to parse csv file taken from stdin and print it to the screen as a list of tuples.
// Run the program usinf the csv file as follows:
// ./target/debug/csv_data_parse < algo_trading_dataset.csv
use std::io;
use std::process;
use std::error::Error;

type Record = (String, String, f64, f64, f64, f64, usize, String, String, String, String); //Using serde to parse csv data given in the file into various datatypes.

fn run() -> Result<(), Box<dyn Error>> {
    let mut reader = csv::Reader::from_reader(io::stdin()); //loading csv file from stdin.

    for result in reader.deserialize() {
        let record: Record = result?;
        println!("{:?}", record);
    }
    Ok(())
}
fn main() {
    if let Err(err) = run() { //error handling
        println!("{}", err); 
        process::exit(1); 
    }
}

