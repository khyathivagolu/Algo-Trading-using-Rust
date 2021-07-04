//Program when inputted with a csv file with 1 min ohlc data (algo_trading_dataset.csv) will compute 15 min ohlc data and store it to another csv file.
//Add dependency csv = "1.1" to cargo.toml
//To run: ./target/debug/ohlc_parsing < algo_trading_dataset.csv

use std::process;
use std::io;
use std::error::Error;

type Record = (String, String, f64, f64, f64, f64, usize, String, String, String, String, String);  //parsing data from csv file

fn run() -> Result<(), Box<dyn Error>> {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let mut wtr = csv::Writer::from_path("./15min_ohlc.csv")?; //create an empty csv file named 15min_ohlc.csv in the directory. 
    wtr.write_record(&["id","datetime","open","high","low","close","volume","instrument_id","created_at","updated_at","source","cumulative_volume"])?;
    let mut _count:i32 = 0;
    let mut open15: f64 = 0.0;
    //let mut close15: f64 = 0.0;         //not used as value is written directly
    let mut high15: f64 = 0.0;
    let mut low15: f64 = 0.0;
    let mut volume15:usize = 0;
    let mut cumvol:usize = 0;
    let mut datetime:String = "".to_string();
    for result in rdr.deserialize() {
        let record: Record = result?;
        //let mut month: i8 = record.1[5..7].to_string().trim().parse().expect("Not number");
        //let mut year: i32 = record.1[0..4].to_string().trim().parse().expect("Not number");
        let min: i32 = record.1[14..16].to_string().trim().parse().expect("Not number");
        if min%15 == 0 {                 
            open15 = record.2;           //stores the opening stock value for the 15 minute frame.
            low15 = record.4;
            high15 = record.3;
            volume15=0;
            datetime = record.1;
        }
        if record.3>= high15{            //records highest value in 15 minute frame.
            high15 = record.3;
        }
        if record.4 <= low15{            //records lowest value in 15 mins frame
            low15 = record.4;
        }
        volume15 = volume15 + record.6;  //total volume in 15 minutes frame.
        cumvol = cumvol + record.6;      //cumilative volume in 15 minutes frame.
        if min%15 == 14{
            wtr.serialize((record.0,&datetime,open15,high15,low15,record.5,volume15,record.7,record.8,record.9,record.10,cumvol))?;
        }
    }
    wtr.flush()?;

    Ok(())
}

fn main() {
    if let Err(err) = run() {    //error handling 
        println!("{}", err);
        process::exit(1);
    }
}