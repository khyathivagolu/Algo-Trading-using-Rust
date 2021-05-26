//Program when inputted with a csv file with 1 min ohlc data (algo_trading_dataset.csv) will compute 15 min ohlc data and store it to another csv file. 
//Add dependency csv = "1.1" to cargo.toml
//To run: ./target/debug/1min_ohlc_indicators 

//Additionally, program calculates EMA indicators for 1 min ohlc.
use std::process;
//use std::io;
use std::error::Error;


fn frame15() -> Result<(), Box<dyn Error>> {

    type Record = (String, String, f64, f64, f64, f64, usize, String, String, String, String, String);  //parsing data from csv file
    let mut rdr = csv::Reader::from_path("./algo_trading_dataset.csv")?;
    let mut wtr = csv::Writer::from_path("./15min_ohlc.csv")?; 
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

fn indicators()-> Result<(), Box<dyn Error>> {                  //to calculate indicators for 1min ohlc and store the data in 1min_ohlc_indicators.csv

    type Record = (String, String, f64, f64, f64, f64, usize, String, String, String, String, String);  //parsing data types from csv file
    let mut rdr1 = csv::Reader::from_path("./algo_trading_dataset.csv")?;
    let mut wtr1 = csv::Writer::from_path("./1min_ohlc_indicators.csv")?;
    wtr1.write_record(&["id","datetime","open","high","low","close","volume","instrument_id","created_at","updated_at","source","cumulative_volume","EMA 20","EMA 50","EMA 200"])?;

    let mut count1: isize = 0;
    let mut init20: f64 = 0.0;
    let mut ema20: f64 = 0.0;
    let mult20:f64 = 2.0/21.0;

    let mut init50: f64 = 0.0;
    let mut ema50: f64 = 0.0;
    let mult50:f64 = 2.0/51.0;

    let mut init200: f64 = 0.0;
    let mut ema200: f64 = 0.0;
    let mult200:f64 = 2.0/201.0;
    
    for result in rdr1.deserialize() {
        let record: Record = result?;
        count1 = count1 + 1;

        if count1<=20{                          //ema 20 calculation 1min ohlc
            init20 = init20 + record.5;
        }
        if count1 == 20{
            ema20 = init20/20.0;
        }
        if count1 > 20{
            ema20 = ((record.5 - ema20) * mult20) + ema20;
        }

        if count1<=50{                        //ema 50 calculation 1min ohlc
            init50 = init50 + record.5;
        }
        if count1 == 50{
            ema50 = init50/50.0;
        }
        if count1 > 50{
            ema50 = ((record.5 - ema50) * mult50) + ema50;
        }

        if count1<=200{                       //ema 200 calculation 1min ohlc
            init200 = init200 + record.5;
        }
        if count1 == 200{
            ema200 = init200/200.0;
        }
        if count1 > 200{
            ema200 = ((record.5 - ema200) * mult200) + ema200;
        }

    wtr1.serialize((record.0,record.1,record.2,record.3,record.4,record.5,record.6,record.7,record.8,record.9,record.10,record.11,ema20,ema50,ema200))?;
    }
    Ok(())    
}

fn main() {
    
    if let Err(err) = frame15() {       //error handling 
        println!("{}", err);
        process::exit(1);    
    }

    if let Err(err) = indicators() {    //error handling 
        println!("{}", err);
        process::exit(1);    
    }              
}
