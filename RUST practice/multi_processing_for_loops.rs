use std::thread;
use std::time::Duration;

fn main() {
    let handle1 = thread::spawn(|| {
        for i in 1..10 {
            println!("number {} from the spawned thread 1!", i);
            thread::sleep(Duration::from_millis(1));
        }
    });

    let handle2 = thread::spawn(|| {
        for i in 1..10 {
            println!("number {} from the spawned thread 2!", i);
            thread::sleep(Duration::from_millis(1));
        }
    });



    for i in 1..5 {
        println!("number {} from the main thread!", i);
        thread::sleep(Duration::from_millis(1));
    }

    handle1.join().unwrap();
    handle2.join().unwrap();
}