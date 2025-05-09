use kala::executor::Executor;

async fn add(x: u32, y: u32) -> u32 {
    x + y
}

fn main() {
    let runtime = Executor::new();
    let handle = runtime.spawn(add(3, 4));
    
    runtime.spawn(async {
        let sum = handle.await;
        println!("3 + 4 = {}", sum);
    });
    
    runtime.run();
}