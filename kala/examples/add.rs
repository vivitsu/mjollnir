use kala::executor::Executor;

async fn add(x: u32, y: u32) -> u32 {
    x + y
}

fn main() {
    let mut runtime = Executor::new().expect("runtime");
    runtime.block_on(async move {
        let sum = add(3, 4).await;
        println!("3+4={}", sum);
    });
}
