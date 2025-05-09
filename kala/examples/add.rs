use std::sync::Arc;

async fn add(x: u32, y: u32) -> u32 {
    x + y
}

#[kala::main]
async fn main() {
    let sum = add(3, 4).await;
    println!("3+4={}", sum);
}
