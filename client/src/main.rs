use std::time::Instant;
use tokio::join;

#[tokio::test]
pub mod test_mod {
    pub async fn test_concurrent_requests() {
        let now = Instant::now();
        let mut handles = Vec::new();
        let client = reqwest::Client::new();
        for _ in 0..10 {
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                let res = client.get(format!("http://localhost:3000/namespaces")).send().await;
                // assert on response, measure latency, etc.
                print!("{}", res );
            }));
        }
    
        join!(handles);
        let elapsed = now.elapsed();
        println!("Total time for 10 concurrent requests: {:?}", elapsed);
    }    
}


#[tokio::main]
async fn main() {
    test_mod::test_concurrent_requests().await
}
