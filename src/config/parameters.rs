use dotenv;

// load the env file
pub fn init() {
    dotenv::dotenv().ok().expect("Failed to load .env file");
}

// get the parameters from the env file and throw errors appropriately
pub fn get(parameter: &str) -> String {
    let env_parameter = std::env::var(parameter)
        .expect(&format!("{} is not defined in the environment", parameter));
    env_parameter
}
