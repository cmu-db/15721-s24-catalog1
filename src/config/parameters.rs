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

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_init() {
        // This test checks if the .env file is loaded correctly
        init();
        assert!(dotenv::var("PORT").is_ok());
    }

    #[test]
    fn test_get() {
        // This test checks if the get function correctly retrieves an environment variable
        init();
        env::set_var("TEST_ENV_VAR", "123");
        assert_eq!(get("TEST_ENV_VAR"), "123");
    }

    #[test]
    #[should_panic(expected = "TEST_ENV_VAR_UNDEFINED is not defined in the environment")]
    fn test_get_undefined() {
        // This test checks if the get function correctly panics when trying to retrieve an undefined environment variable
        init();
        get("TEST_ENV_VAR_UNDEFINED");
    }
}
