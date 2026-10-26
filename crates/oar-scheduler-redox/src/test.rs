use dotenvy::dotenv;

#[test]
fn python_tests() {
    dotenv().ok();

    let oar3_tests_enabled = std::env::var("OAR3_PYTHON_TESTS_ENABLED");
    if oar3_tests_enabled != Ok("true".to_string()) {
        return;
    }

    let oar3_python_path = std::env::var("OAR3_PYTHON_PATH")
        .expect("OAR3_PYTHON_PATH environment variable not set");

    let oar3_python_venv_path = std::env::var("OAR3_PYTHON_VENV_PATH")
        .expect("OAR3_PYTHON_VENV_PATH environment variable not set");

    // Assert that the venv exists
    let activate_script = format!("{}/bin/activate", oar3_python_venv_path);
    if !std::path::Path::new(&activate_script).exists() {
        println!("WARNING: can’t find the virtual environment activate script at {}", activate_script);
        return;
    }

    // Run pytest in a shell
    let command = format!("source {} && cd {} && pytest tests/kao/test_db_*", activate_script, oar3_python_path);
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(&command)
        .status()
        .expect("failed to execute process");

    assert!(status.success());
}
