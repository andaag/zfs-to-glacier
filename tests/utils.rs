use std::{error::Error};
use zfs_to_glacier::cmd_execute::*;


#[test]
fn test_execute() -> Result<(), Box<dyn Error>> {
    let result = ExecutorCommand("echo -n teststring".to_string()).execute()?;
    assert_eq!(result, "teststring");
    Ok(())
}
#[test]
fn test_execute_multiline() -> Result<(), Box<dyn Error>> {
    let result = ExecutorCommand("echo -e teststring \n test".to_string()).execute_by_line()?;
    assert_eq!(result, vec!["teststring", "test"]);
    Ok(())
}
