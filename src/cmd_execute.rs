use std::{error::Error, fmt, process::{Child, ChildStdout, Command, ExitStatus, Stdio}};
use std::str;
use std::io::{self, Read};

pub trait CommandStreamActions<T: Read> {
    fn stdout(&mut self) -> T;
    fn wait(&mut self) -> io::Result<ExitStatus>;
}

impl CommandStreamActions<ChildStdout> for Child {
    fn stdout(&mut self) -> ChildStdout {
        self.stdout.take().unwrap()
    }
    fn wait(&mut self) -> io::Result<ExitStatus> {
        self.wait()
    }
}

#[derive(Debug)]
struct ExecuteError(ExitStatus);
impl fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Command exited with error code: {}", self.0)
    }
}
impl Error for ExecuteError {}

pub struct ExecutorCommand(pub String);

pub trait Executor {
    fn execute(&self) -> Result<String, Box<dyn Error>>;
    fn execute_by_line(&self) -> Result<Vec<String>, Box<dyn Error>>;
    fn spawn(&self) -> Result<Child, Box<dyn Error>>;
}

impl ExecutorCommand {
    fn create_cmd(&self) -> Box<Command> {
        let cmd:Vec<&str> = self.0.split(" ").collect();
        let program = cmd[0];
        let mut arguments = cmd.clone();
        arguments.remove(0);
        let mut command = Box::new(Command::new(program.to_string()));
        command.args(arguments);
        command
    }
}


impl Executor for ExecutorCommand {
    fn execute(&self) -> Result<String, Box<dyn Error>> {
        let output = self.create_cmd().as_mut().output()?;
        if output.status.success() {
            let content = str::from_utf8(&output.stdout)?;
            Ok(content.to_string())
        } else {
            //could add let stderr = str::from_utf8(&self.stderr).unwrap(); here.
            Err(Box::new(ExecuteError(output.status)))
        }
    }

    fn execute_by_line(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let result:Vec<String> = self.execute()?.split("\n")
        .map(|x| x.trim().to_string())
        .filter(|x| x.len() > 0)
        .collect();
        Ok(result)
    }

    fn spawn(&self) -> Result<Child, Box<dyn Error>> {
        Ok(self.create_cmd().as_mut().stdout(Stdio::piped()).spawn()?)
    }
}
