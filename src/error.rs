
#[derive(Debug)]
pub enum ServerError{
    EnvErr(std::env::VarError),
    IoErr(std::io::Error),
}
impl std::error::Error for ServerError {}

impl From<std::env::VarError> for ServerError {
    fn from(error: std::env::VarError) -> Self {
        ServerError::EnvErr(error)
    }
}

impl From<std::io::Error> for ServerError {
    fn from(error: std::io::Error) -> Self {
        ServerError::IoErr(error)
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::EnvErr(e) => write!(f, "Env_err : {}", e),
            ServerError::IoErr(e) => write!(f, "io_err : {}", e),
        }
    }
}


