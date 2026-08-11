

#[derive(Debug)]
pub enum ServerError{
    EnvErr(std::env::VarError),
    IoErr(std::io::Error),
    InvalidRedisType(String),
    InvalidRedisCommand(String),
    NoRedisKey(String),
    InvalidSnapshot(String), 
    SnapshotCorrupted(String), 
    InvalidUtf8(std::string::FromUtf8Error),


    ServerResponse(String)
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

impl From<std::string::FromUtf8Error> for ServerError { 
    fn from(error: std::string::FromUtf8Error) -> Self { 
        ServerError::InvalidUtf8(error) 
    } 
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::EnvErr(e) => write!(f, "Env_err : {}", e),
            ServerError::IoErr(e) => write!(f, "io_err : {}", e),
            ServerError::NoRedisKey(e)=>write!(f, "NoRedisKey : {}", e),
            ServerError::InvalidRedisType(e)=>write!(f, "InvalidRedisType : {}", e),
            ServerError::InvalidRedisCommand(e)=>write!(f, "InvalidRedisCommand : {}", e),
            ServerError::InvalidSnapshot(message) => { write!(f, "Invalid snapshot: {}", message) } 
            ServerError::SnapshotCorrupted(message) => { write!(f, "Snapshot corrupted: {}", message) } 
            ServerError::InvalidUtf8(e) => { write!(f, "Invalid UTF-8 data: {}", e) }
            ServerError::ServerResponse(e)=>write!(f, "ServerRsponseError : {}", e),
        }
    }
}


