// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow_flight::error::FlightError;

/// A specialized `Result` type for UDF operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// /// The error type for UDF operations.
// #[derive(thiserror::Error, Debug)]
// pub enum Error {
//     #[error("failed to connect to UDF service: {0}")]
//     Connect(#[from] tonic::transport::Error),

//     #[error("failed to check UDF: {0}")]
//     Tonic(#[from] Box<tonic::Status>),

//     #[error("failed to call UDF: {0}")]
//     Flight(#[from] Box<FlightError>),

//     #[error("type mismatch: {0}")]
//     TypeMismatch(String),

//     #[error("arrow error: {0}")]
//     Arrow(#[from] arrow_schema::ArrowError),

//     #[error("UDF unsupported: {0}")]
//     Unsupported(String),

//     #[error("UDF service returned no data")]
//     NoReturned,
// }

// struct WrapperInner<E> {
//     source: Option<Box<dyn std::error::Error + Send + Sync>>,
//     error: E,
//     backtrace: Backtrace,
// }

// struct Wrapper<E>(Box<WrapperInner<E>>);

#[derive(thiserror::Error, Debug)]
enum ErrorKind {
    #[error("failed to connect to UDF service")]
    Connect,

    #[error("failed to check UDF")]
    Check,

    #[error("failed to call UDF")]
    Flight,

    #[error("type mismatch: {0}")]
    TypeMismatch(String),

    #[error("arrow error")]
    Arrow,

    #[error("UDF unsupported: {0}")]
    Unsupported(String),

    #[error("UDF service returned no data")]
    NoReturned,
}

// pub type NewError = error_stack::Report<ErrorKind>;

pub struct Error(error_stack::Report<ErrorKind>);

/// This is bad.
///
/// `Report` is not a `std::error::Error` and it utilizes its own way to provide `source`. So it
/// even don't care about the source of the error with `Report::new`. This is important to get it
/// correctly reported for some errors like `aws::SdkError`.
impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(error: tonic::transport::Error) -> Self {
        Self(error_stack::Report::new(error).change_context(ErrorKind::Connect))
    }
}

impl From<tonic::Status> for Error {
    fn from(error: tonic::Status) -> Self {
        Self(error_stack::Report::new(error).change_context(ErrorKind::Check))
    }
}

impl From<FlightError> for Error {
    fn from(error: FlightError) -> Self {
        Self(error_stack::Report::new(error).change_context(ErrorKind::Flight))
    }
}

impl From<arrow_schema::ArrowError> for Error {
    fn from(error: arrow_schema::ArrowError) -> Self {
        Self(error_stack::Report::new(error).change_context(ErrorKind::Arrow))
    }
}

impl From<ErrorKind> for Error {
    fn from(value: ErrorKind) -> Self {
        Self(error_stack::Report::new(value))
    }
}

impl Error {
    pub fn type_mismatch(message: impl Into<String>) -> Self {
        ErrorKind::TypeMismatch(message.into()).into()
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        ErrorKind::Unsupported(message.into()).into()
    }

    pub fn no_returned() -> Self {
        ErrorKind::NoReturned.into()
    }
}

static_assertions::const_assert_eq!(std::mem::size_of::<Error>(), 8);
