use std::error;
use std::error::Error;
use std::fmt;
use std::ffi::NulError;
use libc::{c_uint, c_uchar, c_int, c_void};
use std::ptr;
use oci_bindings::{OCIErrorGet, ErrorHandleType, OCI_NO_DATA};

#[derive(Debug)]
pub enum OciError {
    Oracle(ErrorRecord),
    NoSql,
    Conversion,
    Nul(NulError),
}

impl fmt::Display for OciError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OciError::Oracle(ref err) => write!(f, "{}", err),
            OciError::NoSql => {
                write!(f,
                       "No prepared or direct SQL statement to execute. Call prepare() to prepare \
                        one, or sql() to set one for direct execution")
            }
            OciError::Conversion => write!(f, "{}", self.description()),
            OciError::Nul(ref err) => write!(f, "Nul error: {}", err),
        }
    }
}

impl error::Error for OciError {
    fn description(&self) -> &str {
        match *self {
            OciError::Oracle(_) => "Oracle error",
            OciError::NoSql => "No SQL to execute",
            OciError::Conversion => "Cannot convert from OCI to Rust type",
            OciError::Nul(ref err) => err.description(),
        }
    }
}

#[derive(Debug)]
pub struct ErrorRecord {
    records: Vec<(i32, String)>,
}
impl ErrorRecord {
    fn new() -> ErrorRecord {
        ErrorRecord { records: Vec::new() }
    }

    fn add_error(&mut self, code: i32, description: String) {
        self.records.push((code, description))
    }
}

impl fmt::Display for ErrorRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut text = String::new();
        for (index, error) in self.records.iter().enumerate() {
            text.push_str(format!("Error number {}. Error code ORA-{}. Error text {}",
                                  index,
                                  error.0,
                                  &error.1)
                .as_ref())
        }
        write!(f, "{}", text)
    }
}

pub fn get_error(handle: *mut c_void, handle_type: ErrorHandleType) -> OciError {

    let record_nmb: c_uint = 1;
    let sql_state: *mut c_uchar = ptr::null_mut();
    let buffer_size: c_uint = 1024;
    let mut error_record = ErrorRecord::new();

    loop {

        let mut error_code: c_int = 0;
        let mut error_message: [c_uchar; 1024] = [0; 1024];
        let error_message_ptr = error_message.as_mut_ptr();
        let error_result = unsafe {
            OCIErrorGet(handle,
                        record_nmb,
                        sql_state,
                        &mut error_code,
                        error_message_ptr,
                        buffer_size,
                        handle_type as c_uint)
        };
        if error_result == OCI_NO_DATA {
            break;
        }
        match error_result {
            OCI_NO_DATA => break,
            OCI_SUCCESS => {
                let mut oracle_error_text = match String::from_utf8(Vec::from(&error_message
                                                                                   [..])) {
                    Ok(text) => text,
                    Err(err) => {
                        "Oracle error text is unreadable due to it not being utf8".to_string()
                    } 
                };
                error_record.add_error(error_code, oracle_error_text)
            }
            OCI_ERROR => {
                error_record.add_error(error_code, "Call to OCIErrorGet failed".to_string())
            }
        }
    }
    OciError::Oracle(error_record)
}