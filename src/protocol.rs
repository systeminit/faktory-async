use crate::Job;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct BeatReply {
    pub state: BeatState,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum BeatState {
    Ok,
    Quiet,
    Terminate,
}

#[derive(Debug, Serialize)]
pub struct FailConfig {
    jid: String,
    message: String,
    errtype: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backtrace: Option<Vec<String>>,
}

impl FailConfig {
    pub fn new(
        jid: String,
        mut message: String,
        errtype: String,
        backtrace: Option<Vec<String>>,
    ) -> Self {
        message.truncate(1000);
        Self {
            jid,
            message,
            errtype,
            backtrace,
        }
    }
}

#[derive(Debug, Serialize, Default)]
pub struct BatchConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_bid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    success: Option<Job>,
    #[serde(skip_serializing_if = "Option::is_none")]
    complete: Option<Job>,
}

#[derive(Serialize)]
pub(crate) struct HelloConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,

    #[serde(rename = "v")]
    version: usize,

    /// Hash is hex(sha256(password + salt))
    #[serde(rename = "pwdhash")]
    #[serde(skip_serializing_if = "Option::is_none")]
    password_hash: Option<String>,
}

impl Default for HelloConfig {
    fn default() -> Self {
        Self {
            hostname: None,
            wid: None,
            pid: None,
            labels: Vec::new(),
            password_hash: None,
            version: 2,
        }
    }
}
