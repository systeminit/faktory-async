use crate::protocol::{Fetch, Hello, Push};
use crate::{Config, Error, Job, Result};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

#[derive(Debug)]
pub(crate) struct Connection {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Connection {
    pub async fn new(config: &Config) -> Result<Self> {
        let (reader, writer) = TcpStream::connect(&config.uri).await?.into_split();
        let mut conn = Connection {
            reader: BufReader::new(reader),
            writer,
        };
        conn.validate_response("+HI {\"v\":2}\r\n").await?;

        conn.hello().await?;

        Ok(conn)
    }

    async fn validate_response(&mut self, expected: &str) -> Result<()> {
        let mut output = String::new();
        self.reader.read_line(&mut output).await?;
        if output != expected {
            return Err(Error::UnexpectedResponse(output, expected.to_owned()))?;
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<()> {
        Ok(self.writer.write_all(b"END").await?)
    }

    pub async fn hello(&mut self) -> Result<()> {
        println!("Sending hello");
        Hello::default().send(&mut self.writer).await?;
        println!("Sent hello");
        self.validate_response("+OK\r\n").await?;
        println!("Got OK to hello");

        Ok(())
    }

    pub async fn fetch(&mut self, queues: &[String]) -> Result<Option<Job>> {
        println!("Sending fetch");
        Fetch::new(queues).send(&mut self.writer).await?;
        println!("Sent");

        let mut output = String::new();
        self.reader.read_line(&mut output).await?;

        if !output.starts_with("$") || output.len() <= 3 {
            return Err(Error::UnexpectedResponse(output, "$".to_owned()))?;
        }
        if output == "$-1\r\n" {
            return Ok(None);
        }

        let len: usize = output[1..output.len() - 2].parse()?;
        let mut output = vec![0; len];
        self.reader.read_exact(&mut output).await?;
        //self.reader.read_line(&mut String::new())?;
        self.reader.read_exact(&mut [0; 2]).await?;
        Ok(Some(serde_json::from_slice(&output)?))
    }

    pub async fn push(&mut self, job: Job) -> Result<()> {
        println!("Sending push");
        Push::new(job).send(&mut self.writer).await?;
        println!("Sent");
        self.validate_response("+OK\r\n").await?;
        Ok(())
    }
}
