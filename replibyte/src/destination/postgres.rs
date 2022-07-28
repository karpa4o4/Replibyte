use std::collections::HashMap;
use std::io::{Error, ErrorKind, Write};
use std::process::{Command, Stdio};

use crate::config::SshConfig;
use crate::connector::Connector;
use crate::destination::Destination;
use crate::types::Bytes;
use crate::utils::{binary_exists, wait_for_command};

pub struct Postgres<'a> {
    host: &'a str,
    port: u16,
    database: &'a str,
    username: &'a str,
    password: &'a str,
    wipe_database: bool,
    ssh_config: Option<SshConfig>,
}

impl<'a> Postgres<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        database: &'a str,
        username: &'a str,
        password: &'a str,
        wipe_database: bool,
        ssh_config: Option<SshConfig>,
    ) -> Self {
        Postgres {
            host,
            port,
            database,
            username,
            password,
            wipe_database,
            ssh_config,
        }
    }
}

impl<'a> Connector for Postgres<'a> {
    fn init(&mut self) -> Result<(), Error> {
        let _ = binary_exists("psql")?;

        if self.wipe_database {
            let s_port = self.port.to_string();
            let wipe_db_query = wipe_database_query(self.username);

            let mut wipe_db_cmd = Command::new("psql");
            wipe_db_cmd.args([
                "-h",
                self.host,
                "-p",
                s_port.as_str(),
                "-d",
                self.database,
                "-U",
                self.username,
                "-c",
                wipe_db_query.as_str(),
            ]);

            let mut cmd = match &self.ssh_config {
                Some(ssh_config) => {
                    let mut envs = HashMap::new();
                    envs.insert("PGPASSWORD", self.password);

                    ssh_config.ssh_command(&wipe_db_cmd, &envs)
                },
                None => {
                    wipe_db_cmd.env("PGPASSWORD", self.password);
                    wipe_db_cmd
                },
            };

            let exit_status = cmd
                .stdout(Stdio::null())
                .spawn()?
                .wait()?;

            if !exit_status.success() {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("command error: {:?}", exit_status.to_string()),
                ));
            }
        }

        Ok(())
    }
}

impl<'a> Destination for Postgres<'a> {
    fn write(&self, data: Bytes) -> Result<(), Error> {
        let s_port = self.port.to_string();

        let mut psql_cmd = Command::new("psql");
        psql_cmd.args([
            "-h",
            self.host,
            "-p",
            s_port.as_str(),
            "-d",
            self.database,
            "-U",
            self.username,
        ]);

        let mut cmd = match &self.ssh_config {
            Some(ssh_config) => {
                let mut envs = HashMap::new();
                envs.insert("PGPASSWORD", self.password);

                ssh_config.ssh_command(&psql_cmd, &envs)
            },
            None => {
                psql_cmd.env("PGPASSWORD", self.password);
                psql_cmd
            },
        };

        let mut process = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()?;

        let _ = process.stdin.take().unwrap().write_all(data.as_slice());

        wait_for_command(&mut process)
    }
}

fn wipe_database_query(username: &str) -> String {
    format!(
        "\
    DROP SCHEMA public CASCADE; \
    CREATE SCHEMA public; \
    GRANT ALL ON SCHEMA public TO \"{}\"; \
    GRANT ALL ON SCHEMA public TO public;\
    ",
        username
    )
}

#[cfg(test)]
mod tests {
    use crate::connector::Connector;
    use crate::destination::postgres::Postgres;
    use crate::destination::Destination;

    fn get_postgres() -> Postgres<'static> {
        Postgres::new("localhost", 5453, "root", "root", "password", true, None)
    }

    fn get_invalid_postgres() -> Postgres<'static> {
        Postgres::new("localhost", 5453, "root", "root", "wrongpassword", true, None)
    }

    #[test]
    fn connect() {
        let mut p = get_postgres();
        let _ = p.init().expect("can't init postgres");
        assert!(p.write(b"SELECT 1".to_vec()).is_ok());

        let mut p = get_invalid_postgres();
        assert!(p.init().is_err());
        assert!(p.write(b"SELECT 1".to_vec()).is_err());
    }

    #[test]
    fn test_inserts() {}
}
