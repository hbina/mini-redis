use crate::{Connection, Frame, Parse};
use tracing::instrument;

#[derive(Debug, Default)]
pub struct Config {}

impl Config {
    pub fn new() -> Config {
        Config {}
    }

    /// CONFIG GET parameter [parameter ...]
    /// TODO: This is just a stub implementation
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Config> {
        while parse.next_string().is_ok() {}

        Ok(Config {})
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        dst.write_frame(&Frame::Simple("OK".to_string())).await?;
        Ok(())
    }
}
