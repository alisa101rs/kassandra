use std::collections::HashMap;

use bytes::BufMut;
use eyre::{eyre, Result};

use crate::{frame, frame::parse};

#[derive(Debug)]
pub struct Supported {
    pub options: HashMap<String, Vec<String>>,
}

impl Supported {
    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        let (_, map) =
            parse::string_multimap(buf).map_err(|_| eyre!("Could not parse Supported response"))?;

        let options = map
            .into_iter()
            .map(|(k, values)| {
                (
                    k.to_string(),
                    values.into_iter().map(|it| it.to_string()).collect(),
                )
            })
            .collect();
        Ok(Self { options })
    }

    pub fn serialize(&self, buf: &mut impl BufMut) -> Result<()> {
        frame::write::string_multimap(buf, &self.options);
        Ok(())
    }
}
