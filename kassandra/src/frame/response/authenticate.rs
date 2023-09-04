use nom::IResult;

use crate::frame::parse;

// Implements Authenticate message.
#[derive(Debug)]
pub struct Authenticate {
    pub authenticator_name: String,
}

impl Authenticate {
    pub fn deserialize(buf: &[u8]) -> IResult<&[u8], Self> {
        let (rest, authenticator_name) = parse::short_string(buf)?;
        let authenticator_name = authenticator_name.to_string();

        Ok((rest, Authenticate { authenticator_name }))
    }
}

#[derive(Debug)]
pub struct AuthSuccess {
    pub success_message: Option<Vec<u8>>,
}

impl AuthSuccess {
    pub fn deserialize(buf: &[u8]) -> IResult<&[u8], Self> {
        let (rest, success_message) = parse::bytes_opt(buf)?;
        let success_message = success_message.map(|it| it.to_owned());

        Ok((rest, AuthSuccess { success_message }))
    }
}

#[derive(Debug)]
pub struct AuthChallenge {
    pub authenticate_message: Option<Vec<u8>>,
}

impl AuthChallenge {
    pub fn deserialize(buf: &[u8]) -> IResult<&[u8], Self> {
        let (rest, authenticate_message) = parse::bytes_opt(buf)?;
        let authenticate_message = authenticate_message.map(|it| it.to_owned());

        Ok((
            rest,
            AuthChallenge {
                authenticate_message,
            },
        ))
    }
}
