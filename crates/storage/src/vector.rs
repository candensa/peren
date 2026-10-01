use rusqlite::{Error, Result, functions::Context, types::ValueRef};

pub fn distance(context: &Context<'_>) -> Result<f64> {
    let left = vector(context.get_raw(0))?;
    let right = vector(context.get_raw(1))?;
    if left.len() != right.len() || left.is_empty() {
        return Err(Error::UserFunctionError(
            "vector dimensions must match".into(),
        ));
    }
    let sum = left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum::<f64>();
    Ok(sum.sqrt())
}

fn vector(value: ValueRef<'_>) -> Result<Vec<f64>> {
    match value {
        ValueRef::Text(text) => json(text),
        ValueRef::Blob(bytes) => blob(bytes),
        _ => Err(Error::UserFunctionError(
            "vector value must be a JSON array or f64 blob".into(),
        )),
    }
}

fn json(bytes: &[u8]) -> Result<Vec<f64>> {
    let values: Vec<f64> = serde_json::from_slice(bytes)
        .map_err(|source| Error::UserFunctionError(Box::new(source)))?;
    if values.iter().any(|value| !value.is_finite()) {
        return Err(Error::UserFunctionError(
            "vector values must be finite numbers".into(),
        ));
    }
    Ok(values)
}

fn blob(bytes: &[u8]) -> Result<Vec<f64>> {
    let mut chunks = bytes.chunks_exact(8);
    let values = chunks
        .by_ref()
        .map(|chunk| f64::from_le_bytes(chunk.try_into().expect("chunk size is fixed")))
        .collect::<Vec<_>>();
    if !chunks.remainder().is_empty() || values.iter().any(|value| !value.is_finite()) {
        return Err(Error::UserFunctionError(
            "vector blob must contain finite little-endian f64 values".into(),
        ));
    }
    Ok(values)
}
