#[must_use]
pub fn value(response: &str, name: &str) -> u64 {
    response
        .lines()
        .find_map(|line| {
            let (metric, value) = line.split_once(' ')?;
            (metric == name)
                .then(|| value.parse::<u64>().ok())
                .flatten()
        })
        .unwrap_or_else(|| panic!("metric {name} was not exported in {response}"))
}
