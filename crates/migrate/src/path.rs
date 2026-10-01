pub(crate) fn validate_relative_module_path(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("must not be empty".to_string());
    }
    if path.contains('\0') {
        return Err("must not contain a NUL byte".to_string());
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(format!(
            "{path:?} is an absolute path — module paths must be relative to the project directory"
        ));
    }
    let mut chars = path.chars();
    if let (Some(letter), Some(':')) = (chars.next(), chars.next())
        && letter.is_ascii_alphabetic()
    {
        return Err(format!(
            "{path:?} looks like a Windows absolute path — module paths must be relative to \
                 the project directory"
        ));
    }
    if path.split(['/', '\\']).any(|segment| segment == "..") {
        return Err(format!(
            "{path:?} contains a \"..\" path segment — module paths must stay inside the project \
             directory"
        ));
    }
    Ok(())
}
