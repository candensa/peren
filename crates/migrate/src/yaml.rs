pub(crate) fn refuse_if_yaml_has_anchors_or_aliases(text: &str) -> Result<(), String> {
    enum Mode {
        Normal,
        SingleQuoted,
        DoubleQuoted,
        Comment,
    }

    let mut mode = Mode::Normal;
    let mut prev_nonspace_on_line: Option<char> = None;
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match mode {
            Mode::Comment => {
                if c == '\n' {
                    mode = Mode::Normal;
                    prev_nonspace_on_line = None;
                }
                i += 1;
            }
            Mode::SingleQuoted => {
                if c == '\'' {
                    if chars.get(i + 1) == Some(&'\'') {
                        i += 2;
                        continue;
                    }
                    mode = Mode::Normal;
                }
                if c == '\n' {
                    prev_nonspace_on_line = None;
                }
                i += 1;
            }
            Mode::DoubleQuoted => {
                if c == '\\' && i + 1 < chars.len() {
                    i += 2;
                    continue;
                }
                if c == '"' {
                    mode = Mode::Normal;
                }
                if c == '\n' {
                    prev_nonspace_on_line = None;
                }
                i += 1;
            }
            Mode::Normal => {
                if c == '\n' {
                    prev_nonspace_on_line = None;
                    i += 1;
                    continue;
                }
                if c.is_whitespace() {
                    i += 1;
                    continue;
                }
                if c == '\'' {
                    mode = Mode::SingleQuoted;
                    prev_nonspace_on_line = Some(c);
                    i += 1;
                    continue;
                }
                if c == '"' {
                    mode = Mode::DoubleQuoted;
                    prev_nonspace_on_line = Some(c);
                    i += 1;
                    continue;
                }
                if c == '#' && prev_comment_boundary(&chars, i) {
                    mode = Mode::Comment;
                    i += 1;
                    continue;
                }
                if (c == '&' || c == '*')
                    && matches!(
                        prev_nonspace_on_line,
                        None | Some(':' | '-' | ',' | '[' | '{')
                    )
                {
                    return Err(format!(
                        "YAML anchors/aliases ('{c}') are not accepted by this importer — a real \
                         template.yaml/serverless.yml never needs them, and accepting them would \
                         allow a small document to expand into an enormous in-memory structure \
                         during parsing (a \"billion laughs\"-shaped resource-exhaustion input)"
                    ));
                }
                prev_nonspace_on_line = Some(c);
                i += 1;
            }
        }
    }
    Ok(())
}

fn prev_comment_boundary(chars: &[char], hash_index: usize) -> bool {
    match hash_index.checked_sub(1).and_then(|i| chars.get(i)) {
        None => true,
        Some(c) => *c == '\n' || c.is_whitespace(),
    }
}
