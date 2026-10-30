//! Python's `glob.glob(pattern, recursive=True)`, for a reader's glob
//! (#261).
//!
//! A port of CPython's `glob._iglob` and of the `[...]` rules of
//! `fnmatch.translate` (3.13), so a reader's glob matches the files Python's
//! matches: `*`, `?` and `[...]` within one path segment, a segment that is
//! `**` across directories, and a name that starts with `.` only in a
//! segment of the pattern that starts with `.`. The `glob` crate, which
//! `ListingTableUrl` matches with, is not Python's: it rejects `a**.csv`,
//! where `**` is `*`, and `x[.csv`, where an unclosed `[` is itself; a final
//! `**` matches only directories; and it either skips every name that
//! starts with `.`, even one the pattern spells, or lets `*` and `**` match
//! them, so it lists directories Python never lists.
//!
//! One difference is deliberate. Python's glob reads any error from
//! listing a directory or looking up a path as nothing there, so a pattern
//! would match fewer files without an error. Here only a path that does not
//! exist, or that has a file where a directory should be, is nothing there;
//! any other error, such as a directory that cannot be listed or a name in
//! one that cannot be searched, fails the call, naming the path (contract
//! §4.1). Only the paths the pattern reaches are listed or looked up, so one
//! it excludes cannot fail it.

use std::fs;
use std::io;
use std::path::Path;

/// The paths that `pattern` matches now, as `glob.glob(pattern,
/// recursive=True)` gives them, or the error of a path it cannot list or
/// look up.
///
/// Names that are not UTF-8 are skipped: a reader cannot read their files,
/// as the object store's paths are UTF-8.
pub(crate) fn glob(pattern: &str) -> io::Result<Vec<String>> {
    let mut paths = iglob(pattern, false)?;
    // `glob.glob` drops the empty path a pattern of `**` gives for the
    // working directory.
    paths.retain(|path| !path.is_empty());
    Ok(paths)
}

/// `glob._iglob`: the paths `pattern` matches, only directories if
/// `dironly`. The segments before the last are matched first, as
/// directories, and the last in each of them.
fn iglob(pattern: &str, dironly: bool) -> io::Result<Vec<String>> {
    let (dirname, basename) = split(pattern);
    let dirs = if dirname != pattern && has_magic(dirname) {
        iglob(dirname, true)?
    } else {
        vec![dirname.to_owned()]
    };
    let mut paths = Vec::new();
    for dir in dirs {
        let names = if basename == "**" {
            glob2(&dir, dironly)?
        } else if has_magic(basename) {
            glob1(&dir, basename, dironly)?
        } else {
            glob0(&dir, basename)?
        };
        paths.extend(names.iter().map(|name| join(&dir, name)));
    }
    Ok(paths)
}

/// `glob._glob0`: `[basename]` if it names an entry of `dir`, and `[""]`
/// for an empty `basename` if `dir` is a directory.
fn glob0(dir: &str, basename: &str) -> io::Result<Vec<String>> {
    let found = if basename.is_empty() {
        is_dir(dir)?
    } else {
        let path = join(dir, basename);
        found(Path::new(&path), fs::symlink_metadata(&path))?.is_some()
    };
    Ok(if found {
        vec![basename.to_owned()]
    } else {
        Vec::new()
    })
}

/// `glob._glob1`: the names in `dir` that `pattern` matches. A name that
/// starts with `.` matches only a pattern that does.
fn glob1(dir: &str, pattern: &str, dironly: bool) -> io::Result<Vec<String>> {
    let matcher = Pattern::new(pattern);
    let mut names = Vec::new();
    for entry in listdir(dir)? {
        if (is_hidden(pattern) || !is_hidden(&entry.name))
            && matcher.matches(&entry.name)
            && (!dironly || entry.is_dir(dir)?)
        {
            names.push(entry.name);
        }
    }
    Ok(names)
}

/// `glob._glob2`, for a segment of `**`: `""` for `dir` itself, then every
/// path below it that no name starting with `.` leads to.
fn glob2(dir: &str, dironly: bool) -> io::Result<Vec<String>> {
    let mut names = Vec::new();
    if dir.is_empty() || is_dir(dir)? {
        names.push(String::new());
    }
    rlistdir(dir, "", dironly, &mut names)?;
    Ok(names)
}

/// `glob._rlistdir`: appends to `paths` the names in `dir` that do not
/// start with `.`, each after `prefix`, and then those below it.
fn rlistdir(dir: &str, prefix: &str, dironly: bool, paths: &mut Vec<String>) -> io::Result<()> {
    for entry in listdir(dir)? {
        if is_hidden(&entry.name) {
            continue;
        }
        let is_dir = entry.is_dir(dir)?;
        if dironly && !is_dir {
            continue;
        }
        let path = join(prefix, &entry.name);
        paths.push(path.clone());
        if is_dir {
            rlistdir(&join(dir, &entry.name), &path, dironly, paths)?;
        }
    }
    Ok(())
}

/// A name in a directory that [`listdir`] gives.
struct Entry {
    name: String,
    /// Its type, unless it is a link or the directory's listing did not
    /// give it.
    kind: Option<fs::FileType>,
}

impl Entry {
    /// Whether it is a directory, following a link, as `os.DirEntry.is_dir()`
    /// tells; `dir` is the directory it is in. A link to nothing is not one.
    fn is_dir(&self, dir: &str) -> io::Result<bool> {
        match self.kind {
            Some(kind) => Ok(kind.is_dir()),
            None => is_dir(&join(dir, &self.name)),
        }
    }
}

/// The UTF-8 names in `dir`, the working directory if it is `""`. Like
/// Python's glob, a `dir` that does not exist or is not a directory has
/// none; any other error is returned, naming `dir`.
fn listdir(dir: &str) -> io::Result<Vec<Entry>> {
    let dir = if dir.is_empty() { "." } else { dir };
    let cannot = |e: io::Error| io::Error::new(e.kind(), format!("Cannot list '{dir}': {e}"));
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if is_missing(&e) => return Ok(Vec::new()),
        Err(e) => return Err(cannot(e)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(cannot)?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let kind = entry.file_type().ok().filter(|kind| !kind.is_symlink());
        names.push(Entry { name, kind });
    }
    Ok(names)
}

/// The metadata of `path`, following a link, or `None` if nothing is there
/// ([`found`]).
pub(crate) fn metadata(path: impl AsRef<Path>) -> io::Result<Option<fs::Metadata>> {
    let path = path.as_ref();
    found(path, fs::metadata(path))
}

/// `os.path.isdir`, except that an error other than nothing being there is
/// returned ([`found`]).
fn is_dir(path: &str) -> io::Result<bool> {
    Ok(metadata(path)?.is_some_and(|meta| meta.is_dir()))
}

/// The metadata `looked_up` gives of `path`, or `None` if nothing is there:
/// `path` does not exist, it is a link to nothing, or a component of it that
/// should be a directory is not one. Any other error is returned, naming
/// `path`.
fn found(path: &Path, looked_up: io::Result<fs::Metadata>) -> io::Result<Option<fs::Metadata>> {
    match looked_up {
        Ok(meta) => Ok(Some(meta)),
        Err(e) if is_missing(&e) => Ok(None),
        Err(e) => Err(io::Error::new(
            e.kind(),
            format!("Cannot access '{}': {e}", path.display()),
        )),
    }
}

/// Whether an error that looking up or listing a path gives says that
/// nothing is there.
fn is_missing(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
    )
}

/// `os.path.split`: the path before the last `/`, without the `/`s that
/// end it unless it is only `/`s, and the name after it.
fn split(path: &str) -> (&str, &str) {
    let (head, tail) = path.split_at(path.rfind('/').map_or(0, |i| i + 1));
    let trimmed = head.trim_end_matches('/');
    (if trimmed.is_empty() { head } else { trimmed }, tail)
}

/// `os.path.join` of a directory and a name.
fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() || dir.ends_with('/') {
        format!("{dir}{name}")
    } else {
        format!("{dir}/{name}")
    }
}

/// `glob.has_magic`.
fn has_magic(path: &str) -> bool {
    path.contains(['*', '?', '['])
}

/// `glob._ishidden`.
fn is_hidden(name: &str) -> bool {
    name.starts_with('.')
}

/// A segment of a pattern, as `fnmatch.fnmatchcase` matches a name.
struct Pattern(Vec<Token>);

#[derive(PartialEq)]
enum Token {
    /// `*`: any characters, none included.
    Star,
    /// `?`: one character.
    Any,
    /// The character itself.
    Is(char),
    /// `[...]`: a character in one of the ranges, or in none if `negated`.
    Set {
        negated: bool,
        ranges: Vec<(char, char)>,
    },
}

impl Pattern {
    /// The tokens of `pattern`, as `fnmatch.translate` reads them: a run of
    /// `*` is one, and a `[` that no `]` closes is itself.
    fn new(pattern: &str) -> Pattern {
        let chars: Vec<char> = pattern.chars().collect();
        let mut tokens = Vec::new();
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            i += 1;
            match c {
                '*' if tokens.last() == Some(&Token::Star) => {}
                '*' => tokens.push(Token::Star),
                '?' => tokens.push(Token::Any),
                '[' => match set(&chars[i..]) {
                    Some((token, length)) => {
                        tokens.push(token);
                        i += length;
                    }
                    None => tokens.push(Token::Is('[')),
                },
                c => tokens.push(Token::Is(c)),
            }
        }
        Pattern(tokens)
    }

    /// Whether the whole of `name` matches. Each token other than `*` takes
    /// one character, so a mismatch only needs the last `*` to take one
    /// more.
    fn matches(&self, name: &str) -> bool {
        let tokens = &self.0;
        let name: Vec<char> = name.chars().collect();
        let (mut t, mut n) = (0, 0);
        // The token after the last `*`, and where the name resumes after it.
        let mut star: Option<(usize, usize)> = None;
        while n < name.len() {
            match tokens.get(t) {
                Some(Token::Star) => {
                    star = Some((t + 1, n));
                    t += 1;
                }
                Some(token) if token.matches(name[n]) => {
                    t += 1;
                    n += 1;
                }
                _ => match star {
                    Some((after, taken)) => {
                        star = Some((after, taken + 1));
                        t = after;
                        n = taken + 1;
                    }
                    None => return false,
                },
            }
        }
        tokens[t..].iter().all(|token| *token == Token::Star)
    }
}

impl Token {
    /// Whether the token takes the character `c`; `*` is matched apart.
    fn matches(&self, c: char) -> bool {
        match self {
            Token::Star => false,
            Token::Any => true,
            Token::Is(x) => *x == c,
            Token::Set { negated, ranges } => {
                ranges.iter().any(|&(lo, hi)| lo <= c && c <= hi) != *negated
            }
        }
    }
}

/// The `[...]` that `chars`, the characters after a `[`, start with, as
/// `fnmatch.translate` reads it, and how many characters it takes, its `]`
/// included; `None` if no `]` closes it.
fn set(chars: &[char]) -> Option<(Token, usize)> {
    let n = chars.len();
    let mut j = 0;
    if j < n && chars[j] == '!' {
        j += 1;
    }
    if j < n && chars[j] == ']' {
        j += 1;
    }
    while j < n && chars[j] != ']' {
        j += 1;
    }
    if j >= n {
        return None;
    }
    // `fnmatch` splits the set at the hyphens of its ranges: not one that
    // starts it, ends it or directly follows a range...
    let mut chunks: Vec<Vec<char>> = Vec::new();
    let (mut i, mut k) = (0, if chars[0] == '!' { 2 } else { 1 });
    while let Some(hyphen) = (k..j).find(|&h| chars[h] == '-') {
        chunks.push(chars[i..hyphen].to_vec());
        i = hyphen + 1;
        k = hyphen + 3;
    }
    match chunks.last_mut() {
        Some(last) if i == j => last.push('-'),
        _ => chunks.push(chars[i..j].to_vec()),
    }
    // ...drops each range whose end comes before its start...
    for k in (1..chunks.len()).rev() {
        if chunks[k - 1].last() > chunks[k].first() {
            let next = chunks.remove(k);
            chunks[k - 1].pop();
            chunks[k - 1].extend(next.into_iter().skip(1));
        }
    }
    // ...and `re` reads what is left, the chunks joined by the hyphens of
    // the ranges (`None`), negated if it starts with `!`.
    let mut spelled: Vec<Option<char>> = Vec::new();
    for (k, chunk) in chunks.iter().enumerate() {
        if k > 0 {
            spelled.push(None);
        }
        spelled.extend(chunk.iter().copied().map(Some));
    }
    let (negated, spelled) = match spelled.split_first() {
        Some((Some('!'), rest)) => (true, rest),
        _ => (false, &spelled[..]),
    };
    // A hyphen that does not join two characters is itself.
    let mut ranges = Vec::new();
    let mut s = 0;
    while s < spelled.len() {
        let first = spelled[s].unwrap_or('-');
        if let (Some(None), Some(last)) = (spelled.get(s + 1), spelled.get(s + 2)) {
            ranges.push((first, last.unwrap_or('-')));
            s += 3;
        } else {
            ranges.push((first, first));
            s += 1;
        }
    }
    Some((Token::Set { negated, ranges }, j + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_segment_matches_as_fnmatchcase_does() {
        // Each expectation is Python 3.13's `fnmatch.fnmatchcase(name, pattern)`.
        let cases = [
            ("a**.csv", "a.csv", true),
            ("a**.csv", "ab.csv", true),
            ("***", "", true),
            ("foo[*.csv", "foo[bar.csv", true),
            ("foo[*.csv", "foobar.csv", false),
            ("x[", "x[", true),
            ("[!]", "[!]", true),
            ("[]]", "]", true),
            ("[!]]", "a", true),
            ("[!]]", "]", false),
            ("[a-c]", "b", true),
            ("[a-c]", "d", false),
            ("[!a-c]", "d", true),
            ("[-a]", "-", true),
            ("[a-]", "-", true),
            ("[a-c-e]", "-", true),
            ("[a-c-e]", "d", false),
            ("[z-a]", "a", false),
            ("[!z-a]", "q", true),
            ("[xz-ab]", "b", true),
            ("[xz-ab]", "y", false),
            ("[z-a!b]", "c", true),
            ("[z-a!b]", "b", false),
            ("[z-a!-c]", "-", false),
            ("[z-a!-c]", "b", true),
            ("[^a]", "^", true),
            ("[\\]", "\\", true),
            ("?", "é", true),
            ("*a*b", "xaab", true),
            ("*a*b", "xaba", false),
        ];
        for (pattern, name, expected) in cases {
            assert_eq!(
                Pattern::new(pattern).matches(name),
                expected,
                "{pattern} {name}"
            );
        }
    }

    #[test]
    fn a_path_splits_as_os_path_split_does() {
        let cases = [
            ("d/*.csv", ("d", "*.csv")),
            ("d//*.csv", ("d", "*.csv")),
            ("/*.csv", ("/", "*.csv")),
            ("//*.csv", ("//", "*.csv")),
            ("*.csv", ("", "*.csv")),
            ("d/*/", ("d/*", "")),
        ];
        for (path, expected) in cases {
            assert_eq!(split(path), expected, "{path}");
        }
    }
}
