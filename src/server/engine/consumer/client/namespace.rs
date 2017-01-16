
use glob::{Pattern, MatchOptions};

static MATCH_OPTIONS: MatchOptions = MatchOptions {
    case_sensitive: true,
    require_literal_separator: true,
    require_literal_leading_dot: true,
};

pub struct NamespaceGlob {
    pattern: Pattern,
}

impl NamespaceGlob {
    pub fn new(pattern: &str) -> Result<NamespaceGlob, String> {
        Pattern::new(pattern).map_err(|err| format!("Invalid namespace pattern: {:?}", err)).map(|pattern| {
            NamespaceGlob {
                pattern: pattern
            }
        })
    }

    pub fn matches(&self, namespace: &str) -> bool {
        self.pattern.matches_with(namespace, &MATCH_OPTIONS)
    }
}


#[cfg(test)]
mod test {
    use super::*;

    fn glob(pattern: &str) -> NamespaceGlob {
        NamespaceGlob::new(pattern).expect("failed to create glob")
    }

    #[test]
    fn invalid_glob_syntax_returns_an_error() {
        assert!(NamespaceGlob::new("/***").is_err());
        assert!(NamespaceGlob::new("/**foo").is_err());
        assert!(NamespaceGlob::new("/foo**").is_err());
        assert!(NamespaceGlob::new("/foo[unclosed").is_err());
    }

    #[test]
    fn test_globbing_in_subdirectories() {
        let subject = glob("/*suffix");
        assert!(subject.matches("/foo_suffix"));
        assert!(subject.matches("/suffix"));

        assert!(!subject.matches("/suffixPlusMore"));
        assert!(!subject.matches("/foo/suffix"));

        let subject = glob("/root/**/*suffix");
        assert!(subject.matches("/root/foo/bar/baz-suffix"));
        assert!(subject.matches("/root/baz-suffix"));

        let subject = glob("/root/**/foo/bar*");
        assert!(subject.matches("/root/foo/barista"));
        assert!(subject.matches("/root/this/that/foo/baritone"));
        assert!(subject.matches("/root/foo/foo/bar"));

        assert!(!subject.matches("/root/foo/goo"));
        assert!(!subject.matches("/root/bar"));
        assert!(!subject.matches("/foo/barrel"));
    }

    #[test]
    fn double_star_matches_any_number_of_subdirectories() {
        let subject = glob("/foo/**/bar");
        assert!(subject.matches("/foo/bar"));
        assert!(subject.matches("/foo/baz/bar"));
        assert!(subject.matches("/foo/this/that/the_other/bar"));

        assert!(!subject.matches("/foo/barrrrrrrr"));
        assert!(!subject.matches("/foo"));
    }

    #[test]
    fn slash_start_pattern_matches_all_top_level_namespaces() {
        let subject = glob("/*");
        assert!(subject.matches("/foo"));
        assert!(subject.matches("/bar"));

        assert!(!subject.matches("/foo/bar"));
    }

    #[test]
    fn does_not_match_when_glob_pattern_contains_no_stars() {
        let subject = glob("/this");
        assert!(!subject.matches("/that"));
    }

    #[test]
    fn matches_the_same_namespace() {
        let ns = "/test/namespace";
        let subject = glob(ns);
        assert!(subject.matches(ns));
    }
}
