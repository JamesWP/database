use super::scanner::Scanner;
use std::fmt::Debug;

pub struct Pos {
    line: usize,
    col: usize,
}

pub struct Token {
    tipe: Type,
    #[allow(dead_code)]
    start: Pos,
    #[allow(dead_code)]
    end: Pos,
}
impl Token {
    pub(crate) fn tipe(&self) -> Type {
        self.tipe.clone()
    }
}

#[derive(Debug, Clone)]
pub enum Type {
    // Single-character tokens.
    LeftParen,
    RightParen,
    Comma,
    Dot,
    Minus,
    Plus,
    Semicolon,
    Slash,
    Star,

    // One or two character tokens.
    Bang,
    BangEqual,
    Equal,
    EqualEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,

    // Literals.
    Identifier(String),
    String(String),
    IntegerNumber(i64),
    FloatingPointNumber(f64),

    // Keywords.
    Select,
    As,
    From,
    Where,
    Limit,
    False,
    True,
    Null,
    Create,
    Table,
    Integer,
    Text,
    Real,
    Blob,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Drop,
    Order,
    By,
    Asc,
    Desc,
    Group,
    Having,
    Like,
    Is,
    Not,
    Join,
    On,
    Inner,
    Index,
    Distinct,
    Explain,
    Primary,
    Key,
    Unique,
    Default,

    #[allow(dead_code)]
    Error(Error),

    Eof,
    And,
    Or,
    LeftShift,
    RightShift,
    Percent,
    Pipe,
    Caret,
    Amp,
}

#[derive(Debug, Clone)]
pub enum Error {
    UnterminatedStringLiteral,
    #[allow(dead_code)]
    UnknownCharacter(char),
    #[allow(dead_code)]
    UnknownEscape(char),
    #[allow(dead_code)]
    BadFloatingPointNumber(String),
    #[allow(dead_code)]
    BadIntegerNumber(String),
    MissingEscape,
}

impl Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.tipe.fmt(f)
    }
}

pub fn lex(input: &str) -> Vec<Token> {
    let l = Lexer::new(input);
    l.lex()
}

pub(crate) struct Lexer<'a> {
    scanner: Scanner<'a>, // replaces input + curent_lexeme

    // Current position in the input
    line: usize,
    column: usize,

    // Starting point of the current token
    start: Pos,

    tokens: Vec<Token>,
}

impl<'a> Into<Vec<Token>> for Lexer<'a> {
    fn into(self) -> Vec<Token> {
        let mut tokens = self.tokens;
        tokens.push(Token {
            tipe: Type::Eof,
            start: Pos { col: 0, line: 0 },
            end: Pos { col: 0, line: 0 },
        });
        tokens
    }
}

impl<'a> Lexer<'a> {
    pub fn new(input: &str) -> Lexer<'_> {
        Lexer {
            scanner: Scanner::new(input),
            tokens: Default::default(),
            line: 1,
            column: 0,
            start: Pos { col: 0, line: 0 },
        }
    }

    pub fn lex(mut self) -> Vec<Token> {
        loop {
            if self.is_at_end() {
                break;
            }
            let token = self.scan_token();
            self.tokens.push(token);
        }

        self.into()
    }

    fn peek(&self) -> char {
        self.scanner.peek()
    }

    fn peek_next(&self) -> char {
        self.scanner.peek_next()
    }

    fn advance(&mut self) -> char {
        self.column += 1;
        self.scanner.advance()
    }

    fn is_at_end(&self) -> bool {
        self.scanner.is_at_end()
    }

    fn scan_token(&mut self) -> Token {
        self.skip_whitespace();
        self.scanner.mark_token_start(); // replaces curent_lexeme.clear()
        self.start = Pos {
            col: self.column,
            line: self.line,
        };

        let c = self.advance();

        match c {
            '(' => self.make_token(Type::LeftParen),
            ')' => self.make_token(Type::RightParen),
            ';' => self.make_token(Type::Semicolon),
            ',' => self.make_token(Type::Comma),
            '.' => {
                // Check if this is a float starting with '.' (like .5)
                if is_digit(self.peek()) {
                    self.number()
                } else {
                    self.make_token(Type::Dot)
                }
            }
            '-' => self.make_token(Type::Minus),
            '+' => self.make_token(Type::Plus),
            '/' => self.make_token(Type::Slash),
            '*' => self.make_token(Type::Star),
            '%' => self.make_token(Type::Percent),
            '|' => self.make_token(Type::Pipe),
            '^' => self.make_token(Type::Caret),
            '&' => self.make_token(Type::Amp),
            '!' => {
                let next = self.check_next('=');
                self.make_token(if next { Type::BangEqual } else { Type::Bang })
            }
            '=' => {
                let next = self.check_next('=');
                self.make_token(if next { Type::EqualEqual } else { Type::Equal })
            }
            '<' => {
                let next_equal = self.check_next('=');
                let next_less = self.check_next('<');
                self.make_token(if next_equal {
                    Type::LessEqual
                } else if next_less {
                    Type::LeftShift
                } else {
                    Type::Less
                })
            }
            '>' => {
                let next_equal = self.check_next('=');
                let next_greater = self.check_next('>');
                self.make_token(if next_equal {
                    Type::GreaterEqual
                } else if next_greater {
                    Type::RightShift
                } else {
                    Type::Greater
                })
            }
            '\'' => self.string('\''),
            '"' => self.string('"'),
            '0'..='9' => self.number(),
            'a'..='z' | 'A'..='Z' | '_' => self.identifier(),
            c => self.make_token(Type::Error(Error::UnknownCharacter(c))),
        }
    }

    fn skip_whitespace(&mut self) {
        loop {
            match self.peek() {
                ' ' | '\r' | '\t' => {
                    self.advance();
                }
                '\n' => {
                    self.advance();
                    self.line += 1;
                    self.column = 0;
                }
                '-' => {
                    if self.peek_next() == '-' {
                        // Single line comment: -- like this
                        loop {
                            if self.peek() != '\n' && self.peek() != '\0' {
                                self.advance();
                            } else {
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                }
                '/' if self.peek_next() == '*' => {
                    self.advance(); // consume '/'
                    self.advance(); // consume '*'
                    loop {
                        if self.is_at_end() {
                            break;
                        }
                        if self.peek() == '*' && self.peek_next() == '/' {
                            self.advance(); // consume '*'
                            self.advance(); // consume '/'
                            break;
                        }
                        if self.peek() == '\n' {
                            self.line += 1;
                            self.column = 0;
                        }
                        self.advance();
                    }
                }
                _ => break,
            }
        }
    }

    fn make_token(&self, tipe: Type) -> Token {
        let start = Pos {
            col: self.start.col,
            line: self.start.line,
        };
        let end = Pos {
            col: self.column,
            line: self.line,
        };

        Token { tipe, start, end }
    }

    fn check_next(&mut self, arg: char) -> bool {
        let c = self.peek();

        if c == arg {
            self.advance();
            true
        } else {
            false
        }
    }

    fn string(&mut self, arg: char) -> Token {
        loop {
            if self.is_at_end() {
                break;
            }

            match self.peek() {
                '\\' => {
                    self.advance();
                    self.advance();
                }
                '\n' => {
                    self.line += 1;
                    self.column = 0;
                    self.advance();
                }
                c if c == arg => {
                    break;
                }
                _ => {
                    self.advance();
                }
            }
        }

        if self.is_at_end() {
            return self.make_token(Type::Error(Error::UnterminatedStringLiteral));
        }

        // The closing quote.
        self.advance();

        let slice = self.scanner.current_slice();
        let mut value = String::with_capacity(slice.len());
        let chars = slice.chars();

        // Skip the opening quote
        let mut chars = chars.skip(1).peekable();

        while let Some(c) = chars.next() {
            if chars.peek().is_none() {
                // we just took the ending quote character
                break;
            }

            match c {
                '\\' => match chars.peek() {
                    Some('t') => value.push('\t'),
                    Some('n') => value.push('\n'),
                    Some('\\') => value.push('\\'),
                    Some(c) => {
                        return self.make_token(Type::Error(Error::UnknownEscape(*c)));
                    }
                    None => {
                        return self.make_token(Type::Error(Error::MissingEscape));
                    }
                },
                c => {
                    value.push(c);
                }
            }
        }

        self.make_token(Type::String(value))
    }

    fn number(&mut self) -> Token {
        // Consume integer part (if any - might be empty for numbers like .5)
        loop {
            if !is_digit(self.peek()) {
                break;
            }
            self.advance();
        }

        // Look for a fractional part
        if self.peek() == '.' {
            // Consume the "."
            self.advance();

            // Consume fractional digits (if any - might be empty for numbers like 5.)
            loop {
                if !is_digit(self.peek()) {
                    break;
                }
                self.advance();
            }
        }

        // Look for exponent part (e.g., 1e-3, 2.5E+10)
        if self.peek() == 'e' || self.peek() == 'E' {
            self.advance(); // consume 'e' or 'E'

            // Optional sign
            if self.peek() == '+' || self.peek() == '-' {
                self.advance();
            }

            // Exponent digits (required)
            if !is_digit(self.peek()) {
                let s = self.scanner.current_slice().to_owned();
                return self.make_token(Type::Error(Error::BadFloatingPointNumber(s)));
            }

            loop {
                if !is_digit(self.peek()) {
                    break;
                }
                self.advance();
            }
        }

        let s = self.scanner.current_slice();
        // If contains '.', 'e', or 'E', it's a float
        if s.contains('.') || s.contains('e') || s.contains('E') {
            match s.parse() {
                Err(_e) => {
                    self.make_token(Type::Error(Error::BadFloatingPointNumber(s.to_owned())))
                }
                Ok(n) => self.make_token(Type::FloatingPointNumber(n)),
            }
        } else {
            match s.parse() {
                Err(_e) => self.make_token(Type::Error(Error::BadIntegerNumber(s.to_owned()))),
                Ok(n) => self.make_token(Type::IntegerNumber(n)),
            }
        }
    }

    fn identifier(&mut self) -> Token {
        // consume all characters for the identifier
        loop {
            if !is_digit(self.peek()) && !is_alpha(self.peek()) {
                break;
            }
            self.advance();
        }

        // O(1) trie dispatch via token_byte_at — no to_lowercase() allocation,
        // no chars().nth(n) re-iteration.
        let tipe = match self.scanner.token_byte_at(0) {
            b's' => match self.scanner.token_byte_at(1) {
                b'e' => match self.scanner.token_byte_at(2) {
                    b'l' => self.match_keyword("select", Type::Select),
                    b't' => self.match_keyword("set", Type::Set),
                    _ => self.make_identifier(),
                },
                _ => self.make_identifier(),
            },
            b'a' => match self.scanner.token_byte_at(1) {
                b's' => match self.scanner.token_byte_at(2) {
                    b'c' => self.match_keyword("asc", Type::Asc),
                    _ => self.match_keyword("as", Type::As),
                },
                b'n' => self.match_keyword("and", Type::And),
                _ => self.make_identifier(),
            },
            b'b' => match self.scanner.token_byte_at(1) {
                b'l' => self.match_keyword("blob", Type::Blob),
                b'y' => self.match_keyword("by", Type::By),
                _ => self.make_identifier(),
            },
            b'c' => self.match_keyword("create", Type::Create),
            b'd' => match self.scanner.token_byte_at(1) {
                b'e' => match self.scanner.token_byte_at(2) {
                    b'l' => self.match_keyword("delete", Type::Delete),
                    b's' => self.match_keyword("desc", Type::Desc),
                    b'f' => self.match_keyword("default", Type::Default),
                    _ => self.make_identifier(),
                },
                b'i' => self.match_keyword("distinct", Type::Distinct),
                b'r' => self.match_keyword("drop", Type::Drop),
                _ => self.make_identifier(),
            },
            b'e' => self.match_keyword("explain", Type::Explain),
            b'f' => match self.scanner.token_byte_at(1) {
                b'r' => self.match_keyword("from", Type::From),
                b'a' => self.match_keyword("false", Type::False),
                _ => self.make_identifier(),
            },
            b'g' => self.match_keyword("group", Type::Group),
            b'h' => self.match_keyword("having", Type::Having),
            b'j' => self.match_keyword("join", Type::Join),
            b'k' => self.match_keyword("key", Type::Key),
            b'i' => match self.scanner.token_byte_at(1) {
                b'n' => match self.scanner.token_byte_at(2) {
                    b's' => self.match_keyword("insert", Type::Insert),
                    b't' => match self.scanner.token_byte_at(3) {
                        b'e' => self.match_keyword("integer", Type::Integer),
                        b'o' => self.match_keyword("into", Type::Into),
                        _ => self.make_identifier(),
                    },
                    b'n' => self.match_keyword("inner", Type::Inner),
                    b'd' => self.match_keyword("index", Type::Index),
                    _ => self.make_identifier(),
                },
                b's' => self.match_keyword("is", Type::Is),
                _ => self.make_identifier(),
            },
            b'l' => match self.scanner.token_byte_at(1) {
                b'i' => match self.scanner.token_byte_at(2) {
                    b'm' => self.match_keyword("limit", Type::Limit),
                    b'k' => self.match_keyword("like", Type::Like),
                    _ => self.make_identifier(),
                },
                _ => self.make_identifier(),
            },
            b'n' => match self.scanner.token_byte_at(1) {
                b'u' => self.match_keyword("null", Type::Null),
                b'o' => self.match_keyword("not", Type::Not),
                _ => self.make_identifier(),
            },
            b'o' => match self.scanner.token_byte_at(1) {
                b'r' => match self.scanner.token_byte_at(2) {
                    b'd' => self.match_keyword("order", Type::Order),
                    _ => self.match_keyword("or", Type::Or),
                },
                b'n' => self.match_keyword("on", Type::On),
                _ => self.make_identifier(),
            },
            b'p' => self.match_keyword("primary", Type::Primary),
            b'r' => self.match_keyword("real", Type::Real),
            b't' => match self.scanner.token_byte_at(1) {
                b'r' => self.match_keyword("true", Type::True),
                b'a' => self.match_keyword("table", Type::Table),
                b'e' => self.match_keyword("text", Type::Text),
                _ => self.make_identifier(),
            },
            b'u' => match self.scanner.token_byte_at(1) {
                b'p' => self.match_keyword("update", Type::Update),
                b'n' => self.match_keyword("unique", Type::Unique),
                _ => self.make_identifier(),
            },
            b'v' => self.match_keyword("values", Type::Values),
            b'w' => self.match_keyword("where", Type::Where),
            _ => self.make_identifier(),
        };

        self.make_token(tipe)
    }

    /// Confirm the current token matches `keyword` case-insensitively and return
    /// the keyword type, or fall back to an Identifier token.
    fn match_keyword(&self, keyword: &str, tipe: Type) -> Type {
        if self.scanner.token_eq_keyword(keyword) {
            tipe
        } else {
            self.make_identifier()
        }
    }

    /// Build an Identifier type for the current token: one allocation.
    fn make_identifier(&self) -> Type {
        Type::Identifier(self.scanner.current_slice().to_ascii_lowercase())
    }
}

fn is_digit(c: char) -> bool {
    ('0'..='9').contains(&c)
}

fn is_alpha(c: char) -> bool {
    ('a'..='z').contains(&c) || ('A'..='Z').contains(&c) || c == '_'
}

#[cfg(test)]
mod test {
    use super::lex;

    #[test]
    fn test() {
        let input = "select t.col, t.othercol+1, finalcol*2 from tablename as t where col=1 and finalcol>0 limit 23;";
        let output = lex(input);

        println!("{:?}", input);
        println!("{:?}", output);
    }

    #[test]
    fn test_subtraction_expression() {
        // Regression test for infinite loop bug when lexer encounters
        // a single minus sign that's not part of a -- comment
        let input = "age-20";
        let tokens = lex(input);

        // Should produce: Identifier("age"), Minus, IntegerNumber(20), Eof
        assert_eq!(tokens.len(), 4);
        assert!(matches!(tokens[0].tipe(), super::Type::Identifier(_)));
        assert!(matches!(tokens[1].tipe(), super::Type::Minus));
        assert!(matches!(tokens[2].tipe(), super::Type::IntegerNumber(20)));
        assert!(matches!(tokens[3].tipe(), super::Type::Eof));
    }

    #[test]
    fn test_where_clause_with_subtraction() {
        // Regression test for the bug reported in:
        // "select name from users where age-20>10"
        let input = "select name from users where age-20>10";
        let tokens = lex(input);

        // Verify it doesn't hang and produces tokens
        assert!(tokens.len() > 0);
        assert!(matches!(tokens.last().unwrap().tipe(), super::Type::Eof));

        // Verify the subtraction is properly tokenized
        let has_minus = tokens
            .iter()
            .any(|t| matches!(t.tipe(), super::Type::Minus));
        assert!(has_minus, "Should contain a Minus token");
    }

    #[test]
    fn test_multiple_whitespace_consumption() {
        // Verify that skip_whitespace consumes multiple consecutive spaces
        let input = "select     name    from     users";
        let tokens = lex(input);

        // Should not have any whitespace tokens
        assert!(tokens
            .iter()
            .all(|t| !matches!(t.tipe(), super::Type::Identifier(s) if s.trim().is_empty())));
    }

    #[test]
    fn test_float_literal_regular() {
        // Test regular float: 4.5
        let tokens = lex("4.5");
        assert_eq!(tokens.len(), 2); // FloatingPointNumber + Eof
        match tokens[0].tipe() {
            super::Type::FloatingPointNumber(4.5) => {}
            other => panic!("Expected FloatingPointNumber(4.5), got {:?}", other),
        }
    }

    #[test]
    fn test_float_literal_leading_dot() {
        // Test float starting with dot: .5
        let tokens = lex(".5");
        assert_eq!(tokens.len(), 2); // FloatingPointNumber + Eof
        match tokens[0].tipe() {
            super::Type::FloatingPointNumber(0.5) => {}
            other => panic!("Expected FloatingPointNumber(0.5), got {:?}", other),
        }
    }

    #[test]
    fn test_float_literal_trailing_dot() {
        // Test float ending with dot: 5.
        let tokens = lex("5.");
        assert_eq!(tokens.len(), 2); // FloatingPointNumber + Eof
        match tokens[0].tipe() {
            super::Type::FloatingPointNumber(5.0) => {}
            other => panic!("Expected FloatingPointNumber(5.0), got {:?}", other),
        }
    }

    #[test]
    fn test_float_literal_point_one() {
        // Test 0.1 which has floating point representation issues
        let tokens = lex("0.1");
        assert_eq!(tokens.len(), 2); // FloatingPointNumber + Eof
        match tokens[0].tipe() {
            super::Type::FloatingPointNumber(n) if (n - 0.1).abs() < 1e-10 => {}
            other => panic!("Expected FloatingPointNumber(~0.1), got {:?}", other),
        }
    }

    #[test]
    fn test_float_literal_scientific_notation() {
        // Test scientific notation: 1e-3
        let tokens = lex("1e-3");
        assert_eq!(tokens.len(), 2); // FloatingPointNumber + Eof
        match tokens[0].tipe() {
            super::Type::FloatingPointNumber(0.001) => {}
            other => panic!("Expected FloatingPointNumber(0.001), got {:?}", other),
        }
    }

    #[test]
    fn test_float_literal_scientific_with_plus() {
        // Test scientific notation with plus: 2.5E+10
        let tokens = lex("2.5E+10");
        assert_eq!(tokens.len(), 2); // FloatingPointNumber + Eof
        match tokens[0].tipe() {
            super::Type::FloatingPointNumber(n) if n == 2.5e10 => {}
            other => panic!("Expected FloatingPointNumber(2.5e10), got {:?}", other),
        }
    }

    #[test]
    fn test_float_in_insert_statement() {
        // Regression test for bug where "4.5" was tokenized as three tokens
        let tokens = lex("INSERT INTO sprocket VALUES (4.5)");

        // Find the float token - should have exactly one
        let floats: Vec<_> = tokens
            .iter()
            .filter(|t| matches!(t.tipe(), super::Type::FloatingPointNumber(_)))
            .collect();

        assert_eq!(floats.len(), 1, "Should have exactly one float token");
        match floats[0].tipe() {
            super::Type::FloatingPointNumber(4.5) => {}
            other => panic!("Expected FloatingPointNumber(4.5), got {:?}", other),
        }
    }

    #[test]
    fn test_dot_operator_vs_float() {
        // Test that a dot followed by non-digit is still a Dot token
        let tokens = lex("foo.bar");

        // Should have: Identifier("foo"), Dot, Identifier("bar"), Eof
        assert_eq!(tokens.len(), 4);
        match (&tokens[0].tipe(), &tokens[1].tipe(), &tokens[2].tipe()) {
            (super::Type::Identifier(t), super::Type::Dot, super::Type::Identifier(c))
                if t == "foo" && c == "bar" => {}
            other => panic!(
                "Expected (Identifier(foo), Dot, Identifier(bar)), got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_mixed_numbers_in_expression() {
        // Test integers and floats mixed together: 1 + 4.5
        let tokens = lex("1 + 4.5");

        // Find integer 1
        let has_int_1 = tokens
            .iter()
            .any(|t| matches!(t.tipe(), super::Type::IntegerNumber(1)));
        assert!(has_int_1, "Should have IntegerNumber(1)");

        // Find float 4.5
        let has_float_4_5 = tokens
            .iter()
            .any(|t| matches!(t.tipe(), super::Type::FloatingPointNumber(4.5)));
        assert!(has_float_4_5, "Should have FloatingPointNumber(4.5)");
    }

    #[test]
    fn test_index_keyword() {
        let tokens = lex("CREATE INDEX idx_age ON users(age)");
        let types: Vec<super::Type> = tokens.iter().map(|t| t.tipe()).collect();
        assert!(matches!(types[0], super::Type::Create));
        assert!(matches!(types[1], super::Type::Index));
        assert!(matches!(types[2], super::Type::Identifier(_)));
        assert!(matches!(types[3], super::Type::On));
        assert!(matches!(types[4], super::Type::Identifier(_)));
        assert!(matches!(types[5], super::Type::LeftParen));
        assert!(matches!(types[6], super::Type::Identifier(_)));
        assert!(matches!(types[7], super::Type::RightParen));
    }

    #[test]
    fn test_join_keywords() {
        // Test JOIN keyword
        let input1 = "SELECT a FROM x JOIN y ON a.id = b.id";
        let tokens1 = lex(input1);
        let has_join = tokens1
            .iter()
            .any(|t| matches!(t.tipe(), super::Type::Join));
        let has_on = tokens1.iter().any(|t| matches!(t.tipe(), super::Type::On));
        assert!(has_join, "Should contain a Join token");
        assert!(has_on, "Should contain an On token");

        // Test INNER JOIN keyword
        let input2 = "SELECT a FROM x INNER JOIN y ON a.id = b.id";
        let tokens2 = lex(input2);
        let has_inner = tokens2
            .iter()
            .any(|t| matches!(t.tipe(), super::Type::Inner));
        let has_join2 = tokens2
            .iter()
            .any(|t| matches!(t.tipe(), super::Type::Join));
        let has_on2 = tokens2.iter().any(|t| matches!(t.tipe(), super::Type::On));
        assert!(has_inner, "Should contain an Inner token");
        assert!(has_join2, "Should contain a Join token");
        assert!(has_on2, "Should contain an On token");
    }

    #[test]
    fn test_block_comment_ignored() {
        let tokens = lex("/* this is a comment */ SELECT");
        assert!(tokens
            .iter()
            .any(|t| matches!(t.tipe(), super::Type::Select)));
    }

    #[test]
    fn test_block_comment_multiline() {
        let tokens = lex("/*\nline one\nline two\n*/ 42");
        assert!(
            matches!(tokens[0].tipe(), super::Type::IntegerNumber(42)),
            "Expected integer 42, got {:?}",
            tokens[0].tipe()
        );
    }
}
