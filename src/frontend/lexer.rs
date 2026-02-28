use peekmore::{PeekMore, PeekMoreIterator};
use std::{fmt::Debug, str::Chars};

pub struct Pos {
    line: usize,
    col: usize,
}

pub struct Token {
    tipe: Type,
    lexeme: String,
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
        core::fmt::Debug::fmt(&self.lexeme, f)
        // self.tipe.fmt(f)
    }
}

pub fn lex(input: &str) -> Vec<Token> {
    let l = Lexer::new(input);
    l.lex()
}

pub(crate) struct Lexer<'a> {
    input: PeekMoreIterator<Chars<'a>>,

    // Current position in the input
    line: usize,
    column: usize,

    // Starting point of the curent token
    start: Pos,

    curent_lexeme: String,

    tokens: Vec<Token>,
}

impl<'a> Into<Vec<Token>> for Lexer<'a> {
    fn into(mut self) -> Vec<Token> {
        let mut token = self.make_token(Type::Eof);
        token.lexeme.clear();
        self.tokens.push(token);
        self.tokens
    }
}

impl<'a> Lexer<'a> {
    pub fn new(input: &str) -> Lexer<'_> {
        Lexer {
            input: input.chars().peekmore(),
            tokens: Default::default(),
            line: 1,
            column: 0,
            start: Pos { col: 0, line: 0 },
            curent_lexeme: String::new(),
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

    fn peek(&mut self) -> char {
        match self.input.peek() {
            Some(c) => *c,
            None => '\0',
        }
    }

    fn peek_next(&mut self) -> char {
        match self.input.peek_nth(1) {
            Some(c) => *c,
            None => '\0',
        }
    }

    fn advance(&mut self) -> char {
        self.column += 1;

        let c = match self.input.next() {
            Some(c) => c,
            None => '\0',
        };

        self.curent_lexeme.push(c);

        c
    }

    fn is_at_end(&mut self) -> bool {
        self.peek() == '\0'
    }

    fn scan_token(&mut self) -> Token {
        self.skip_whitespace();

        self.start = Pos {
            col: self.column,
            line: self.line,
        };
        self.curent_lexeme.clear();

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
                    // Continue consuming whitespace
                }
                '\n' => {
                    self.advance();
                    self.line += 1;
                    self.column = 0;
                    // Continue consuming whitespace
                }
                '-' => {
                    if self.peek_next() == '-' {
                        // Single line comment: -- like this
                        loop {
                            if self.peek() != '\n' && self.peek() != '\0' {
                                self.advance();
                            } else {
                                // we leave the '/n' in the input for the next loop in skip_whitespace to handle
                                break;
                            }
                        }
                    } else {
                        // Single '-' is not whitespace, stop here
                        break;
                    }
                }
                _ => break,
            }
        }
    }

    fn make_token(&mut self, tipe: Type) -> Token {
        let start = Pos {
            col: self.start.col,
            line: self.start.line,
        };
        let end = Pos {
            col: self.column,
            line: self.line,
        };

        Token {
            tipe,
            lexeme: self.curent_lexeme.clone(),
            start,
            end,
        }
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

            // This is a single character escape sequence
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

        let mut value = String::with_capacity(self.curent_lexeme.len());
        let chars = self.curent_lexeme.chars();

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
                return self.make_token(Type::Error(Error::BadFloatingPointNumber(
                    self.curent_lexeme.to_owned(),
                )));
            }

            loop {
                if !is_digit(self.peek()) {
                    break;
                }
                self.advance();
            }
        }

        // If contains '.', 'e', or 'E', it's a float
        if self.curent_lexeme.contains('.')
            || self.curent_lexeme.contains('e')
            || self.curent_lexeme.contains('E')
        {
            let n = self.curent_lexeme.parse();
            match n {
                Err(_e) => self.make_token(Type::Error(Error::BadFloatingPointNumber(
                    self.curent_lexeme.to_owned(),
                ))),
                Ok(n) => self.make_token(Type::FloatingPointNumber(n)),
            }
        } else {
            let n = self.curent_lexeme.parse();
            match n {
                Err(_e) => self.make_token(Type::Error(Error::BadIntegerNumber(
                    self.curent_lexeme.to_owned(),
                ))),
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

        let ident: String = self.curent_lexeme.clone().to_lowercase();
        let ident = ident.as_str();

        let tipe = match ident.chars().next().unwrap() {
            's' => match ident.chars().nth(1) {
                Some('e') => match ident.chars().nth(2) {
                    Some('l') => match_reserved(ident, "select", Type::Select),
                    Some('t') => match_reserved(ident, "set", Type::Set),
                    _ => Type::Identifier(ident.to_owned()),
                },
                _ => Type::Identifier(ident.to_owned()),
            },
            'a' => match ident.chars().nth(1) {
                Some('s') => match ident.chars().nth(2) {
                    Some('c') => match_reserved(ident, "asc", Type::Asc),
                    _ => match_reserved(ident, "as", Type::As),
                },
                Some('n') => match_reserved(ident, "and", Type::And),
                _ => Type::Identifier(ident.to_owned()),
            },
            'b' => match ident.chars().nth(1) {
                Some('l') => match_reserved(ident, "blob", Type::Blob),
                Some('y') => match_reserved(ident, "by", Type::By),
                _ => Type::Identifier(ident.to_owned()),
            },
            'c' => match_reserved(ident, "create", Type::Create),
            'd' => match ident.chars().nth(1) {
                Some('e') => match ident.chars().nth(2) {
                    Some('l') => match_reserved(ident, "delete", Type::Delete),
                    Some('s') => match_reserved(ident, "desc", Type::Desc),
                    _ => Type::Identifier(ident.to_owned()),
                },
                Some('i') => match_reserved(ident, "distinct", Type::Distinct),
                Some('r') => match_reserved(ident, "drop", Type::Drop),
                _ => Type::Identifier(ident.to_owned()),
            },
            'e' => match_reserved(ident, "explain", Type::Explain),
            'f' => match ident.chars().nth(1) {
                Some('r') => match_reserved(ident, "from", Type::From),
                Some('a') => match_reserved(ident, "false", Type::False),
                _ => Type::Identifier(ident.to_owned()),
            },
            'g' => match_reserved(ident, "group", Type::Group),
            'h' => match_reserved(ident, "having", Type::Having),
            'j' => match_reserved(ident, "join", Type::Join),
            'k' => match_reserved(ident, "key", Type::Key),
            'i' => match ident.chars().nth(1) {
                Some('n') => match ident.chars().nth(2) {
                    Some('s') => match_reserved(ident, "insert", Type::Insert),
                    Some('t') => match ident.chars().nth(3) {
                        Some('e') => match_reserved(ident, "integer", Type::Integer),
                        Some('o') => match_reserved(ident, "into", Type::Into),
                        _ => Type::Identifier(ident.to_owned()),
                    },
                    Some('n') => match_reserved(ident, "inner", Type::Inner),
                    Some('d') => match_reserved(ident, "index", Type::Index),
                    _ => Type::Identifier(ident.to_owned()),
                },
                Some('s') => match_reserved(ident, "is", Type::Is),
                _ => Type::Identifier(ident.to_owned()),
            },
            'l' => match ident.chars().nth(1) {
                Some('i') => match ident.chars().nth(2) {
                    Some('m') => match_reserved(ident, "limit", Type::Limit),
                    Some('k') => match_reserved(ident, "like", Type::Like),
                    _ => Type::Identifier(ident.to_owned()),
                },
                _ => Type::Identifier(ident.to_owned()),
            },
            'n' => match ident.chars().nth(1) {
                Some('u') => match_reserved(ident, "null", Type::Null),
                Some('o') => match_reserved(ident, "not", Type::Not),
                _ => Type::Identifier(ident.to_owned()),
            },
            'o' => match ident.chars().nth(1) {
                Some('r') => match ident.chars().nth(2) {
                    Some('d') => match_reserved(ident, "order", Type::Order),
                    _ => match_reserved(ident, "or", Type::Or),
                },
                Some('n') => match_reserved(ident, "on", Type::On),
                _ => Type::Identifier(ident.to_owned()),
            },
            'p' => match_reserved(ident, "primary", Type::Primary),
            'r' => match_reserved(ident, "real", Type::Real),
            't' => match ident.chars().nth(1) {
                Some('r') => match_reserved(ident, "true", Type::True),
                Some('a') => match_reserved(ident, "table", Type::Table),
                Some('e') => match_reserved(ident, "text", Type::Text),
                _ => Type::Identifier(ident.to_owned()),
            },
            'u' => match ident.chars().nth(1) {
                Some('p') => match_reserved(ident, "update", Type::Update),
                Some('n') => match_reserved(ident, "unique", Type::Unique),
                _ => Type::Identifier(ident.to_owned()),
            },
            'v' => match_reserved(ident, "values", Type::Values),
            'w' => match_reserved(ident, "where", Type::Where),
            _ => Type::Identifier(ident.to_owned()),
        };

        self.make_token(tipe)
    }
}

fn match_reserved(ident: &str, possible_keyword: &str, tipe: Type) -> Type {
    if ident == possible_keyword {
        tipe
    } else {
        Type::Identifier(ident.to_owned())
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
}
