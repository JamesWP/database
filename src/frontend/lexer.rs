use peekmore::{PeekMore, PeekMoreIterator};
use std::{
    fmt::{Debug, Display},
    str::Chars,
};

pub struct Pos {
    line: usize,
    col: usize,
}

pub struct Token {
    tipe: Type,
    lexeme: String,
    start: Pos,
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
    UnknownCharacter(char),
    UnknownEscape(char),
    BadFloatingPointNumber(String),
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
    let mut l = Lexer::new(input);
    l.lex()
}

struct Lexer<'a> {
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
    pub fn new(input: &str) -> Lexer {
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
        match self.input.peek_nth(2) {
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
            '.' => self.make_token(Type::Dot),
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
                    break;
                }
                '\n' => {
                    self.advance();
                    self.line += 1;
                    self.column = 0;
                    break;
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
        let mut chars = self.curent_lexeme.chars();

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
        loop {
            if !is_digit(self.peek()) {
                break;
            }
            self.advance();
        }

        // Look for a fractional part.
        if self.peek() == '.' && is_digit(self.peek_next()) {
            // Consume the ".".
            self.advance();

            loop {
                if !is_digit(self.peek()) {
                    break;
                }
                self.advance();
            }
        }

        if self.curent_lexeme.contains('.') {
            let n = self.curent_lexeme.parse();
            match n {
                Err(e) => self.make_token(Type::Error(Error::BadFloatingPointNumber(
                    self.curent_lexeme.to_owned(),
                ))),
                Ok(n) => self.make_token(Type::FloatingPointNumber(n)),
            }
        } else {
            let n = self.curent_lexeme.parse();
            match n {
                Err(e) => self.make_token(Type::Error(Error::BadIntegerNumber(
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
            's' => match_reserved(ident, "select", Type::Select),
            'a' => match ident.chars().nth(1) {
                Some('s') => match_reserved(ident, "as", Type::As),
                Some('n') => match_reserved(ident, "and", Type::And),
                _ => Type::Identifier(ident.to_owned()),
            },
            'f' => match ident.chars().nth(1) {
                Some('r') => match_reserved(ident, "from", Type::From),
                Some('a') => match_reserved(ident, "false", Type::False),
                _ => Type::Identifier(ident.to_owned()),
            },
            'w' => match_reserved(ident, "where", Type::Where),
            'o' => match_reserved(ident, "or", Type::Or),
            'l' => match_reserved(ident, "limit", Type::Limit),
            't' => match_reserved(ident, "true", Type::True),
            'n' => match_reserved(ident, "null", Type::Null),
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
}
