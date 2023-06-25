use peekmore::{PeekMore, PeekMoreIterator};
use std::str::Chars;

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
    Number(i64),

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
}

pub enum Error {
    UnterminatedStringLiteral
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
    fn into(self) -> Vec<Token> {
        todo!("add EOF token");
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
            self.start = Pos { col: self.column, line: self.line };
            self.curent_lexeme.clear();
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

        match self.input.next() {
            Some(c) => c,
            None => '\0',
        }
    }

    fn is_at_end(&mut self) -> bool {
        self.peek() != '\0'
    }

    fn scan_token(&mut self) -> Token {
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
            '!' => {
                let next = self.check_next('=');
                self.make_token(if next { Type::BangEqual } else { Type::Bang })
            }
            '=' => {
                let next = self.check_next('=');
                self.make_token(if next { Type::EqualEqual } else { Type::Equal })
            }
            '<' => {
                let next = self.check_next('=');
                self.make_token(if next { Type::LessEqual } else { Type::Less })
            }
            '>' => {
                let next = self.check_next('=');
                self.make_token(if next {
                    Type::GreaterEqual
                } else {
                    Type::Greater
                })
            }
            '\'' => self.string('\''),
            '"' => self.string('"'),
            '0' ..= '9' => self.number(),
            'a' ..= 'z' | 'A' ..= 'Z' | '_' => self.identifier(),
            _ => todo!(),
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
        let start = Pos{ col: self.start.col,  line: self.start.line };
        let end = Pos{ col: self.column,  line: self.line };

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
            if self.peek() == arg {
                break;
            }
            if self.is_at_end() {
                break;
            }

            if self.peek() == '\n' {
                self.line += 1;
                self.column = 0;
            }

            self.advance();
        }

        if self.is_at_end() {
            return self.make_token(Type::Error(Error::UnterminatedStringLiteral));
        }

        // The closing quote.
        self.advance();
        self.make_token(Type::String(todo!()))
    }

    fn number(&mut self) -> Token {
        loop {
            if !is_digit(self.peek()){
                break;
            }
            self.advance();
        } 

        // Look for a fractional part.
        if self.peek() == '.' && is_digit(self.peek_next()){
            // Consume the ".".
            self.advance();

            loop {
                if !is_digit(self.peek()) {
                    break;
                }
                self.advance();
            }
        }

        let n = todo!();
        self.make_token(Type::Number(n))
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
        let ident= ident.as_str();

        let tipe = match ident.chars().next().unwrap() {
            's' => match_reserved(ident, "select", Type::Select),
            'a' => match_reserved(ident, "as", Type::As),
            'f' => {
                match ident.chars().nth(1) {
                    Some('r') => match_reserved(ident, "from", Type::From),
                    Some('a') => match_reserved(ident, "false", Type::False),
                    _ => Type::Identifier(ident.to_owned())
                }
            },
            'w' => match_reserved(ident, "where", Type::Where),
            'l' => match_reserved(ident, "limit", Type::Where),
            't' => match_reserved(ident, "true", Type::True),
            'n' => match_reserved(ident, "null", Type::Null),
            _ => Type::Identifier(ident.to_owned())
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
    ('a' ..= 'z').contains(&c) || ('A' ..= 'Z').contains(&c)  || c == '_'
}
