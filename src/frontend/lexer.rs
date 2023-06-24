use std::{str::Chars, iter::Peekable};


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

pub enum Identifiers {

}

pub enum Type {
  // Single-character tokens.
  LeftParen, RightParen, LeftBrace, RightBrace,
  Comma, Dot, Minus, Plus, Semicolon, Slash, Star,

  // One or two character tokens.
  Bang, BangEqual,
  Equal, EqualEqual,
  Greater, GreaterEqual,
  Less, LessEqual,

  // Literals.
  Identifier(Identifiers), String(String), Number(i64),

  // Keywords.
  Select, From, Where, Limit,
  False, Null, True,

  Eof,
}

pub fn lex(input: &str) -> Vec<Token> {
    let mut l = Lexer::new(input);

    loop {
        if l.isAtEnd() {
            break;
        }
        l.beginLexeme();
        l.scanToken();
    }

    l.into()
}

struct Lexer<'a> {
    input: Peekable<Chars<'a>>,

    tokens: Vec<Token>
}

impl<'a> Into<Vec<Token>> for Lexer<'a> {
    fn into(self) -> Vec<Token> {
        todo!("add EOF token");
        self.tokens
    }
}

impl<'a> Lexer<'a> {
    pub fn new(input: &str) -> Lexer {
        Lexer { input: input.chars().peekable(), tokens: Default::default() }
    }

    pub fn peek(&mut self) -> char {
        match self.input.peek() {
            Some(c) => *c,
            None => '\0',
        }
    }

    pub fn next(&mut self) -> char {
        match self.input.next() {
            Some(c) => c,
            None => '\0',
        }
    }

    fn isAtEnd(&mut self) -> bool {
        self.peek() != '\0'
    }

    fn beginLexeme(&self) {
        todo!("set pos correctly")
    }

    fn scanToken(&self) {
        todo!()
    }
}