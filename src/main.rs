use ariadne::{Color, Fmt, Label, Report, ReportKind, Source};
use chumsky::{prelude::*, stream::Stream};
use std::{env, fmt, fs};
use std::fs::soft_link;
use chumsky::text::{Character, ident};

//
// LEXER
//

pub type Span = std::ops::Range<usize>;

type Qualifier = (String, bool);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Token {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    FStr(String),
    SStr(String),
    Date(String, String, String),
    Op(String),
    Ident(Vec<Qualifier>),
    Type(String),
    Ctrl(char),
    Break,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Null => write!(f, "null"),
            Token::Bool(x) => write!(f, "{}", x),
            Token::Num(n) => write!(f, "{}", n),
            Token::Str(s) => write!(f, "\"{}\"", s),
            Token::FStr(s) => write!(f, "f\"{}\"", s),
            Token::SStr(s) => write!(f, "s\"{}\"", s),
            Token::Date(y, m, d) => write!(f, "{}-{}-{}", y, m, d),
            Token::Op(s) => write!(f, "{}", s),
            Token::Ctrl(c) => write!(f, "{}", c),
            Token::Ident(qualifiers) => {
                write!(f, "{}",
                       qualifiers.iter().map(|(ident, quoted)| {
                           if *quoted {
                               format!("`{}`", ident)
                           } else {
                               ident.to_string()
                           }
                       }).collect::<Vec<_>>().join(".")
                )
            },
            Token::Type(s) => write!(f, "<{}>", s),
            Token::Break => write!(f, "[break]"),
        }
    }
}

fn namespaced_ident() -> impl Parser<char, Vec<Qualifier>, Error = Simple<char>> {
    // TODO: use none_of for quoting
    let quoting = just('`');
    let qualifier = quoting.clone().or_not().then(ident()).then(quoting.clone().or_not()).map(|((qs,i),qe)| (i, qe == Some('`')));

    qualifier.clone().chain::<Qualifier, _, _>(just('.').ignore_then(qualifier.clone()).repeated())
}

fn lexer() -> impl Parser<char, Vec<(Token, Span)>, Error = Simple<char>> {
    let num = text::int(10)
        .chain::<char, _, _>(just('.').chain(text::digits(10)).or_not().flatten())
        .collect::<String>()
        .map(Token::Num);

    let string_expr =
        just('"')
        .ignore_then(filter(|c| *c != '"').repeated())
        .then_ignore(just('"'));

    let strings =
        string_expr.clone()
        .collect::<String>()
        .map(Token::Str);

    let fstrings = just('f').ignore_then(string_expr.clone()).collect::<String>().map(Token::FStr);
    let sstrings = just('s').ignore_then(string_expr.clone()).collect::<String>().map(Token::FStr);

    let date = just('@')
        .ignore_then(
            filter(|c: &char| c.is_digit(10)).repeated().exactly(4).collect::<String>()
            .then_ignore(just("-"))
            .then(filter(|c: &char| c.is_digit(10)).repeated().exactly(2).collect::<String>())
            .then_ignore(just("-"))
            .then(filter(|c: &char| c.is_digit(10)).repeated().exactly(2).collect::<String>())
        )
        .map( |((year, month), day)| Token::Date(year.to_string(), month.to_string(), day.to_string()));

    let op = one_of("+-*/!=><?.")
        .repeated()
        .at_least(1)
        .collect::<String>()
        .map(Token::Op);

    let ctrl = one_of("()[]{}|,").map(Token::Ctrl);

    /*
    TODO:
    let keywords = select! {
        "null" => Token::Null,
        "true" => Token::Bool(true),
        "false" => Token::Bool(false),
    };
     */

    let ident = namespaced_ident().map(Token::Ident);

    let token = date
        .or(num)
        .or(fstrings)
        .or(sstrings)
        .or(strings)
        .or(op)
        .or(ctrl)
        //.or(keywords)
        .or(ident)
        .recover_with(skip_then_retry_until([])); // TODO: BETTER RECOVERY STRATEGY

    let inline_whitespace = filter(|c| *c == ' ' || *c == '\t').repeated();
    let comment = just('#').then(take_until(just('\n'))).repeated();
    let breaks = just('\n').to(Token::Break).map_with_span(|a,b| (a,b)).repeated();

    breaks.clone().chain(token.map_with_span(|a,b| (a,b)).padded_by(comment).padded_by(inline_whitespace)).chain(breaks.clone()).repeated().flatten()
        /*
    token
        .map_with_span(|tok, span| (tok, span))
        .padded_by(comment.repeated())
        .padded_by(breaks)
        .padded()
        .repeated()

         */
}

//
// PARSER
//


#[derive(Clone, Debug, PartialEq)]
enum ColumnExpression {
    Null,
    Bool(bool),
    Num(f64), // TODO: leave as string?
    Range(f64, f64), // TODO: leave as string?
    Str(String),
    FStr(String),
    SStr(String),
    Date(String, String, String),
    List(Vec<ColumnExpression>),
}

#[derive(Clone, Debug, PartialEq)]
enum Operator {
    Coalesce,
    Equality,
    Unknown(String),
}

struct Error {
    span: Span,
    msg: String,
}

impl std::fmt::Display for ColumnExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(x) => write!(f, "{}", x),
            Self::Num(x) => write!(f, "{}", x),
            Self::Range(x, y) => write!(f, "{}..{}", x, y),
            Self::Str(x) => write!(f, "{}", x),
            Self::FStr(x) => write!(f, "{}", x),
            Self::SStr(x) => write!(f, "{}", x),
            Self::Date(y, m, d) => write!(f, "{}-{}-{}", y, m, d),
            Self::List(xs) => write!(
                f,
                "[{}]",
                xs.iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

pub type Spanned<T> = (T, Span);

#[derive(Clone, Debug)]
enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Nullify,
}

#[derive(Clone, Debug)]
enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug)]
struct SortColumn {
    direction: SortDirection,
    column: String,
}

#[derive(Clone, Debug)]
enum Expr {
    Error,
    Value(ColumnExpression),
    Local(Vec<Qualifier>),
    List(Vec<Spanned<Self>>),
    Bool(Box<Spanned<Self>>, Spanned<Operator>, Box<Spanned<Self>>),
    Assignment(String, Box<Spanned<Self>>),
    Binary(Box<Spanned<Self>>, BinaryOp, Box<Spanned<Self>>),
    Call(Vec<Spanned<Self>>),
    FuncCall {
        func: Box<Spanned<Self>>,
        //arg: Box<Spanned<Self>>,
        args: Vec<Spanned<Self>>,
    },
    Pipeline(Vec<Spanned<Self>>),
    Break,
}

/*
fn pipeline_expr_parser() -> impl Parser<Spanned<Expr>, Spanned<Expr>, Error = Simple<Expr>> + Clone {
    value_expr_parser().chain(value_expr_parser())
}
 */

fn value_expr_parser() -> impl Parser<Token, Spanned<Expr>, Error = Simple<Token>> + Clone {
    let val_expr = recursive(|value_expr| {
        let val = select! {
            Token::Null => Expr::Value(ColumnExpression::Null),
            Token::Bool(x) => Expr::Value(ColumnExpression::Bool(x)),
            Token::Num(n) => Expr::Value(ColumnExpression::Num(n.parse().unwrap())),
            Token::Str(s) => Expr::Value(ColumnExpression::Str(s)),
            Token::FStr(s) => Expr::Value(ColumnExpression::FStr(s)),
            Token::SStr(s) => Expr::Value(ColumnExpression::SStr(s)),
            Token::Date(y, m, d) => Expr::Value(ColumnExpression::Date(y, m, d)),
            // TODO: RANGE
        }
        .labelled("column");


        let ident = select! { Token::Ident(idents) => Expr::Local(idents) }.labelled("identifier");

        let items = value_expr
            .clone()
            .separated_by(just(Token::Ctrl(',')))
            .allow_trailing();

        let list = items
            .clone()
            .delimited_by(just(Token::Ctrl('[')), just(Token::Ctrl(']')))
            .map(Expr::List)
            .labelled("list");

        let atom = //filter(|t| *t == Token::Break).ignored().then(
        just(Token::Break).or_not().ignore_then(
            val
            .or(ident)
            .or(list)
            .map_with_span(|expr, span| (expr, span))
            .or(value_expr.clone().delimited_by(just(Token::Ctrl('(')), just(Token::Ctrl(')'))))
            .recover_with(nested_delimiters(
                Token::Ctrl('('),
                Token::Ctrl(')'),
                [
                    (Token::Ctrl('['), Token::Ctrl(']')),
                    (Token::Ctrl('{'), Token::Ctrl('}')),
                ],
                |span| (Expr::Error, span),
            ))
            .recover_with(nested_delimiters(
                Token::Ctrl('['),
                Token::Ctrl(']'),
                [
                    (Token::Ctrl('('), Token::Ctrl(')')),
                    (Token::Ctrl('{'), Token::Ctrl('}')),
                ],
                |span| (Expr::Error, span),
            ))
        );

        // TODO: this operator stuff is messy

        let op = just(Token::Op("??".to_string())).to(BinaryOp::Nullify);
        let coalesce = atom.clone()
            .then(op.then(atom.clone()).repeated())
            .foldl(|a, (op, b)| {
                let span = a.1.start..b.1.end;
                (Expr::Binary(Box::new(a), op, Box::new(b)), span)
            });
            

        let op = just(Token::Op("*".to_string())).to(BinaryOp::Mul)
            .or(just(Token::Op("/".to_string())).to(BinaryOp::Div));
        let product = coalesce
            .clone()
            .then(op.then(coalesce).repeated())
            .foldl(|a, (op, b)| {
                let span = a.1.start..b.1.end;
                (Expr::Binary(Box::new(a), op, Box::new(b)), span)
            });

        let op = just(Token::Op("+".to_string()))
            .to(BinaryOp::Add)
            .or(just(Token::Op("-".to_string())).to(BinaryOp::Sub));
        let sum = product
            .clone()
            .then(op.then(product).repeated())
            .foldl(|a, (op, b)| {
                let span = a.1.start..b.1.end;
                (Expr::Binary(Box::new(a), op, Box::new(b)), span)
            });

        let op = just(Token::Op("==".to_string()))
            .to(BinaryOp::Eq)
            .or(just(Token::Op("!=".to_string())).to(BinaryOp::NotEq));
        let compare = sum
            .clone()
            .then(op.then(sum).repeated())
            .foldl(|a, (op, b)| {
                let span = a.1.start..b.1.end;
                (Expr::Binary(Box::new(a), op, Box::new(b)), span)
            });

        compare
    });

    choice((just(Token::Break).map_with_span(|a,b|(Expr::Break,b)), val_expr)).repeated().map_with_span(|expr, span| {
        (Expr::Call(expr), span)
    })
}


//
// MAIN
//

fn main() {
    //let src = fs::read_to_string(env::args().nth(1).expect("Expected file argument"))
    //    .expect("Failed to read file");
    let src = "from employees
    derive a #= 2
    derive b #= 6
    ";

    let (tokens, mut errs) = lexer().parse_recovery(src);

    let parse_errs = if let Some(tokens) = tokens {
        dbg!(tokens.clone());
        let len = src.chars().count();
        let (ast, parse_errs) =
            value_expr_parser().parse_recovery(Stream::from_iter(len..len + 1, tokens.into_iter()));

        dbg!(ast);
        parse_errs
    } else {
        Vec::new()
    };

    errs.into_iter()
        .map(|e| e.map(|c| c.to_string()))
        .chain(parse_errs.into_iter().map(|e| e.map(|tok| tok.to_string())))
        .for_each(|e| {
            let report = Report::build(ReportKind::Error, (), e.span().start);

            let report = match e.reason() {
                chumsky::error::SimpleReason::Unclosed { span, delimiter } => report
                    .with_message(format!(
                        "Unclosed delimiter {}",
                        delimiter.fg(Color::Yellow)
                    ))
                    .with_label(
                        Label::new(span.clone())
                            .with_message(format!(
                                "Unclosed delimiter {}",
                                delimiter.fg(Color::Yellow)
                            ))
                            .with_color(Color::Yellow),
                    )
                    .with_label(
                        Label::new(e.span())
                            .with_message(format!(
                                "Must be closed before this {}",
                                e.found()
                                    .unwrap_or(&"end of file".to_string())
                                    .fg(Color::Red)
                            ))
                            .with_color(Color::Red),
                    ),
                chumsky::error::SimpleReason::Unexpected => report
                    .with_message(format!(
                        "{}, expected {}",
                        if e.found().is_some() {
                            "Unexpected token in input"
                        } else {
                            "Unexpected end of input"
                        },
                        if e.expected().len() == 0 {
                            "something else".to_string()
                        } else {
                            e.expected()
                                .map(|expected| match expected {
                                    Some(expected) => expected.to_string(),
                                    None => "end of input".to_string(),
                                })
                                .collect::<Vec<_>>()
                                .join(", ")
                        }
                    ))
                    .with_label(
                        Label::new(e.span())
                            .with_message(format!(
                                "Unexpected token {}",
                                e.found()
                                    .unwrap_or(&"end of file".to_string())
                                    .fg(Color::Red)
                            ))
                            .with_color(Color::Red),
                    ),
                chumsky::error::SimpleReason::Custom(msg) => report.with_message(msg).with_label(
                    Label::new(e.span())
                        .with_message(format!("{}", msg.fg(Color::Red)))
                        .with_color(Color::Red),
                ),
            };

            report.finish().print(Source::from(&src)).unwrap();
        });
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_bool_expr() -> Result<(), ()> {
        //let src = "1 + 2 == 3";
        //let src = "take 1";
        //let src = "(take 1)";
        //let src = "(take 1) (from employees)";
        //let src = "(take 1) (from employees)";
        //let src = "from employees (top_one)";
        //let src = "take (a - 2) (from employees)";
        //let src = "testing.`uniq` [a,b,c] (from employees)";
        let src = "
from employees
derive
[
    gross_salary ## = salary + tax
    gross_cost ## = salary + benefits
]
        ";
        let (tokens, mut errs) = lexer().parse_recovery(src);

        let parse_errs =
            if let Some(tokens) = tokens {
                dbg!(tokens.clone());
                let len = src.chars().count();
                let (ast, parse_errs) =
                    value_expr_parser().parse_recovery(Stream::from_iter(len..len + 1, tokens.into_iter()));

                dbg!(ast);
                parse_errs
            } else {
                Vec::new()
            };

        errs.into_iter()
            .map(|e| e.map(|c| c.to_string()))
            .chain(parse_errs.into_iter().map(|e| e.map(|tok| tok.to_string())))
            .for_each(|e| {
                let report = Report::build(ReportKind::Error, (), e.span().start);

                let report = match e.reason() {
                    chumsky::error::SimpleReason::Unclosed { span, delimiter } => report
                        .with_message(format!(
                            "Unclosed delimiter {}",
                            delimiter.fg(Color::Yellow)
                        ))
                        .with_label(
                            Label::new(span.clone())
                                .with_message(format!(
                                    "Unclosed delimiter {}",
                                    delimiter.fg(Color::Yellow)
                                ))
                                .with_color(Color::Yellow),
                        )
                        .with_label(
                            Label::new(e.span())
                                .with_message(format!(
                                    "Must be closed before this {}",
                                    e.found()
                                        .unwrap_or(&"end of file".to_string())
                                        .fg(Color::Red)
                                ))
                                .with_color(Color::Red),
                        ),
                    chumsky::error::SimpleReason::Unexpected => report
                        .with_message(format!(
                            "{}, expected {}",
                            if e.found().is_some() {
                                "Unexpected token in input"
                            } else {
                                "Unexpected end of input"
                            },
                            if e.expected().len() == 0 {
                                "something else".to_string()
                            } else {
                                e.expected()
                                    .map(|expected| match expected {
                                        Some(expected) => expected.to_string(),
                                        None => "end of input".to_string(),
                                    })
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            }
                        ))
                        .with_label(
                            Label::new(e.span())
                                .with_message(format!(
                                    "Unexpected token {}",
                                    e.found()
                                        .unwrap_or(&"end of file".to_string())
                                        .fg(Color::Red)
                                ))
                                .with_color(Color::Red),
                        ),
                    chumsky::error::SimpleReason::Custom(msg) => report.with_message(msg).with_label(
                        Label::new(e.span())
                            .with_message(format!("{}", msg.fg(Color::Red)))
                            .with_color(Color::Red),
                    ),
                };

                report.finish().print(Source::from(&src)).unwrap();
            });

        Ok(())
    }
}