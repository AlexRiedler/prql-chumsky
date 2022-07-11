use ariadne::{Color, Fmt, Label, Report, ReportKind, Source};
use chumsky::{prelude::*, stream::Stream};
use std::{collections::HashMap, env, fmt, fs};

//
// LEXER
//

pub type Span = std::ops::Range<usize>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Token {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Date(String, String, String),
    Op(String),
    Ident(String),
    Ctrl(char),
    From,
    Derive,
    Filter,
    Select,
    Group,

}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Null => write!(f, "null"),
            Token::Bool(x) => write!(f, "{}", x),
            Token::Num(n) => write!(f, "{}", n),
            Token::Str(s) => write!(f, "{}", s),
            Token::Date(y, m, d) => write!(f, "{}-{}-{}", y, m, d),
            Token::Op(s) => write!(f, "{}", s),
            Token::Ctrl(c) => write!(f, "{}", c),
            Token::Ident(s) => write!(f, "{}", s),
            Token::From => write!(f, "from"),
            Token::Derive => write!(f, "derive"),
            Token::Filter => write!(f, "filter"),
            Token::Select => write!(f, "select"),
            Token::Group => write!(f, "group"),

        }
    }
}


fn lexer() -> impl Parser<char, Vec<(Token, Span)>, Error = Simple<char>> {
    let num = text::int(10)
        .chain::<char, _, _>(just('.').chain(text::digits(10)).or_not().flatten())
        .collect::<String>()
        .map(Token::Num);

    let strings = just('"')
        .ignore_then(filter(|c| *c != '"').repeated())
        .then_ignore(just('"'))
        .collect::<String>()
        .map(Token::Str);

    let date = just('@')
        .ignore_then(text::int(10).repeated().exactly(4).collect::<String>())
        .then_ignore(just("-"))
        .then(text::int(10).repeated().exactly(2).collect::<String>())
        .then_ignore(just("-"))
        .then(text::int(10).repeated().exactly(2).collect::<String>())
        .map( |((year, month), day)| Token::Date(year, month, day)); // TODO: fix

    let op = one_of("+-*/!=><?.")
        .repeated()
        .at_least(1)
        .collect::<String>()
        .map(Token::Op);

    let ctrl = one_of("()[]{}|,").map(Token::Ctrl);

    // TODO: might have to support dots between idents
    let ident = text::ident().map(|ident: String| match ident.as_str() {
        "null" => Token::Null,
        "from" => Token::From,
        "derive" => Token::Derive,
        "filter" => Token::Filter,
        "select" => Token::Select,
        "group" => Token::Group,
        _ => Token::Ident(ident),
    });

    let token = num
        .or(date)
        .or(strings)
        .or(op)
        .or(ctrl)
        .or(ident)
        .recover_with(skip_then_retry_until([]));

    let comment = just('#').then(take_until(just('\n'))).padded();

    token
        .map_with_span(|tok, span| (tok, span))
        .padded_by(comment.repeated())
        .padded()
        .repeated()
}

//
// PARSER
//


#[derive(Clone, Debug, PartialEq)]
enum Value {
    Null,
    Bool(bool),
    Num(f64), // TODO: leave as string?
    Str(String),
    // TODO: DATE
    List(Vec<Value>),
}

#[derive(Clone, Debug, PartialEq)]
enum Operator {
    Unknown(String),
}

struct Error {
    span: Span,
    msg: String,
}

impl Value {
    fn num(self, span: Span) -> Result<f64, Error> {
        if let Value::Num(x) = self {
            Ok(x)
        } else {
            Err(Error {
                span,
                msg: format!("'{}' is not a number", self),
            })
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(x) => write!(f, "{}", x),
            Self::Num(x) => write!(f, "{}", x),
            Self::Str(x) => write!(f, "{}", x),
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

#[derive(Debug)]
enum Expr {
    Error,
    Value(Value),
    Local(String),
    List(Vec<Spanned<Self>>),
    Bool(Box<Spanned<Self>>, Spanned<Operator>, Box<Spanned<Self>>),
    Then(Box<Spanned<Self>>, Box<Spanned<Self>>),
    FromTable(String, Vec<Spanned<Self>>),
    Filter(Box<Spanned<Self>>),
    Group(Vec<Spanned<String>>, Box<Spanned<Self>>),
    Derive(Vec<Spanned<Self>>), // TODO: these are all assignments
    Assignment(String, Box<Spanned<Self>>),
    Math(Box<Spanned<Self>>, String, Box<Spanned<Self>>),
    Binary(Box<Spanned<Self>>, BinaryOp, Box<Spanned<Self>>),
}

fn expr_parser() -> impl Parser<Token, Spanned<Expr>, Error = Simple<Token>> + Clone {
    recursive(|expr: Recursive<Token, Spanned<Expr>, _>| {
        let raw_expr = recursive(|raw_expr| {
            let val = select! {
                Token::Null => Expr::Value(Value::Null),
                Token::Bool(x) => Expr::Value(Value::Bool(x)),
                Token::Num(n) => Expr::Value(Value::Num(n.parse().unwrap())),
                Token::Str(s) => Expr::Value(Value::Str(s)),
                // TODO: DATE
            }
            .labelled("value");

            let ident = select! { Token::Ident(ident) => ident.clone() }.labelled("identifier");

            let items = raw_expr
                .clone()
                .separated_by(just(Token::Ctrl(',')))
                .allow_trailing();

            let list = items
                .clone()
                .delimited_by(just(Token::Ctrl('[')), just(Token::Ctrl(']')))
                .map(Expr::List);


            let atom = val
                .or(ident.map(Expr::Local))
                .or(list)
                .map_with_span(|expr, span| (expr, span))
                .or(raw_expr
                    .clone()
                    .delimited_by(just(Token::Ctrl('(')), just(Token::Ctrl(')'))))
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
                ));

            let op = just(Token::Op("??".to_string())).to(BinaryOp::Nullify);
            let coalesce = atom.clone()
                .then(op.then(atom).repeated())
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

        let ident = select! { Token::Ident(ident) => ident.clone() }.labelled("identifier");
        let operator = select! { Token::Op(op) => op.clone() }.labelled("operator");

        let bool_expr = recursive(|raw_bool_expr| {
            raw_expr.clone()
                .map_with_span(|expr, span: Span| (expr, span))
                .then(operator)
                .then(raw_expr.clone().map_with_span(|expr, span: Span| (expr, span)))
                .map(|(((expr_a, span_a), op), (expr_b, span_b))| {
                    let span = span_a.start..span_b.end;
                    (
                        Expr::Bool(
                            Box::new(expr_a),
                            (Operator::Unknown(op), 0..0),
                            Box::new(expr_b),
                        ),
                        span
                    )
                })

        });

        let ident_list =
            ident
            .clone()
            .map_with_span(|ident, span| (ident, span))
            .separated_by(just(Token::Ctrl(',')))
            .allow_trailing()
            .delimited_by(just(Token::Ctrl('[')), just(Token::Ctrl(']')));

        let filter_expr =
            just(Token::Filter)
            .ignore_then(bool_expr)
            .map(|expr| Expr::Filter(Box::new(expr)));

        let pipeline = expr.clone()
            .delimited_by(just(Token::Ctrl('(')), just(Token::Ctrl(')')));

        let group_expr =
            just(Token::Group)
            .ignore_then(ident_list)
            .then(pipeline)
            .map(|(idents, pipeline)| Expr::Group(idents, Box::new(pipeline)));


        let calculation_expression = raw_expr.clone();

        let assignment =
            ident
            .then_ignore(just(Token::Op("=".to_string())))
            .then(calculation_expression.clone())
            .map_with_span(|(ident, expr), span| (Expr::Assignment(ident, Box::new(expr)), span));

        let derive_list =
            assignment
            .separated_by(just(Token::Ctrl(',')))
            .allow_trailing()
            .delimited_by(just(Token::Ctrl('[')), just(Token::Ctrl(']')));
            

        let derive_expr =
            just(Token::Derive)
            .ignore_then(derive_list)
            .map(|expr| Expr::Derive(expr));

        let transform = filter_expr.clone()
            .or(group_expr.clone())
            .or(derive_expr.clone())
            .then_ignore(just(Token::Ctrl('|')).or_not());

        let from_expr =
            just(Token::From)
            .ignore_then(ident)
            .then(transform.map_with_span(|expr, span: Span| (expr, span)).repeated())
            .map(|(ident, transforms)| Expr::FromTable(ident, transforms))
            .map_with_span(|expr, span: Span| (expr, span));


            /*
        let block = expr
            .clone()
            .delimited_by(just(Token::Ctrl('{')), just(Token::Ctrl('}')))
            .recover_with(nested_delimiters(
                Token::Ctrl('{'),
                Token::Ctrl('}'),
                [
                    (Token::Ctrl('('), Token::Ctrl(')')),
                    (Token::Ctrl('['), Token::Ctrl(']')),
                ],
                |span| (Expr::Error, span),
            ))
            .labelled("block");

        let block_chain = block
            .clone()
            .then(block.clone().repeated())
            .foldl(|a, b| {
                let span = a.1.start..b.1.end;
                (Expr::Then(Box::new(a), Box::new(b)), span)
            });
            */

        from_expr.clone()
            .then(expr.repeated())
            .foldl(|a, b| {
                let span = a.1.start..b.1.end;
                (
                    Expr::Then(
                        Box::new(a),
                        Box::new(b),
                    ),
                    span,
                )
            })
            .then_ignore(end())
    })
}

//
// MAIN
//

fn main() {
    let src = fs::read_to_string(env::args().nth(1).expect("Expected file argument"))
        .expect("Failed to read file");

    let (tokens, mut errs) = lexer().parse_recovery(src.as_str());

    let parse_errs = if let Some(tokens) = tokens {
        dbg!(tokens.clone());
        let len = src.chars().count();
        let (ast, parse_errs) =
            expr_parser().parse_recovery(Stream::from_iter(len..len + 1, tokens.into_iter()));

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
