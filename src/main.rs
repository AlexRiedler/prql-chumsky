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
    FStr(String),
    Date(String, String, String),
    Op(String),
    Ident(String),
    Ctrl(char),
    From,
    Derive,
    Filter,
    Select,
    Group,
    Aggregate,
    Sort,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Null => write!(f, "null"),
            Token::Bool(x) => write!(f, "{}", x),
            Token::Num(n) => write!(f, "{}", n),
            Token::Str(s) => write!(f, "\"{}\"", s),
            Token::FStr(s) => write!(f, "f\"{}\"", s),
            Token::Date(y, m, d) => write!(f, "{}-{}-{}", y, m, d),
            Token::Op(s) => write!(f, "{}", s),
            Token::Ctrl(c) => write!(f, "{}", c),
            Token::Ident(s) => write!(f, "{}", s),
            Token::From => write!(f, "from"),
            Token::Derive => write!(f, "derive"),
            Token::Filter => write!(f, "filter"),
            Token::Select => write!(f, "select"),
            Token::Group => write!(f, "group"),
            Token::Aggregate => write!(f, "aggregate"),
            Token::Sort => write!(f, "sort"),
        }
    }
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
        "aggregate" => Token::Aggregate,
        "sort" => Token::Sort,
        _ => Token::Ident(ident),
    });

    let token = num
        .or(date)
        .or(fstrings)
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
    Range(f64, f64), // TODO: leave as string?
    Str(String),
    FStr(String),
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
            Self::Range(x, y) => write!(f, "{}..{}", x, y),
            Self::Str(x) => write!(f, "{}", x),
            Self::FStr(x) => write!(f, "{}", x),
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
    Group(Vec<Spanned<String>>, Vec<Spanned<Self>>),
    Aggregate(Vec<Spanned<Self>>),
    Derive(Vec<Spanned<Self>>), // TODO: these are all assignments
    Sort(Vec<Spanned<SortColumn>>),
    Take(Value),
    Assignment(String, Box<Spanned<Self>>),
    Math(Box<Spanned<Self>>, String, Box<Spanned<Self>>),
    Binary(Box<Spanned<Self>>, BinaryOp, Box<Spanned<Self>>),
    Average(Box<Spanned<Self>>),
    Sum(Box<Spanned<Self>>),
}

fn expr_parser() -> impl Parser<Token, Spanned<Expr>, Error = Simple<Token>> + Clone {
    recursive(|expr: Recursive<Token, Spanned<Expr>, _>| {
        let raw_expr = recursive(|raw_expr| {
            let val = select! {
                Token::Null => Expr::Value(Value::Null),
                Token::Bool(x) => Expr::Value(Value::Bool(x)),
                Token::Num(n) => Expr::Value(Value::Num(n.parse().unwrap())),
                Token::Str(s) => Expr::Value(Value::Str(s)),
                Token::FStr(s) => Expr::Value(Value::FStr(s)),
                // TODO: DATE
            }
            .labelled("value");

            let ident = select! { Token::Ident(ident) => ident.clone() }.labelled("identifier");

            let items = raw_expr
                .clone()
                .separated_by(just(Token::Ctrl(',')))
                .allow_trailing();

            let average =
                just(Token::Ident("average".to_string()))
                .ignore_then(raw_expr.clone())
                .map(|expr| Expr::Average(Box::new(expr)));

            let sum =
                just(Token::Ident("sum".to_string()))
                .ignore_then(raw_expr.clone())
                .map(|expr| Expr::Sum(Box::new(expr)));

            let list = items
                .clone()
                .delimited_by(just(Token::Ctrl('[')), just(Token::Ctrl(']')))
                .map(Expr::List)
                .labelled("list");

            let atom = val
                .or(average)
                .or(sum)
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

        // TODO: can do better then this
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

        let transform_expr = recursive(|t_expr| {
            let ident_list =
                ident
                .clone()
                .map_with_span(|ident, span| (ident, span))
                .separated_by(just(Token::Ctrl(',')))
                .allow_trailing()
                .delimited_by(just(Token::Ctrl('[')).or_not(), just(Token::Ctrl(']')).or_not());

            // TODO: make into Expr::Transform(Transform::Filter(Box::new(expr)))
            let filter_expr =
                just(Token::Filter)
                .ignore_then(bool_expr)
                .map(|expr| Expr::Filter(Box::new(expr)));

            let assignment =
                ident
                .then_ignore(just(Token::Op("=".to_string())))
                .then(raw_expr.clone())
                .map_with_span(|(ident, expr), span| (Expr::Assignment(ident, Box::new(expr)), span));

            let aggregate_transform =
                just(Token::Aggregate)
                .ignore_then(
                    assignment.clone().or(raw_expr.clone())
                        .separated_by(just(Token::Ctrl(','))).allow_trailing()
                        .delimited_by(just(Token::Ctrl('[')), just(Token::Ctrl(']')))
                )
                .map(Expr::Aggregate);


            let group_transform =
                just(Token::Group)
                .ignore_then(ident_list)
                .then(
                    t_expr.clone()
                    .separated_by(just(Token::Ctrl(','))).allow_trailing()
                    .delimited_by(just(Token::Ctrl('(')), just(Token::Ctrl(')')))
                )
                .map(|(idents, pipeline)| Expr::Group(idents, pipeline));


            let derive_transform =
                just(Token::Derive)
                .ignore_then(
                    assignment
                    .separated_by(just(Token::Ctrl(',')))
                    .allow_trailing()
                    .delimited_by(just(Token::Ctrl('[')).or_not(), just(Token::Ctrl(']')).or_not())
                )
                .map(|expr| Expr::Derive(expr));


            let sort_direction = just(Token::Op("-".to_string())).to(SortDirection::Desc).or(just(Token::Op("+".to_string())).to(SortDirection::Asc));
            let sort_column = sort_direction.or_not().map(|dir| match dir { Some(d) => d, None => SortDirection::Asc }).then(ident.clone())
                .map(|(direction, column)| SortColumn { direction, column });

            let sort_columns =
                sort_column
                .map_with_span(|expr, span| (expr, span))
                .separated_by(just(Token::Ctrl(',')))
                .allow_trailing()
                .delimited_by(just(Token::Ctrl('[')).or_not(), just(Token::Ctrl(']')).or_not())
                .map(|sort_columns| Expr::Sort(sort_columns));

            let sort_transform = just(Token::Sort).ignore_then(sort_columns);

            let number = select! { Token::Num(n) => n.parse().unwrap() }.labelled("number");
            let number_expr = number.map(|n| Value::Num(n));

            let number_range =
                number
                .then_ignore(just(Token::Op("..".to_string())))
                .then(number)
                .map(|(a, b)| Value::Range(a, b));

            let take_transform =
                just(Token::Ident("take".to_string()))
                .ignore_then(number_range.or(number_expr))
                .map(|num_or_range| Expr::Take(num_or_range));

            let from_transform =
                just(Token::From)
                .ignore_then(ident)
                .then(t_expr.repeated())
                .map(|(ident, transforms)| Expr::FromTable(ident, transforms));

            from_transform
                .or(filter_expr.clone())
                .or(group_transform.clone())
                .or(derive_transform.clone())
                .or(aggregate_transform.clone())
                .or(sort_transform.clone())
                .or(take_transform.clone())
                .then_ignore(just(Token::Ctrl('|')).or_not())
                .map_with_span(|expr, span: Span| (expr, span))
        });

        transform_expr.clone()
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
