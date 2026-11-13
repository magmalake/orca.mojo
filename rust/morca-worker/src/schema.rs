//! The output schema, declared once as an Iceberg schema JSON file that the
//! Mojo proxy pipeline (`Schema.parse`) and the worker (`include_str!`) both
//! read.
//!
//! - [`to_arrow`] makes the Arrow schema a worker builds its batches with,
//!   each field carrying its Iceberg id (`PARQUET:field_id`).
//! - [`check`] says whether an Arrow schema fits the file, by the rule
//!   morca applies to the stream before it writes anything; call it, or
//!   [`assert_fits`], from a `cargo test`, so drift fails there first.
//!
//! **The rule.** Columns are matched by name, at every depth. Every column
//! of the table must be in the worker's schema and no other; a required one
//! must not be nullable. A type fits when it's the one [`to_arrow`] gives,
//! or one Iceberg widens to it: `int` takes Int8/Int16/Int32, `long` those
//! and Int64, `double` Float32 or Float64, `string` Utf8 or LargeUtf8,
//! `binary` Binary or LargeBinary, a `decimal(P,S)` any Decimal128 of scale
//! S and precision up to P, and a timestamp any time unit (with a time
//! zone for `timestamptz`, without one for `timestamp`).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use serde_json::Value;

/// The metadata key a field's Iceberg id goes in, as Parquet and Iceberg
/// readers expect it.
pub const FIELD_ID_KEY: &str = "PARQUET:field_id";

/// An Iceberg type, as the schema JSON spells it.
#[derive(Debug, Clone)]
enum IType {
    Prim(String),
    Struct(Vec<IField>),
    List {
        id: i64,
        element: Box<IType>,
        required: bool,
    },
    Map {
        key_id: i64,
        key: Box<IType>,
        value_id: i64,
        value: Box<IType>,
        value_required: bool,
    },
}

#[derive(Debug, Clone)]
struct IField {
    id: i64,
    name: String,
    required: bool,
    ty: IType,
}

fn err<T>(msg: impl Into<String>) -> Result<T, String> {
    Err(msg.into())
}

fn parse_type(v: &Value, at: &str) -> Result<IType, String> {
    if let Some(s) = v.as_str() {
        return Ok(IType::Prim(s.to_owned()));
    }
    let o = v
        .as_object()
        .ok_or_else(|| format!("schema: {at}'s type is neither a name nor an object"))?;
    let kind = o.get("type").and_then(Value::as_str).unwrap_or("");
    let int = |k: &str| -> Result<i64, String> {
        o.get(k)
            .and_then(Value::as_i64)
            .ok_or_else(|| format!("schema: {at} has no {k}"))
    };
    let flag = |k: &str| o.get(k).and_then(Value::as_bool).unwrap_or(false);
    match kind {
        "struct" => Ok(IType::Struct(parse_fields(o.get("fields"), at)?)),
        "list" => Ok(IType::List {
            id: int("element-id")?,
            element: Box::new(parse_type(
                o.get("element")
                    .ok_or_else(|| format!("schema: {at} has no element"))?,
                &format!("{at}.element"),
            )?),
            required: flag("element-required"),
        }),
        "map" => Ok(IType::Map {
            key_id: int("key-id")?,
            key: Box::new(parse_type(
                o.get("key")
                    .ok_or_else(|| format!("schema: {at} has no key"))?,
                &format!("{at}.key"),
            )?),
            value_id: int("value-id")?,
            value: Box::new(parse_type(
                o.get("value")
                    .ok_or_else(|| format!("schema: {at} has no value"))?,
                &format!("{at}.value"),
            )?),
            value_required: flag("value-required"),
        }),
        other => err(format!(
            "schema: {at} has type {other:?}, not struct, list or map"
        )),
    }
}

fn parse_fields(v: Option<&Value>, at: &str) -> Result<Vec<IField>, String> {
    let list = v.and_then(Value::as_array).ok_or_else(|| {
        format!(
            "schema: {} has no fields",
            if at.is_empty() { "the schema" } else { at }
        )
    })?;
    let mut out = Vec::new();
    for f in list {
        let name = f
            .get("name")
            .and_then(Value::as_str)
            .ok_or("schema: a field has no name")?
            .to_owned();
        let path = if at.is_empty() {
            name.clone()
        } else {
            format!("{at}.{name}")
        };
        out.push(IField {
            id: f
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("schema: {path} has no id"))?,
            required: f.get("required").and_then(Value::as_bool).unwrap_or(false),
            ty: parse_type(
                f.get("type")
                    .ok_or_else(|| format!("schema: {path} has no type"))?,
                &path,
            )?,
            name,
        });
    }
    Ok(out)
}

fn parse(json: &str) -> Result<Vec<IField>, String> {
    let v: Value = serde_json::from_str(json).map_err(|e| format!("schema: not JSON: {e}"))?;
    parse_fields(v.get("fields"), "")
}

fn with_id(f: Field, id: i64) -> Field {
    let mut m = HashMap::new();
    m.insert(FIELD_ID_KEY.to_owned(), id.to_string());
    f.with_metadata(m)
}

/// `decimal(P, S)` or `fixed[N]`'s numbers.
fn args(s: &str, open: char, close: char) -> Option<Vec<i64>> {
    let inner = s.split_once(open)?.1.strip_suffix(close)?;
    inner.split(',').map(|x| x.trim().parse().ok()).collect()
}

fn prim_to_arrow(p: &str, at: &str) -> Result<DataType, String> {
    Ok(match p {
        "boolean" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Microsecond),
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "timestamptz_ns" => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        "string" => DataType::Utf8,
        "uuid" => DataType::FixedSizeBinary(16),
        "binary" => DataType::Binary,
        _ if p.starts_with("fixed[") => match args(p, '[', ']').as_deref() {
            Some([n]) => DataType::FixedSizeBinary(*n as i32),
            _ => return err(format!("schema: {at} has type {p:?}")),
        },
        _ if p.starts_with("decimal(") => match args(p, '(', ')').as_deref() {
            Some([prec, scale]) => DataType::Decimal128(*prec as u8, *scale as i8),
            _ => return err(format!("schema: {at} has type {p:?}")),
        },
        _ => {
            return err(format!(
                "schema: {at} has type {p:?}, which morca-worker doesn't know"
            ))
        }
    })
}

fn type_to_arrow(t: &IType, at: &str) -> Result<DataType, String> {
    Ok(match t {
        IType::Prim(p) => prim_to_arrow(p, at)?,
        IType::Struct(fields) => DataType::Struct(fields_to_arrow(fields, at)?),
        IType::List {
            id,
            element,
            required,
        } => DataType::List(Arc::new(with_id(
            Field::new(
                "element",
                type_to_arrow(element, &format!("{at}.element"))?,
                !required,
            ),
            *id,
        ))),
        IType::Map {
            key_id,
            key,
            value_id,
            value,
            value_required,
        } => {
            let entries = Fields::from(vec![
                with_id(
                    Field::new("key", type_to_arrow(key, &format!("{at}.key"))?, false),
                    *key_id,
                ),
                with_id(
                    Field::new(
                        "value",
                        type_to_arrow(value, &format!("{at}.value"))?,
                        !value_required,
                    ),
                    *value_id,
                ),
            ]);
            DataType::Map(
                Arc::new(Field::new("key_value", DataType::Struct(entries), false)),
                false,
            )
        }
    })
}

fn fields_to_arrow(fields: &[IField], at: &str) -> Result<Fields, String> {
    let mut out = Vec::new();
    for f in fields {
        let path = if at.is_empty() {
            f.name.clone()
        } else {
            format!("{at}.{}", f.name)
        };
        out.push(with_id(
            Field::new(&f.name, type_to_arrow(&f.ty, &path)?, !f.required),
            f.id,
        ));
    }
    Ok(Fields::from(out))
}

/// The Arrow schema of an Iceberg schema JSON file: the types the table
/// stores, each field with its Iceberg id, nullable unless required.
pub fn to_arrow(json: &str) -> Result<Schema, String> {
    Ok(Schema::new(fields_to_arrow(&parse(json)?, "")?))
}

fn prim_fits(p: &str, dt: &DataType, at: &str) -> Result<bool, String> {
    use DataType::*;
    let want = prim_to_arrow(p, at)?;
    Ok(match (&want, dt) {
        (Int32, Int8 | Int16 | Int32) => true,
        (Int64, Int8 | Int16 | Int32 | Int64) => true,
        (Float64, Float32 | Float64) => true,
        (Utf8, Utf8 | LargeUtf8) => true,
        (Binary, Binary | LargeBinary) => true,
        (Decimal128(wp, ws), Decimal128(p2, s2)) => s2 == ws && p2 <= wp,
        (Timestamp(_, wtz), Timestamp(_, tz)) => wtz.is_some() == tz.is_some(),
        (Time64(_), Time64(_)) => true,
        _ => &want == dt,
    })
}

fn describe(dt: &DataType) -> String {
    format!("{dt}")
}

fn fits(t: &IType, dt: &DataType, at: &str) -> Result<(), String> {
    match (t, dt) {
        (IType::Prim(p), _) => {
            if prim_fits(p, dt, at)? {
                Ok(())
            } else {
                err(format!(
                    "column {at} is {} in the worker's schema, which doesn't fit the table's {p}",
                    describe(dt)
                ))
            }
        }
        (IType::Struct(fields), DataType::Struct(got)) => fields_fit(fields, got, at),
        (
            IType::List {
                element, required, ..
            },
            DataType::List(f) | DataType::LargeList(f),
        ) => {
            let path = format!("{at}.element");
            nullable_fits(*required, f.is_nullable(), &path)?;
            fits(element, f.data_type(), &path)
        }
        (
            IType::Map {
                key,
                value,
                value_required,
                ..
            },
            DataType::Map(entries, _),
        ) => {
            let DataType::Struct(kv) = entries.data_type() else {
                return err(format!(
                    "column {at} is a map whose entries aren't a struct"
                ));
            };
            if kv.len() != 2 {
                return err(format!(
                    "column {at} is a map whose entries aren't key and value"
                ));
            }
            let kpath = format!("{at}.key");
            let vpath = format!("{at}.value");
            nullable_fits(true, kv[0].is_nullable(), &kpath)?;
            fits(key, kv[0].data_type(), &kpath)?;
            nullable_fits(*value_required, kv[1].is_nullable(), &vpath)?;
            fits(value, kv[1].data_type(), &vpath)
        }
        (IType::Struct(_), _) => err(format!(
            "column {at} is {} in the worker's schema, not a struct",
            describe(dt)
        )),
        (IType::List { .. }, _) => err(format!(
            "column {at} is {} in the worker's schema, not a list",
            describe(dt)
        )),
        (IType::Map { .. }, _) => err(format!(
            "column {at} is {} in the worker's schema, not a map",
            describe(dt)
        )),
    }
}

fn nullable_fits(required: bool, nullable: bool, at: &str) -> Result<(), String> {
    if required && nullable {
        return err(format!(
            "column {at} is required in the table but nullable in the worker's schema"
        ));
    }
    Ok(())
}

fn fields_fit(want: &[IField], got: &Fields, at: &str) -> Result<(), String> {
    let path = |n: &str| {
        if at.is_empty() {
            n.to_owned()
        } else {
            format!("{at}.{n}")
        }
    };
    for f in want {
        let p = path(&f.name);
        let Some(g) = got.iter().find(|g| g.name() == &f.name) else {
            return err(format!(
                "column {p} is in the table but not in the worker's schema"
            ));
        };
        nullable_fits(f.required, g.is_nullable(), &p)?;
        fits(&f.ty, g.data_type(), &p)?;
    }
    for g in got.iter() {
        if !want.iter().any(|f| &f.name == g.name()) {
            return err(format!(
                "column {} is in the worker's schema but not in the table",
                path(g.name())
            ));
        }
    }
    Ok(())
}

/// Whether `arrow` fits the Iceberg schema JSON `json` (the module's rule);
/// the error names the first column that doesn't.
pub fn check(arrow: &Schema, json: &str) -> Result<(), String> {
    fields_fit(&parse(json)?, arrow.fields(), "")
}

/// [`check`], panicking with its message: for a worker's `cargo test`.
pub fn assert_fits(arrow: &Schema, json: &str) {
    if let Err(e) = check(arrow, json) {
        panic!("the worker's schema doesn't fit the table's: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"{"schema-id":0,"type":"struct","fields":[
        {"id":1,"name":"id","required":true,"type":"long"},
        {"id":2,"name":"name","required":false,"type":"string"},
        {"id":3,"name":"tags","required":false,"type":{"type":"list","element-id":5,"element":"string","element-required":false}},
        {"id":4,"name":"at","required":false,"type":"timestamptz"}]}"#;

    #[test]
    fn to_arrow_fits_itself() {
        let s = to_arrow(SCHEMA).unwrap();
        assert_eq!(s.fields().len(), 4);
        assert_eq!(s.field(0).metadata().get(FIELD_ID_KEY).unwrap(), "1");
        assert!(!s.field(0).is_nullable());
        check(&s, SCHEMA).unwrap();
    }

    #[test]
    fn widening_fits() {
        let s = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::LargeUtf8, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
                true,
            ),
        ]);
        check(&s, SCHEMA).unwrap();
    }

    #[test]
    fn names_the_column() {
        let base = to_arrow(SCHEMA).unwrap();
        let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields[1] = Field::new("name", DataType::Int64, true);
        let e = check(&Schema::new(fields.clone()), SCHEMA).unwrap_err();
        assert!(e.contains("column name is Int64"), "{e}");

        let e = check(&Schema::new(fields[..1].to_vec()), SCHEMA).unwrap_err();
        assert!(e.contains("column name is in the table but not"), "{e}");

        let mut extra = base
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        extra.push(Field::new("more", DataType::Int64, true));
        let e = check(&Schema::new(extra), SCHEMA).unwrap_err();
        assert!(
            e.contains("column more is in the worker's schema but not"),
            "{e}"
        );

        let mut nullable = base
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        nullable[0] = Field::new("id", DataType::Int64, true);
        let e = check(&Schema::new(nullable), SCHEMA).unwrap_err();
        assert!(e.contains("column id is required"), "{e}");

        let mut narrow = base
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        narrow[3] = Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true);
        let e = check(&Schema::new(narrow), SCHEMA).unwrap_err();
        assert!(e.contains("column at is"), "{e}");
    }

    #[test]
    fn nested_types() {
        let json = r#"{"type":"struct","fields":[
            {"id":1,"name":"s","required":false,"type":{"type":"struct","fields":[
                {"id":2,"name":"a","required":true,"type":"int"},
                {"id":3,"name":"d","required":false,"type":"decimal(10, 2)"}]}},
            {"id":4,"name":"m","required":false,"type":{"type":"map","key-id":5,"key":"string","value-id":6,"value":"fixed[4]","value-required":false}}]}"#;
        let s = to_arrow(json).unwrap();
        check(&s, json).unwrap();
        let DataType::Struct(kids) = s.field(0).data_type() else {
            panic!()
        };
        assert_eq!(kids[1].data_type(), &DataType::Decimal128(10, 2));
    }
}
