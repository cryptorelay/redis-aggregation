#[macro_use]
extern crate redismodule;

use std::collections::HashMap;
use std::vec::Vec;
use std::convert::TryInto;
use std::num::TryFromIntError;

use redismodule::native_types::RedisType;
use redismodule::{
    Context, RedisResult, RedisError, REDIS_OK,
    parse_integer, parse_float};

type Time = f64;
type Value = f64;

enum TimeFunc {
    Interval(u32)
}
impl TimeFunc {
    fn apply(&self, time: Time) -> Time {
        match self {
            TimeFunc::Interval(n) => (time / *n as f64).floor() * (*n as f64)
        }
    }
}

trait AggOp {
    fn update(&mut self, value: Value);
    fn reset(&mut self);
    fn current(&self) -> Option<Value>;
}

struct AggFirst(Option<Value>);
impl AggOp for AggFirst {
    fn update(&mut self, value: Value) {
        if let None = self.0 {
            self.0 = Some(value)
        }
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        return self.0;
    }
}

struct AggLast(Option<Value>);
impl AggOp for AggLast {
    fn update(&mut self, value: Value) {
        self.0 = Some(value)
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        return self.0;
    }
}

struct AggMin(Option<Value>);
impl AggOp for AggMin {
    fn update(&mut self, value: Value) {
        match self.0 {
            None => {
                self.0 = Some(value)
            }
            Some(v) if v > value => {
                self.0 = Some(value)
            }
            _ => {}
        }
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        return self.0;
    }
}

struct AggMax(Option<Value>);
impl AggOp for AggMax {
    fn update(&mut self, value: Value) {
        match self.0 {
            None => {
                self.0 = Some(value)
            }
            Some(v) if v < value => {
                self.0 = Some(value)
            }
            _ => {}
        }
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        return self.0
    }
}

struct AggAvg {
    count: usize,
    sum: Value
}
impl AggOp for AggAvg {
    fn update(&mut self, value: Value) {
        self.sum += value
    }
    fn reset(&mut self) {
        self.count = 0;
        self.sum = 0.;
    }
    fn current(&self) -> Option<Value> {
        if self.count == 0 {
            return None
        } else {
            return Some(self.sum / self.count as f64)
        }
    }
}

struct GroupState {
    current: Time,
    func: TimeFunc
}

fn parse_agg_type(name: &String) -> Option<Box<AggOp>>
{
    if name == "first" {
        return Some(Box::new(AggFirst(None)));
    } else if name == "last" {
        return Some(Box::new(AggLast(None)));
    } else if name == "min" {
        return Some(Box::new(AggMin(None)));
    } else if name == "max" {
        return Some(Box::new(AggMax(None)));
    } else if name == "avg" {
        return Some(Box::new(AggAvg{count: 0, sum: 0.}));
    } else {
        return None;
    }
}

pub struct AggField {
    index: usize,
    op: Box<AggOp>
}

pub struct AggView {
    name: String,
    fields: Vec<AggField>,
    groupby: Option<GroupState>
}

pub struct AggTable {
    fields: Vec<String>,
    fields_by_name: HashMap<String, usize>,
    views: Vec<AggView>
}

impl AggView {
    pub fn update(&mut self, ctx: &Context, time: Time, values: &[Value]) {
        if self.groupby.is_some() {
            let groupby = self.groupby.as_ref().unwrap();
            let grouptime = groupby.func.apply(time);
            if grouptime > groupby.current {
                // save current and reset
                self.save(ctx).unwrap();
                for agg in &mut self.fields {
                    agg.op.reset()
                }
                self.groupby.as_mut().unwrap().current = grouptime;
            } else if grouptime < groupby.current {
                // ignore the item
                return
            }
        }
        for agg in &mut self.fields {
            agg.op.update(values[agg.index])
        }
    }

    pub fn save(&self, ctx: &Context) -> RedisResult {
        // save current values into sortedset
        ctx.call("set", &[&self.name, "1"])
    }
}

impl AggTable {
    pub fn new(fields: Vec<String>) -> AggTable {
        let mut fields_by_name = HashMap::new();
        for (i, field) in fields.iter().enumerate() {
            fields_by_name.insert(field.clone(), i);
        }

        return AggTable {
            fields: fields,
            fields_by_name: fields_by_name,
            views: Vec::new()
        }
    }

    pub fn parse_agg_field(&self, func: &String, field: &String) -> Result<AggField, RedisError> {
        let op = parse_agg_type(func).ok_or(RedisError::Str("invalid aggregate operation"))?;
        let index = self.fields_by_name.get(field).ok_or(RedisError::Str("invalid field name"))?;
        return Ok(AggField {
            index: *index,
            op: op
        });
    }

    pub fn parse_view(&self, name: String, interval: Option<u32>, args: &[String]) -> Result<AggView, RedisError> {
        let mut fields = Vec::new();
        for chunk in args.chunks_exact(2) {
            fields.push(self.parse_agg_field(&chunk[0], &chunk[1])?);
        }
        return Ok(AggView {
            name: name,
            fields: fields,
            groupby: interval.map(|i| GroupState {
                current: 0.,
                func: TimeFunc::Interval(i)
            })
        });
    }

    pub fn add_view(&mut self, args: &[String]) -> RedisResult {
        if args.len() <= 1 {
            return Err(RedisError::WrongArity);
        }
        let name = args[0].clone();
        let (interval, args) = if args[1].to_lowercase() == "interval" {
            if args.len() <= 3 {
                return Err(RedisError::WrongArity);
            }
            let i = parse_integer(&args[2])?;
            let i = i.try_into().map_err(|err: TryFromIntError| RedisError::String(err.to_string()))?;
            if i > 3600 * 24 * 365 * 10 {
                return Err(RedisError::Str("Invalid time interval"));
            }
            (Some(i), &args[3..])
        } else {
            (None, &args[1..])
        };

        self.views.push(self.parse_view(name, interval, args)?);
        REDIS_OK
    }

    pub fn update(&mut self, ctx: &Context, args: &[String]) -> RedisResult {
        if args.len() != self.fields.len() {
            return Err(RedisError::WrongArity);
        }
        let args = args.into_iter().map(parse_float).collect::<Result<Vec<f64>, RedisError>>()?;
        for view in &mut self.views {
            view.update(ctx, args[0], &args);
        }
        REDIS_OK
    }
}

//////////////////////////////////////////////////////
static AGG_REDIS_TYPE: RedisType = RedisType::new("aggre-hy1");

fn new_aggregates(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() <= 2 {
        return Err(RedisError::WrongArity);
    }
    ctx.auto_memory();
    let key = ctx.open_key_writable(&args[1]);
    match key.get_value::<AggTable>(&AGG_REDIS_TYPE)? {
        Some(_) => {
            return Err(RedisError::Str("key already exist"));
        }
        None => {
            key.set_value(
                &AGG_REDIS_TYPE,
                AggTable::new(args[2..].to_vec())
            )?;
        }
    }
    ctx.replicate_verbatim();
    REDIS_OK
}

fn add_view(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() <= 2 {
        return Err(RedisError::WrongArity);
    }
    ctx.auto_memory();
    let key = ctx.open_key_writable(&args[1]);
    match key.get_value::<AggTable>(&AGG_REDIS_TYPE)? {
        None => {
            Err(RedisError::Str("key not exist"))
        }
        Some(v) => {
            let result = v.add_view(&args[2..])?;
            ctx.replicate_verbatim();
            Ok(result)
        }
    }
}

fn insert_data(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() <= 2 {
        return Err(RedisError::WrongArity);
    }
    ctx.auto_memory();
    let key = ctx.open_key_writable(&args[1]);
    match key.get_value::<AggTable>(&AGG_REDIS_TYPE)? {
        None => {
            Err(RedisError::Str("key not exist"))
        }
        Some(v) => {
            let result = v.update(ctx, &args[2..])?;
            ctx.replicate_verbatim();
            Ok(result)
        }
    }

}

redis_module! {
    name: "aggregate",
    version: 1,
    data_types: [
        AGG_REDIS_TYPE,
    ],
    commands: [
        ["agg.new", new_aggregates, "write"],
        ["agg.view", add_view, "write"],
        ["agg.insert", insert_data, "write"],
    ],
}
