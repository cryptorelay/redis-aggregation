#[macro_use]
extern crate redismodule;

#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;
use std::vec::Vec;
use std::convert::TryInto;
use std::num::TryFromIntError;
use serde_json;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::ser::SerializeSeq;
use serde::de::Error;

use redismodule::{
    Context, RedisResult, RedisError, RedisValue, REDIS_OK,
    parse_integer, parse_float};

type Time = f64;
type Value = f64;

#[derive(Serialize, Deserialize)]
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
    fn save(&self) -> (&str, String);
    fn load(&mut self, buf: &str);
    fn update(&mut self, value: Value);
    fn reset(&mut self);
    fn current(&self) -> Option<Value>;
}

struct AggFirst(Option<Value>);
impl AggOp for AggFirst {
    fn save(&self) -> (&str, String) {
        ("first", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str::<Option<Value>>(buf).unwrap();
    }
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
    fn save(&self) -> (&str, String) {
        ("last", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str::<Option<Value>>(buf).unwrap();
    }
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
    fn save(&self) -> (&str, String) {
        ("min", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str::<Option<Value>>(buf).unwrap();
    }
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
    fn save(&self) -> (&str, String) {
        ("max", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str::<Option<Value>>(buf).unwrap();
    }
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
    fn save(&self) -> (&str, String) {
        ("avg", serde_json::to_string(&(self.count, self.sum)).unwrap())
    }
    fn load(&mut self, buf: &str) {
        let t = serde_json::from_str::<(usize, Value)>(buf).unwrap();
        self.count = t.0;
        self.sum =t.1;
    }
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

impl Serialize for Box<AggOp> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (name, value) = self.save();
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(name)?;
        seq.serialize_element(&value)?;
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Box<AggOp> {
    fn deserialize<D>(deserializer: D) -> Result<Box<AggOp>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (name, value) = Deserialize::deserialize(deserializer)?;
        let mut agg = parse_agg_type(&name).ok_or(Error::custom("invalid agg type"))?;
        agg.load(value);
        Ok(agg)
    }
}

#[derive(Serialize, Deserialize)]
struct GroupState {
    current: Time,
    func: TimeFunc
}

#[derive(Serialize, Deserialize)]
pub struct AggField {
    index: usize,
    op: Box<AggOp>
}

#[derive(Serialize, Deserialize)]
pub struct AggView {
    name: String,
    fields: Vec<AggField>,
    groupby: Option<GroupState>
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

    pub fn encode(&self) -> Result<String, RedisError> {
        let mut values = Vec::new();
        values.reserve_exact(self.fields.len());
        for agg in &self.fields {
            values.push(agg.op.current());
        }
        serde_json::to_string(&values).map_err(|err| RedisError::String(format!("encode failed: {}", err)))
    }

    pub fn save(&self, ctx: &Context) -> RedisResult {
        let encoded = self.encode()?;
        match self.groupby {
            None => {
                ctx.call("set", &[&self.name, &encoded])
            }
            Some(ref groupby) => {
                ctx.call("hset", &[&self.name, &groupby.current.to_string(), &encoded])
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct AggTable {
    fields: Vec<String>,
    #[serde(skip)]
    fields_by_name: HashMap<String, usize>,
    views: Vec<AggView>
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
mod agg {
    use redismodule::native_types::RedisType;
    use redismodule::raw;
    use std::os::raw::{c_int, c_void};
    use crate::{AggTable};

    pub(crate) static REDIS_TYPE: RedisType = RedisType::new("aggre-hy1");

    #[allow(non_snake_case, unused)]
    unsafe extern "C" fn agg_rdb_load(rdb: *mut raw::RedisModuleIO, encver: c_int) -> *mut c_void {
        let table = serde_json::from_str::<AggTable>(&raw::load_string(rdb)).unwrap();
        let table = Box::new(table);
        Box::into_raw(table) as *mut c_void
    }

    #[allow(non_snake_case, unused)]
    #[no_mangle]
    unsafe extern "C" fn agg_free(value: *mut c_void) {
        Box::from_raw(value as *mut AggTable);
    }

    #[allow(non_snake_case, unused)]
    #[no_mangle]
    unsafe extern "C" fn agg_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
        let table = &*(value as *mut AggTable);
        raw::save_string(rdb, &serde_json::to_string(table).unwrap());
    }

    pub(crate) static mut TYPE_METHODS: raw::RedisModuleTypeMethods = raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,

        rdb_load: Some(agg_rdb_load),
        rdb_save: Some(agg_rdb_save),
        aof_rewrite: None,
        free: Some(agg_free),

        // Currently unused by Redis
        mem_usage: None,
        digest: None,
    };
}

fn new_table(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() <= 2 {
        return Err(RedisError::WrongArity);
    }
    ctx.auto_memory();
    let key = ctx.open_key_writable(&args[1]);
    match key.get_value::<AggTable>(&agg::REDIS_TYPE)? {
        Some(_) => {
            return Err(RedisError::Str("key already exist"));
        }
        None => {
            key.set_value(
                &agg::REDIS_TYPE,
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
    match key.get_value::<AggTable>(&agg::REDIS_TYPE)? {
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
    match key.get_value::<AggTable>(&agg::REDIS_TYPE)? {
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

fn dump_table(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() <= 2 {
        return Err(RedisError::WrongArity);
    }
    ctx.auto_memory();
    let key = ctx.open_key(&args[1]);
    match key.get_value::<AggTable>(&agg::REDIS_TYPE)? {
        None => {
            Err(RedisError::Str("key not exist"))
        }
        Some(v) => {
            let s = serde_json::to_string(v).unwrap();
            Ok(RedisValue::SimpleString(s))
        }
    }
}

redis_module! {
    name: "aggregate",
    version: 1,
    data_types: [
        agg,
    ],
    commands: [
        ["agg.new", new_table, "write"],
        ["agg.view", add_view, "write"],
        ["agg.insert", insert_data, "write"],
        ["agg.dump", dump_table, ""],
    ],
}
