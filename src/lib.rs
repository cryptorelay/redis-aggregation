#[macro_use]
extern crate redis_module;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_tuple;

use std::collections::HashMap;
use std::convert::TryInto;
use std::mem;
use std::num::{ParseIntError, TryFromIntError};
use std::time::Duration;
use std::vec::Vec;

use serde::de::Error;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json;

use redis_module::{
    parse_float, parse_integer, raw, Context, RedisError, RedisResult, RedisValue, REDIS_OK,
};

use libc::c_int;
use redis_module::native_types::RedisType;
use std::os::raw::c_void;

type Time = f64;
type Value = f64;
const TIMER_INTERVAL: u64 = 1000;

#[derive(Serialize, Deserialize)]
enum TimeFunc {
    Interval(u32),
}
impl TimeFunc {
    fn apply(&self, time: Time) -> Time {
        match self {
            TimeFunc::Interval(n) => (time / *n as f64).floor() * (*n as f64),
        }
    }
}

/// ```
/// assert_eq!(StreamID{ms: 10, seq: 0} > StreamID{ms: 0, seq: 10});
/// assert_eq!(StreamID{ms: 10, seq: 10} > StreamID{ms: 10, seq: 9});
/// let id = StreamID{ms: 10, seq: 10}
/// assert_eq!(id.increment(10))
/// assert_eq!(id.seq, 11)
/// ```
#[derive(Serialize_tuple, Deserialize_tuple, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct StreamID {
    ms: u64,
    seq: u64,
}

impl StreamID {
    fn new() -> StreamID {
        StreamID { ms: 0, seq: 0 }
    }

    fn increment(&mut self, ms_: u64) -> bool {
        if ms_ < self.ms {
            false
        } else if ms_ == self.ms {
            self.seq += 1;
            true
        } else {
            self.ms = ms_;
            self.seq = 0;
            true
        }
    }
}

impl Into<String> for StreamID {
    fn into(self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

trait AggOp {
    fn save(&self) -> (&str, String);
    fn load(&mut self, buf: &str);
    fn update(&mut self, value: Value);
    fn reset(&mut self);
    fn current(&self) -> Option<Value>;
}

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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
            None => self.0 = Some(value),
            Some(v) if v > value => self.0 = Some(value),
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

#[derive(Default)]
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
            None => self.0 = Some(value),
            Some(v) if v < value => self.0 = Some(value),
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

#[derive(Default)]
struct AggAvg {
    count: usize,
    sum: Value,
}
impl AggOp for AggAvg {
    fn save(&self) -> (&str, String) {
        (
            "avg",
            serde_json::to_string(&(self.count, self.sum)).unwrap(),
        )
    }
    fn load(&mut self, buf: &str) {
        let t = serde_json::from_str::<(usize, Value)>(buf).unwrap();
        self.count = t.0;
        self.sum = t.1;
    }
    fn update(&mut self, value: Value) {
        self.sum += value;
        self.count += 1;
    }
    fn reset(&mut self) {
        self.count = 0;
        self.sum = 0.;
    }
    fn current(&self) -> Option<Value> {
        if self.count == 0 {
            return None;
        } else {
            return Some(self.sum / self.count as f64);
        }
    }
}

#[derive(Default)]
struct AggSum(Value);
impl AggOp for AggSum {
    fn save(&self) -> (&str, String) {
        ("sum", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str(buf).unwrap();
    }
    fn update(&mut self, value: Value) {
        self.0 += value;
    }
    fn reset(&mut self) {
        self.0 = 0.;
    }
    fn current(&self) -> Option<Value> {
        return Some(self.0);
    }
}

#[derive(Default)]
struct AggCount(usize);
impl AggOp for AggCount {
    fn save(&self) -> (&str, String) {
        ("count", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str(buf).unwrap();
    }
    fn update(&mut self, _value: Value) {
        self.0 += 1;
    }
    fn reset(&mut self) {
        self.0 = 0;
    }
    fn current(&self) -> Option<Value> {
        return Some(self.0 as Value);
    }
}

#[derive(Default)]
struct AggStd {
    sum: Value,
    sum_2: Value,
    count: usize,
}

impl AggStd {
    fn to_string(&self) -> String {
        serde_json::to_string(&(self.sum, self.sum_2, self.count)).unwrap()
    }
    fn from_str(buf: &str) -> AggStd {
        let t = serde_json::from_str::<(Value, Value, usize)>(buf).unwrap();
        return Self {
            sum: t.0,
            sum_2: t.1,
            count: t.2,
        };
    }
    fn add(&mut self, value: Value) {
        self.sum += value;
        self.sum_2 += value * value;
        self.count += 1;
    }
    fn reset(&mut self) {
        self.sum = 0.;
        self.sum_2 = 0.;
        self.count = 0;
    }
    fn variance(&self) -> Value {
        // ported from: https://github.com/RedisTimeSeries/RedisTimeSeries/blob/7911f43e2861472565b2aa61d8e91a9c37ec6cae/src/compaction.c
        //  var(X) = sum((x_i - E[X])^2)
        //  = sum(x_i^2) - 2 * sum(x_i) * E[X] + E^2[X]
        if self.count <= 1 {
            0.
        } else {
            let avg = self.sum / self.count as Value;
            self.sum_2 - 2. * self.sum * avg + avg * avg * self.count as Value
        }
    }
}

#[derive(Default)]
struct AggVarP(AggStd);
impl AggOp for AggVarP {
    fn save(&self) -> (&str, String) {
        ("varp", self.0.to_string())
    }
    fn load(&mut self, buf: &str) {
        self.0 = AggStd::from_str(buf);
    }
    fn update(&mut self, value: Value) {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else {
            Some(self.0.variance() / self.0.count as Value)
        }
    }
}

#[derive(Default)]
struct AggVarS(AggStd);
impl AggOp for AggVarS {
    fn save(&self) -> (&str, String) {
        ("vars", self.0.to_string())
    }
    fn load(&mut self, buf: &str) {
        self.0 = AggStd::from_str(buf);
    }
    fn update(&mut self, value: Value) {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else if self.0.count == 1 {
            Some(0.)
        } else {
            Some(self.0.variance() / (self.0.count - 1) as Value)
        }
    }
}

#[derive(Default)]
struct AggStdP(AggStd);
impl AggOp for AggStdP {
    fn save(&self) -> (&str, String) {
        ("stdp", self.0.to_string())
    }
    fn load(&mut self, buf: &str) {
        self.0 = AggStd::from_str(buf);
    }
    fn update(&mut self, value: Value) {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else {
            Some((self.0.variance() / self.0.count as Value).sqrt())
        }
    }
}

#[derive(Default)]
struct AggStdS(AggStd);
impl AggOp for AggStdS {
    fn save(&self) -> (&str, String) {
        ("stds", self.0.to_string())
    }
    fn load(&mut self, buf: &str) {
        self.0 = AggStd::from_str(buf);
    }
    fn update(&mut self, value: Value) {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else if self.0.count == 1 {
            Some(0.)
        } else {
            Some((self.0.variance() / (self.0.count - 1) as Value).sqrt())
        }
    }
}

fn parse_agg_type(name: &str) -> Option<Box<dyn AggOp>> {
    match name {
        "first" => Some(Box::new(AggFirst::default())),
        "last" => Some(Box::new(AggLast::default())),
        "min" => Some(Box::new(AggMin::default())),
        "max" => Some(Box::new(AggMax::default())),
        "avg" => Some(Box::new(AggAvg::default())),
        "sum" => Some(Box::new(AggSum::default())),
        "count" => Some(Box::new(AggCount::default())),
        "stds" => Some(Box::new(AggStdS::default())),
        "stdp" => Some(Box::new(AggStdP::default())),
        "vars" => Some(Box::new(AggVarS::default())),
        "varp" => Some(Box::new(AggVarP::default())),
        _ => None,
    }
}

impl Serialize for Box<dyn AggOp> {
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

impl<'de> Deserialize<'de> for Box<dyn AggOp> {
    fn deserialize<D>(deserializer: D) -> Result<Box<dyn AggOp>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (name, value) = Deserialize::deserialize(deserializer)?;
        let mut agg = parse_agg_type(name).ok_or(Error::custom("invalid agg type"))?;
        agg.load(value);
        Ok(agg)
    }
}

#[derive(Serialize, Deserialize)]
struct GroupState {
    current: Time,
    func: TimeFunc,
}

#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct AggField {
    index: usize,
    op: Box<dyn AggOp>,
}

#[derive(Serialize, Deserialize)]
pub struct AggView {
    name: String,
    fields: Vec<AggField>,
    groupby: Option<GroupState>,
}

impl AggView {
    pub fn update(&mut self, ctx: &Context, values: &[Value]) -> Result<(), RedisError> {
        match self.groupby {
            None => {}
            Some(ref groupby) => {
                let grouptime = groupby.func.apply(values[0]);
                if grouptime > groupby.current {
                    // save current and reset
                    if groupby.current > 0. {
                        self.save(ctx)?;
                    }
                    for agg in &mut self.fields {
                        agg.op.reset()
                    }
                    self.groupby.as_mut().unwrap().current = grouptime;
                } else if grouptime < groupby.current {
                    // ignore the item
                    return Ok(());
                }
            }
        }
        for agg in &mut self.fields {
            agg.op.update(values[agg.index])
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<String, RedisError> {
        let mut values = Vec::new();
        values.reserve_exact(self.fields.len());
        for agg in &self.fields {
            values.push(agg.op.current());
        }
        serde_json::to_string(&values)
            .map_err(|err| RedisError::String(format!("encode failed: {}", err)))
    }

    pub fn save(&self, ctx: &Context) -> Result<(), RedisError> {
        match self.groupby {
            None => {
                ctx.call("set", &[&self.name, &self.encode()?])?;
            }
            Some(ref groupby) => {
                if groupby.current > 0. {
                    ctx.call(
                        "hset",
                        &[&self.name, &groupby.current.to_string(), &self.encode()?],
                    )?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct AggTable {
    fields: Vec<String>,
    #[serde(skip)]
    fields_by_name: HashMap<String, usize>,
    views: Vec<AggView>,
    last_id: StreamID,

    #[serde(skip)]
    timer: u64,
}

impl AggTable {
    pub fn new(fields: Vec<String>) -> AggTable {
        let mut fields_by_name = HashMap::new();
        for (i, field) in fields.iter().enumerate() {
            fields_by_name.insert(field.clone(), i);
        }

        return AggTable {
            fields,
            fields_by_name,
            views: Vec::new(),
            last_id: StreamID::new(),

            timer: 0,
        };
    }

    pub fn parse_agg_field(&self, func: &String, field: &String) -> Result<AggField, RedisError> {
        let op = parse_agg_type(func).ok_or(RedisError::Str("invalid aggregate operation"))?;
        let index = self
            .fields_by_name
            .get(field)
            .ok_or(RedisError::Str("invalid field name"))?;
        return Ok(AggField { index: *index, op });
    }

    pub fn parse_view(
        &self,
        name: String,
        interval: Option<u32>,
        args: &[String],
    ) -> Result<AggView, RedisError> {
        let mut fields = Vec::new();
        for chunk in args.chunks_exact(2) {
            fields.push(self.parse_agg_field(&chunk[0], &chunk[1])?);
        }
        return Ok(AggView {
            name,
            fields,
            groupby: interval.map(|i| GroupState {
                current: 0.,
                func: TimeFunc::Interval(i),
            }),
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
            let i = i
                .try_into()
                .map_err(|err: TryFromIntError| RedisError::String(err.to_string()))?;
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

    pub fn update(&mut self, ctx: &Context, time: &str, args: &[String]) -> RedisResult {
        if args.len() + 1 != self.fields.len() {
            return Err(RedisError::WrongArity);
        }
        let parse_err = |e: ParseIntError| RedisError::String(e.to_string());
        let (ms, seq) = match time.find('-') {
            None => (time.parse::<u64>().map_err(parse_err)?, None),
            Some(i) => (
                time[..i].parse::<u64>().map_err(parse_err)?,
                Some(time[i + 1..].parse::<u64>().map_err(parse_err)?),
            ),
        };
        let id = match seq {
            None => {
                if !self.last_id.increment(ms) {
                    return Err(RedisError::Str("input time is smaller"));
                }
                self.last_id.clone()
            }
            Some(seq) => {
                let id = StreamID { ms, seq };
                if id <= self.last_id {
                    return Err(RedisError::Str("input time is smaller"));
                }
                self.last_id = id.clone();
                id
            }
        };
        let mut args = args
            .iter()
            .map(|s| parse_float(&s))
            .collect::<Result<Vec<_>, _>>()?;
        args.insert(0, id.ms as Value / 1000.);
        for view in &mut self.views {
            view.update(ctx, &args)?;
        }
        Ok(RedisValue::SimpleString(id.into()))
    }

    pub fn save(&self, ctx: &Context) -> Result<(), RedisError> {
        for view in &self.views {
            view.save(ctx)?;
        }
        Ok(())
    }
}

//////////////////////////////////////////////////////

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
    let table: &AggTable = mem::transmute(value);
    raw::save_string(rdb, &serde_json::to_string(table).unwrap());
}

pub(crate) static AGG_REDIS_TYPE: RedisType = RedisType::new(
    "aggre-hy1",
    1,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,

        rdb_load: Some(agg_rdb_load),
        rdb_save: Some(agg_rdb_save),
        aof_rewrite: None,
        free: Some(agg_free),

        aux_load: None,
        aux_save: None,
        aux_save_triggers: 0,

        // Currently unused by Redis
        mem_usage: None,
        digest: None,
    },
);

fn timer_callback(ctx: &Context, name: String) -> () {
    ctx.auto_memory();
    let key = ctx.open_key_writable(&name);
    match key.get_value::<AggTable>(&AGG_REDIS_TYPE).unwrap() {
        Some(table) => {
            table.timer =
                ctx.create_timer(Duration::from_secs(TIMER_INTERVAL), timer_callback, name);
            match table.save(&ctx) {
                Ok(_) => {}
                Err(err) => {
                    println!("save failed: {:?}", err);
                }
            }
        }
        None => {
            println!("timer key not found");
        }
    };
}

fn new_table(ctx: &Context, args: Vec<String>) -> RedisResult {
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
            key.set_value(&AGG_REDIS_TYPE, AggTable::new(args[2..].to_vec()))?;
            let table = key
                .get_value::<AggTable>(&AGG_REDIS_TYPE)?
                .ok_or(RedisError::Str("impossible"))?;
            table.timer = ctx.create_timer(
                Duration::from_secs(TIMER_INTERVAL),
                timer_callback,
                args[1].clone(),
            );
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
        None => Err(RedisError::Str("key not exist")),
        Some(v) => {
            let result = v.add_view(&args[2..])?;
            ctx.replicate_verbatim();
            Ok(result)
        }
    }
}

fn insert_data(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() <= 3 {
        return Err(RedisError::WrongArity);
    }
    ctx.auto_memory();
    let key = ctx.open_key_writable(&args[1]);
    match key.get_value::<AggTable>(&AGG_REDIS_TYPE)? {
        None => Err(RedisError::Str("key not exist")),
        Some(v) => {
            let result = v.update(ctx, &args[2], &args[3..])?;
            ctx.replicate_verbatim();
            Ok(result)
        }
    }
}

fn dump_table(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() <= 1 {
        return Err(RedisError::WrongArity);
    }
    ctx.auto_memory();
    let key = ctx.open_key(&args[1]);
    match key.get_value::<AggTable>(&AGG_REDIS_TYPE)? {
        None => Err(RedisError::Str("key not exist")),
        Some(v) => {
            let s = serde_json::to_string(v).map_err(|err| RedisError::String(err.to_string()))?;
            Ok(RedisValue::SimpleString(s))
        }
    }
}

fn save_table(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() != 2 {
        return Err(RedisError::WrongArity);
    }
    ctx.auto_memory();

    let key = ctx.open_key(&args[1]);
    match key.get_value::<AggTable>(&AGG_REDIS_TYPE)? {
        None => Err(RedisError::Str("key not exist")),
        Some(v) => {
            v.save(ctx)?;
            REDIS_OK
        }
    }
}

fn get_last_id(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() != 2 {
        return Err(RedisError::WrongArity);
    }

    let key = ctx.open_key(&args[1]);
    match key.get_value::<AggTable>(&AGG_REDIS_TYPE)? {
        None => Err(RedisError::Str("key not exist")),
        Some(v) => Ok(RedisValue::SimpleString(v.last_id.clone().into())),
    }
}

fn get_current_value(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() != 2 {
        return Err(RedisError::WrongArity);
    }

    let key = ctx.open_key(&args[1]);
    match key.get_value::<AggTable>(&AGG_REDIS_TYPE)? {
        None => Err(RedisError::Str("key not exist")),
        Some(v) => {
            let mut result = Vec::new();
            for view in &v.views {
                let mut items = Vec::new();
                for field in &view.fields {
                    items.push(
                        field
                            .op
                            .current()
                            .map_or(RedisValue::Null, |v| RedisValue::Float(v)),
                    )
                }
                result.push(RedisValue::SimpleString(view.name.clone()));
                result.push(RedisValue::Array(items));
                result.push(
                    view.groupby
                        .as_ref()
                        .map_or(RedisValue::Null, |g| RedisValue::Float(g.current)),
                );
            }
            Ok(RedisValue::Array(result))
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
        ["agg.new", new_table, "write", 1, 1, 1],
        ["agg.view", add_view, "write", 1, 1, 1],
        ["agg.insert", insert_data, "write", 1, 1, 1],
        ["agg.save", save_table, "write", 1, 1, 1],
        ["agg.dump", dump_table, "readonly", 1, 1, 1],
        ["agg.last_id", get_last_id, "readonly", 1, 1, 1],
        ["agg.current", get_current_value, "readonly", 1, 1, 1],
    ],
}
