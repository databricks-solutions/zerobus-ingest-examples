//! SDK matrix client, Rust (databricks-zerobus-ingest-sdk, built from the monorepo's rust/sdk).
//! Usage: client <format> <scenario> --table T --endpoint E --uc U --client-id ID --client-secret SEC
//! Prints one line at the end: ZB_RESULT {"ok": bool, "error": str, "acked": int}.

use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use databricks_zerobus_ingest_sdk::{AvroBytes, JsonString, ZerobusSdk};
use prost_types::{field_descriptor_proto::{Label, Type}, DescriptorProto, FieldDescriptorProto};

const N: usize = 20;
const MISMATCH_AT: usize = 5;

fn opt(args: &[String], key: &str) -> String {
    let flag = format!("--{key}");
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone()).unwrap_or_default()
}

fn field(name: &str, number: i32, ty: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    }
}

fn proto_descriptor(mismatch: bool) -> DescriptorProto {
    let mut fields = vec![
        field("device_name", 1, Type::String),
        field("temp", 2, Type::Int32),
        field("humidity", 3, Type::Int64),
    ];
    if mismatch {
        fields.push(field("region", 4, Type::String));
    }
    DescriptorProto { name: Some("SdkMatrix".into()), field: fields, ..Default::default() }
}

fn varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn zigzag(n: i64, out: &mut Vec<u8>) {
    varint(((n << 1) ^ (n >> 63)) as u64, out);
}

/// Protobuf encoding of SdkMatrix(device_name, temp, humidity).
fn proto_record(i: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let name = format!("m-{i}");
    out.push((1 << 3) | 2);
    varint(name.len() as u64, &mut out);
    out.extend_from_slice(name.as_bytes());
    out.push((2 << 3) | 0);
    varint(i as u64, &mut out);
    out.push((3 << 3) | 0);
    varint(2, &mut out);
    out
}

/// Avro datum for the writer schema with [null, int] temp and [null, long] humidity.
fn avro_record(i: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let name = format!("m-{i}");
    zigzag(name.len() as i64, &mut out);
    out.extend_from_slice(name.as_bytes());
    zigzag(1, &mut out); // union branch 1 = the non-null type
    zigzag(i as i64, &mut out);
    zigzag(1, &mut out);
    zigzag(2, &mut out);
    out
}

fn avro_schema(mismatch: bool) -> String {
    let mut fields = String::from(
        r#"{"name":"device_name","type":"string"},{"name":"temp","type":["null","int"],"default":null},{"name":"humidity","type":["null","long"],"default":null}"#,
    );
    if mismatch {
        fields.push_str(r#",{"name":"region","type":"string"}"#);
    }
    format!(r#"{{"type":"record","name":"SdkMatrix","fields":[{fields}]}}"#)
}

fn arrow_schema(mismatch: bool) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("device_name", DataType::Utf8, false),
        Field::new("temp", DataType::Int32, true),
        Field::new("humidity", DataType::Int64, true),
    ];
    if mismatch {
        fields.push(Field::new("region", DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

fn arrow_batch(start: usize, rows: usize) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let schema = arrow_schema(false);
    let names: Vec<String> = (0..rows).map(|j| format!("m-{}", start + j)).collect();
    let temps: Vec<i32> = (0..rows).map(|j| (start + j) as i32).collect();
    let hums: Vec<i64> = vec![2; rows];
    RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(names)),
        Arc::new(Int32Array::from(temps)),
        Arc::new(Int64Array::from(hums)),
    ])
}

async fn run(format: &str, mismatch: bool, args: &[String]) -> Result<usize, String> {
    let endpoint = opt(args, "endpoint");
    let uc = opt(args, "uc");
    let table = opt(args, "table");
    let id = opt(args, "client-id");
    let secret = opt(args, "client-secret");
    let sdk = ZerobusSdk::builder()
        .endpoint(endpoint)
        .unity_catalog_url(uc)
        .build()
        .map_err(|e| e.to_string())?;
    let mut acked = 0usize;
    match format {
        "json" => {
            let mut stream = sdk.stream_builder().table(table).oauth(id, secret).json()
                .build().await.map_err(|e| e.to_string())?;
            for i in 0..N {
                let extra = if mismatch && i == MISMATCH_AT { r#","region":"eu""# } else { "" };
                let rec = format!(r#"{{"device_name":"m-{i}","temp":{i},"humidity":2{extra}}}"#);
                if let Err(e) = stream.ingest_record_offset(JsonString(rec)).await {
                    let _ = stream.flush().await;
                    return Err(e.to_string());
                }
                acked += 1;
            }
            stream.flush().await.map_err(|e| e.to_string())?;
            stream.close().await.map_err(|e| e.to_string())?;
        }
        "proto" => {
            let mut stream = sdk.stream_builder().table(table).oauth(id, secret)
                .compiled_proto(proto_descriptor(mismatch)).build().await.map_err(|e| e.to_string())?;
            for i in 0..N {
                if let Err(e) = stream.ingest_record_offset(proto_record(i)).await {
                    let _ = stream.flush().await;
                    return Err(e.to_string());
                }
                acked += 1;
            }
            stream.flush().await.map_err(|e| e.to_string())?;
            stream.close().await.map_err(|e| e.to_string())?;
        }
        "avro" => {
            let mut stream = sdk.stream_builder().table(table).oauth(id, secret).avro(avro_schema(mismatch))
                .build().await.map_err(|e| e.to_string())?;
            for i in 0..N {
                if let Err(e) = stream.ingest_record_offset(AvroBytes(avro_record(i))).await {
                    let _ = stream.flush().await;
                    return Err(e.to_string());
                }
                acked += 1;
            }
            stream.flush().await.map_err(|e| e.to_string())?;
            stream.close().await.map_err(|e| e.to_string())?;
        }
        "arrow" => {
            let mut stream = sdk.stream_builder().table(table).oauth(id, secret).arrow(arrow_schema(mismatch))
                .build_arrow().await.map_err(|e| e.to_string())?;
            for start in (0..N).step_by(4) {
                let batch = arrow_batch(start, 4).map_err(|e| e.to_string())?;
                stream.ingest_batch(batch).await.map_err(|e| e.to_string())?;
                acked += 4;
            }
            stream.flush().await.map_err(|e| e.to_string())?;
            stream.close().await.map_err(|e| e.to_string())?;
        }
        other => return Err(format!("unknown format {other}")),
    }
    Ok(acked)
}

fn quote(s: &str) -> String {
    let escaped: String = s.chars().map(|c| match c {
        '"' => "\\\"".to_string(),
        '\\' => "\\\\".to_string(),
        '\n' => " ".to_string(),
        c => c.to_string(),
    }).collect();
    format!("\"{escaped}\"")
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let format = args.get(1).cloned().unwrap_or_default();
    let scenario = args.get(2).cloned().unwrap_or_default();
    let mismatch = scenario == "mismatch";
    let (ok, error, acked) = match run(&format, mismatch, &args[3.min(args.len())..]).await {
        Ok(n) => (true, String::new(), n),
        Err(e) => (false, e, 0),
    };
    println!("ZB_RESULT {{\"ok\": {ok}, \"error\": {}, \"acked\": {acked}}}", quote(&error));
}
