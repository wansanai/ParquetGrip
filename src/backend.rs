// ParquetGrip - A high-performance Parquet file viewer.
// Copyright (c) 2026 Edward (wansanai)
// SPDX-License-Identifier: MIT

use duckdb::{Connection, Result};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub enum BackendMessage {
    FileOpened { path: String },
    Schema { path: String, columns: Vec<String> },
    QueryData { path: String, rows: Vec<Vec<String>> },
    RowCount { path: String, count: usize },
    Error { path: Option<String>, message: String },
}

#[derive(Clone)]
pub struct Backend {
    conn: Arc<Mutex<Option<Connection>>>,
}

impl Backend {
    pub fn new() -> Self {
        Self {
            conn: Arc::new(Mutex::new(None)),
        }
    }

    fn get_conn(&self) -> Result<Arc<Mutex<Option<Connection>>>, String> {
        let mut conn_guard = self.conn.lock().map_err(|e| e.to_string())?;
        if conn_guard.is_none() {
            match Connection::open_in_memory() {
                Ok(c) => *conn_guard = Some(c),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(self.conn.clone())
    }

    pub fn open_file(&self, path: String) -> Result<BackendMessage, String> {
        let conn_arc = self.get_conn()?;
        let conn_guard = conn_arc.lock().map_err(|e| e.to_string())?;
        let conn = conn_guard.as_ref().ok_or("No connection")?;
        
        // Use a temporary check to see if we can read the file
        let sql = format!("SELECT 1 FROM read_parquet('{}') LIMIT 0;", path);
        match conn.execute(&sql, []) {
            Ok(_) => Ok(BackendMessage::FileOpened { path }),
            Err(e) => Err(e.to_string()),
        }
    }

    pub fn get_schema(&self, path: String) -> Result<BackendMessage, String> {
        let conn_arc = self.get_conn()?;
        let conn_guard = conn_arc.lock().map_err(|e| e.to_string())?;
        let conn = conn_guard.as_ref().ok_or("No connection")?;
        
        let sql = format!("DESCRIBE SELECT * FROM read_parquet('{}');", path);
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        
        let mut names = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            // column_name is the first column in DESCRIBE output
            names.push(row.get::<_, String>(0).unwrap_or_default());
        }
        Ok(BackendMessage::Schema { path, columns: names })
    }

    pub fn get_row_count(&self, path: String) -> Result<usize, String> {
        let conn_arc = self.get_conn()?;
        let conn_guard = conn_arc.lock().map_err(|e| e.to_string())?;
        let conn = conn_guard.as_ref().ok_or("No connection")?;
        
        let sql = format!("SELECT count(*) FROM read_parquet('{}');", path);
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        
        if let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let count: i64 = row.get(0).map_err(|e| e.to_string())?;
            return Ok(count as usize);
        }
        Ok(0)
    }

    pub fn run_query(&self, path: String, query_template: String, limit: Option<usize>, offset: Option<usize>) -> Result<BackendMessage, String> {
        let conn_arc = self.get_conn()?;
        let conn_guard = conn_arc.lock().map_err(|e| e.to_string())?;
        let conn = conn_guard.as_ref().ok_or("No connection")?;
        
        // Simple replacement: replace $TABLE with read_parquet('path')
        let mut query = query_template.replace("$TABLE", &format!("read_parquet('{}')", path));
        
        if let Some(l) = limit {
            query.push_str(&format!(" LIMIT {}", l));
        }
        if let Some(o) = offset {
            query.push_str(&format!(" OFFSET {}", o));
        }

        let mut stmt = conn.prepare(&query).map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        
        let mut column_count = 0;
        let mut result_rows = Vec::new();
        let mut row_count = 0;
        
        // Safety break
        let max_rows = limit.unwrap_or(50_000);

        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            if row_count >= max_rows {
                break;
            }
            
            if column_count == 0 {
                while row.get_ref(column_count).is_ok() {
                    column_count += 1;
                }
            }
            
            let mut row_data = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let val_ref = row.get_ref(i).unwrap();
                row_data.push(value_ref_to_string(val_ref));
            }
            result_rows.push(row_data);
            row_count += 1;
        }

        Ok(BackendMessage::QueryData { path, rows: result_rows })
    }
}

use duckdb::types::{ValueRef, TimeUnit};
use chrono::{Utc, TimeZone, NaiveDate, Duration};

fn value_ref_to_string(v: ValueRef<'_>) -> String {
    match v {
        ValueRef::Null => "(null)".to_string(),
        ValueRef::Boolean(b) => b.to_string(),
        ValueRef::TinyInt(i) => i.to_string(),
        ValueRef::SmallInt(i) => i.to_string(),
        ValueRef::Int(i) => i.to_string(),
        ValueRef::BigInt(i) => i.to_string(),
        ValueRef::HugeInt(i) => i.to_string(),
        ValueRef::UTinyInt(i) => i.to_string(),
        ValueRef::USmallInt(i) => i.to_string(),
        ValueRef::UInt(i) => i.to_string(),
        ValueRef::UBigInt(i) => i.to_string(),
        ValueRef::Float(f) => f.to_string(),
        ValueRef::Double(f) => f.to_string(),
        ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
        ValueRef::Blob(b) => format!("<blob {} bytes>", b.len()),
        ValueRef::Date32(d) => {
            if let Some(date) = NaiveDate::from_ymd_opt(1970, 1, 1) {
                if let Some(final_date) = date.checked_add_signed(Duration::days(d as i64)) {
                    return final_date.format("%Y-%m-%d").to_string();
                }
            }
            format!("Date32({})", d)
        }
        ValueRef::Time64(_u, t) => format!("Time64({})", t),
        ValueRef::Timestamp(u, t) => {
            let dt = match u {
                TimeUnit::Second => Utc.timestamp_opt(t, 0),
                TimeUnit::Millisecond => Utc.timestamp_opt(t / 1000, ((t % 1000) * 1_000_000) as u32),
                TimeUnit::Microsecond => Utc.timestamp_opt(t / 1_000_000, ((t % 1_000_000) * 1000) as u32),
                TimeUnit::Nanosecond => Utc.timestamp_opt(t / 1_000_000_000, (t % 1_000_000_000) as u32),
            };
            
            match dt {
                chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                _ => format!("Timestamp({:?}, {})", u, t),
            }
        }
        ValueRef::Interval { months, days, nanos } => format!("Interval(M: {}, D: {}, N: {})", months, days, nanos),
        ValueRef::Decimal(d) => d.to_string(),
        ValueRef::List(_t, _idx) => "[List]".to_string(),
        ValueRef::Struct(_s, _idx) => "{Struct}".to_string(),
        ValueRef::Enum(_t, idx) => format!("Enum({})", idx),
        _ => format!("{:?}", v),
    }
}
