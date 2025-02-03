use calamine::{open_workbook, Data, DataType, Range, Reader, Xlsx};
use csv::{Writer, WriterBuilder};
use std::io::{Cursor, Read, Seek};

use crate::error::ProcessorError;

pub struct ExcelReader<R: Read + Seek> {
    workbook: Xlsx<R>,
}

impl<R: Read + Seek> ExcelReader<R> {
    pub fn new(reader: R) -> Result<Self, ProcessorError> {
        let workbook = Xlsx::new(reader).map_err(|e| {
            ProcessorError::Processing(format!("Failed to open Excel workbook: {}", e))
        })?;
        Ok(Self { workbook })
    }

    pub fn get_sheet_as_csv(&mut self, sheet_name: &str) -> Result<Vec<u8>, ProcessorError> {
        let range = self.workbook.worksheet_range(sheet_name).map_err(|e| {
            ProcessorError::Processing(format!("Sheet '{sheet_name}' not found in workbook: {e}"))
        })?;

        let mut writer = WriterBuilder::new().from_writer(vec![]);

        // Write each row to CSV
        for row in range.rows() {
            let row_data: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(row, cell)| match cell {
                    Data::Int(n) => n.to_string(),
                    Data::Float(n) => n.to_string(),
                    Data::String(s) => s.to_string(),
                    Data::Bool(b) => b.to_string(),
                    Data::DateTime(n) => {
                        // Excel dates are stored as number of days since 1900-01-01
                        // First convert to seconds since epoch
                        let chrono_datetime_opt = n.as_datetime();
                        match chrono_datetime_opt {
                            Some(chrono_datetime) => chrono_datetime.format("%Y-%m-%d").to_string(),
                            None => {
                                tracing::warn!("Failed to parse Excel date: {} [row {}]", n, row);
                                n.to_string()
                            }
                        }
                    }
                    Data::DateTimeIso(n) => n.to_string(),
                    Data::DurationIso(n) => n.to_string(),
                    Data::Error(e) => format!("ERROR: {}", e),
                    Data::Empty => String::new(),
                })
                .collect();
            writer.write_record(&row_data).map_err(|e| {
                ProcessorError::Processing(format!("Failed to write CSV record: {}", e))
            })?;
        }

        writer.flush().map_err(|e| {
            ProcessorError::Processing(format!("Failed to flush CSV writer: {}", e))
        })?;

        Ok(writer
            .into_inner()
            .map_err(|e| ProcessorError::Processing(format!("Failed to get CSV data: {}", e)))?)
    }

    pub fn sheet_names(&self) -> Vec<String> {
        self.workbook.sheet_names().to_vec()
    }
}
