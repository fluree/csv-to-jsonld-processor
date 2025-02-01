use calamine::{open_workbook, DataType, Range, Reader, Xlsx};
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
            let row_data: Vec<String> = row.iter().map(|cell| cell.to_string()).collect();
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
