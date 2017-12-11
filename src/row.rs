use std::ops::Index;
use types::SqlValue;

/// Represents a row of data returned from a SQL query.
///
#[derive(Debug)]
pub struct Row {
    cells: Vec<SqlValue>,
}
impl Row {
    pub(crate) fn new(
        cells: Vec<SqlValue>
    )-> Self {
        Row { cells }
    }

    /// Returns the columns in the row.
    ///
    pub fn cells(&self) -> &Vec<SqlValue> {
        &self.cells
    }
}
impl Index<usize> for Row {
    type Output = SqlValue;

    fn index(&self, index: usize) -> &SqlValue {
        &self.cells[index]
    }
}
