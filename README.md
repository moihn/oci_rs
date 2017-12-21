# oci_rs

oci_rs provides a Rust wrapper to the [Oracle Call Interface][1] (OCI) library.
The Oracle site describes OCI as a "...comprehensive, high performance, native C
language interface to Oracle Database for custom or packaged applications...".

Documentation is available [here][12].

## Setup

This crate is developed against version 11.1 of the OCI library. It is expected to work with
any newer version. The OCI client library needs to be installed on your machine and can be
downloaded [here][7].

If you are on Linux then you are likely to need to tell the linker where
to find the files. Adding this to my `.bashrc` file worked for me, however the details may vary according to your distro, for [openSuse and Redhat/Oracle Enterprise Linux 7][8].

```text
export LIBRARY_PATH=$LIBRARY_PATH:/usr/lib/oracle/[oci_version]/client64/lib/
```

This crate has not been tested against Windows and so the setup will be different.

Testing has been done against a local installation of [Oracle 11g and 12g][9].
In order to run the crate tests then a local database needs to be
available on `localhost:1521/xe` with a user `oci_rs` and password `test`.

In order to use `oci_rs` add this to your `Cargo.toml`:

```toml
[dependencies]
oci_rs = "0.5.0"
```
and this to your crate root:

```rust
extern crate oci_rs;
```

## Examples

In the following example we will create a connection to a database and then create a table,
insert a couple of rows using bind variables and then execute a query to fetch them back again.
There is a lot of error handling needed. Every OCI function call can fail and so `Result` and
`Option` are used extensively. The below code takes the usual documentation shortcut of calling
`unwrap()` a lot but doing so in real client code will prove ill-fated. Any remote database connection is
inherently unreliable.

```rust
use oci_rs::connection::Connection;

let conn = Connection::new("localhost:1521/xe", "oci_rs", "test").unwrap();

// Create a table
let sql_create = "CREATE TABLE Toys (ToyId int,
                                     Name varchar(20),
                                     Price float)";
let mut create = conn.create_prepared_statement(sql_create).unwrap();

// Execute the create statement
create.execute().unwrap();

// Commit in case we lose connection (an abnormal disconnection would result
// in an automatic roll-back.)
create.commit().unwrap();

// Insert some values using bind variables
let sql_insert = "INSERT INTO Toys (ToyId, Name, Price)
                  VALUES (:id, :name, :price)";
let mut insert = conn.create_prepared_statement(sql_insert).unwrap();

let values = [(1, "Barbie", 23.45),
              (2, "Dinosaurs", -5.21)];

// Run through the list of values, bind them and execute the statement
for value in values.iter() {
    insert.bind(&[&value.0, &value.1, &value.2]).unwrap();
    insert.execute().unwrap()
}

insert.commit().unwrap();

{
    // create a select query
    let sql = "SELECT * FROM AM_USER_ROLE";
    let mut select = match conn.create_prepared_statement(sql)
    {
        Ok(select) => select,
        Err(err) => panic!("failed to create select statement {}", err),
    };
    // get optional result, which is result set for select query
    let optional_rs = select.execute().unwrap();
    if let Some(mut rs) = optional_rs {
        {
            let columns = rs.columns().unwrap();
            for column in columns {
                print!("{},", column.name());
            }
            println!();
        }
        while let Some(row) = rs.next() {
            for cell in row.unwrap().cells()
            {
                match String::from_sql_value(&cell)
                {
                    Some(vstr) => print!("{},", vstr),
                    None => print!("unsupported_value_type"),
                }
            }
            println!();
        }
    }
}

{
    // create a same query but get the result set in another way
    let sql = "SELECT * FROM AM_USER_ROLE";
    let mut select = match conn.create_prepared_statement(sql)
    {
        Ok(select) => select,
        Err(err) => panic!("failed to create select statement {}", err),
    };
    // get the result set
    let optional_rs = select.execute().unwrap();
    if let Some(mut rs) = optional_rs {
        {
            let columns = rs.columns().unwrap();
            for column in columns {
                print!("{},", column.name());
            }
            println!();
        }
        // get all the result rows at once
        let rows_result: Result<Vec<_>, OciError> = rs.collect();
        match rows_result {
            Ok(rows) => for row in rows {
                for cell in row.cells()
                {
                    match String::from_sql_value(&cell)
                    {
                        Some(vstr) => print!("{},", vstr),
                        None => print!("unsupported_value_type"),
                    }
                }
                /*
                 if you know the type of column value,
                 you also can use something like:
                 let id: i64 = first_row[0].value().unwrap();
                 let name: String = first_row[1].value().unwrap();
                 let price: f64 = first_row[2].value().unwrap();
                 */
                println!();
            },
            Err(err) => panic!("failed to get rows: {}", err),
        }
    }
}

```

[1]: http://www.oracle.com/technetwork/database/features/oci/index-090945.html
[2]: https://github.com/oracle/odpi
[3]: https://crates.io/crates/postgres
[4]: connection/struct.Connection.html
[5]: statement/struct.Statement.html
[6]: types/enum.SqlValue.html
[7]: http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html
[8]: https://www.opensuse.org/
[9]: http://www.oracle.com/technetwork/database/database-technologies/express-edition/overview/index.html
[10]: http://docs.oracle.com/database/122/LNOCI/toc.htm
[11]: https://docs.oracle.com/database/122/ERRMG/toc.htm
[12]: https://docs.rs/oci_rs/0.3.1/oci_rs/ 
