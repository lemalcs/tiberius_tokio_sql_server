use tiberius::{AuthMethod, Client, Config, Query, QueryItem};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tokio_stream::StreamExt;

async fn connect_with_host_port() -> Result<Client<Compat<TcpStream>>, Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.authentication(AuthMethod::Integrated);
    config.host("127.0.0.1");
    config.port(22828);
    config.database("FakeAdventureWorks");
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr()).await?;
    let client = Client::connect(config, tcp.compat_write()).await?;

    Ok(client)
}

pub async fn create_table() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let statement = Query::new(
        r#"
create table dbo.SalesOrderHeader
(
    SalesOrderID           int identity
        constraint PK_SalesOrderHeader_SalesOrderID
            primary key,
    RevisionNumber         tinyint
        constraint DF_SalesOrderHeader_RevisionNumber default 0       not null,
    OrderDate              datetime
        constraint DF_SalesOrderHeader_OrderDate default getdate()    not null,
    DueDate                datetime                                   not null,
    ShipDate               datetime,
    Status                 tinyint
        constraint DF_SalesOrderHeader_Status default 1               not null
        constraint CK_SalesOrderHeader_Status
            check ([Status] >= 0 AND [Status] <= 8),
    SalesOrderNumber       as isnull(N'SO' + CONVERT([nvarchar](23), [SalesOrderID]), N'*** ERROR ***'),
    CreditCardApprovalCode varchar(15),
    SubTotal               money
        constraint DF_SalesOrderHeader_SubTotal default 0.00          not null
        constraint CK_SalesOrderHeader_SubTotal
            check ([SubTotal] >= 0.00),
    TaxAmt                 money
        constraint DF_SalesOrderHeader_TaxAmt default 0.00            not null
        constraint CK_SalesOrderHeader_TaxAmt
            check ([TaxAmt] >= 0.00),
    Freight                money
        constraint DF_SalesOrderHeader_Freight default 0.00           not null
        constraint CK_SalesOrderHeader_Freight
            check ([Freight] >= 0.00),
    TotalDue               as isnull([SubTotal] + [TaxAmt] + [Freight], 0),
    Comment                nvarchar(128),
    rowguid                uniqueidentifier default newid()        not null,
    ModifiedDate           datetime
)
    "#,
    );

    let _ = statement.execute(&mut client).await?;
    println!("Created table");

    let _ = client.close().await?;

    Ok(())
}

pub async fn insert_row() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let result = client
        .execute(
            r#"INSERT INTO dbo.SalesOrderHeader (
        RevisionNumber, OrderDate, DueDate, ShipDate, Status, CreditCardApprovalCode,
        SubTotal, TaxAmt, Freight, Comment, rowguid, ModifiedDate
        )
        VALUES
        (
        @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, @P11, @P12
        );"#,
            // These are the values for the parameters in the
            // INSERT statement
            &[
                &8i32,                                   // RevisionNumber
                &"2024-07-30",                           // OrderDate
                &"2024-08-12",                           // DueDate
                &"2024-07-07",                           // ShipDate
                &5i32,                                   // Status
                &"105041Vi84182",                        // CreditCardApprovalCode
                &20565.6206f64,                          // SubTotal
                &1971.5149f64,                           // TaxAmt
                &616.0984f64,                            // Freight
                &None::<&str>,                           // Comment
                &"79B65321-39CA-4115-9CBA-8FE0903E12E6", // rowguid
                &"2024-07-07",                           // ModifiedDate
            ],
        )
        .await?;

    println!("Rows affected: {}", result.total());

    client.close().await?;

    Ok(())
}

pub async fn select_row() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    // Get the sale with an order ID equals to 1
    let mut result = Query::new(
        r#"select SalesOrderID,
       RevisionNumber,
       OrderDate,
       DueDate,
       ShipDate,
       Status,
       SalesOrderNumber,
       CreditCardApprovalCode,
       SubTotal,
       TaxAmt,
       Freight,
       TotalDue,
       Comment,
       rowguid,
       ModifiedDate
from dbo.SalesOrderHeader
WHERE
    SalesOrderID = @P1;"#,
    );

    // this will be the value of the parameter @P1
    result.bind(1i32);

    // get the result set from SQL Server
    let mut rows = result.query(&mut client).await?;

    // This will read all the returned rows
    while let Some(row) = rows.try_next().await? {
        match row {
            // This section contains the rows returned by the query
            QueryItem::Row(r) => {
                let salesorderid: i32 = r.get(0).unwrap();
                let revisionnumber: u8 = r.get(1).unwrap();
                let orderdate: chrono::NaiveDateTime = r.get(2).unwrap();
                let duedate: chrono::NaiveDateTime = r.get(3).unwrap();
                let shipdate: chrono::NaiveDateTime = r.get(4).unwrap();
                let status: u8 = r.get(5).unwrap();
                let salesordernumber: &str = r.get(6).unwrap();
                let creditcardapprovalcode: &str = r.get(7).unwrap();
                let subtotal: f64 = r.get(8).unwrap();
                let taxamt: f64 = r.get(9).unwrap();
                let freight: f64 = r.get(10).unwrap();
                let totaldue: f64 = r.get(11).unwrap();
                let comment: &str = r.get(12).unwrap_or_else(|| "");
                let rowguid: uuid::Uuid = r.get(13).unwrap();
                let modifieddate: chrono::NaiveDateTime = r.get(14).unwrap();

                println!("SalesOrderID: {}", salesorderid);
                println!("RevisionNumber: {}", revisionnumber);
                println!("OrderDate: {}", orderdate);
                println!("DueDate: {}", duedate);
                println!("ShipDate: {}", shipdate);
                println!("Status: {}", status);
                println!("SalesOrderNumber: {}", salesordernumber);
                println!(
                    "CreditCardApprovalCode: {}",
                    creditcardapprovalcode.to_owned()
                );
                println!("SubTotal: {}", subtotal);
                println!("TaxAmt: {}", taxamt);
                println!("Freight: {}", freight);
                println!("TotalDue: {}", totaldue);
                println!("Comment: {}", comment.to_owned());
                println!("rowguid: {}", rowguid);
                println!("ModifiedDate: {}", modifieddate);
                println!("--------------------------------------");
                println!();
            }

            // This section contains the metadata of the result set
            QueryItem::Metadata(meta) => {
                println!("Metadata: {:?}", meta);
                // The above line comes out with this:
                // ResultMetadata { columns: [
                // Column { name: "SalesOrderID", column_type: Int4 },
                // Column { name: "RevisionNumber", column_type: Int1 },
                // Column { name: "OrderDate", column_type: Datetime },
                // Column { name: "DueDate", column_type: Datetime },
                // Column { name: "ShipDate", column_type: Datetimen },
                // Column { name: "Status", column_type: Int1 },
                // Column { name: "SalesOrderNumber", column_type: NVarchar },
                // Column { name: "CreditCardApprovalCode", column_type: BigVarChar },
                // Column { name: "SubTotal", column_type: Money },
                // Column { name: "TaxAmt", column_type: Money },
                // Column { name: "Freight", column_type: Money },
                // Column { name: "TotalDue", column_type: Money },
                // Column { name: "Comment", column_type: NVarchar },
                // Column { name: "rowguid", column_type: Guid },
                // Column { name: "ModifiedDate", column_type: Datetimen }], result_index: 0 }
            }
        }
    }

    Ok(())
}

pub async fn update_row() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let result = client
        .execute(
            r#"UPDATE dbo.SalesOrderHeader
SET
    RevisionNumber = @P1,
    OrderDate = @P2,
    DueDate = @P3,
    ShipDate = @P4,
    Status = @P5,
    CreditCardApprovalCode = @P6,
    SubTotal = @P7,
    TaxAmt = @P8,
    Freight = @P9,
    Comment = @P10,
    rowguid = @P11,
    ModifiedDate = @P12
WHERE
    SalesOrderID = @P13;"#,
            // These will be the values for the parameters in the
            // UPDATE statement
            &[
                &8i32,                                             // RevisionNumber
                &"2024-07-31",                                     // OrderDate
                &"2024-08-12",                                     // DueDate
                &"2024-07-07",                                     // ShipDate
                &5i32,                                             // Status
                &"105041Vi84182",                                  // CreditCardApprovalCode
                &20565.6206f64,                                    // SubTotal
                &1971.5149f64,                                     //TaxAmt
                &616.0984f64,                                      //Freight
                &"I updated this row from a Rust 🦀 application.", // Comment
                &"6d805000-034b-421e-8489-9168b7fe3de6",           // rowguid
                &"2024-07-07",                                     // ModifiedDate
                &1i32,                                             //SalesOrderID
            ],
        )
        .await?;

    println!("Rows affected: {}", result.total());

    client.close().await?;

    Ok(())
}

pub async fn delete_row() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    // Delete the sale with order ID equals to 1
    let result = client
        .execute(
            r#"DELETE FROM dbo.SalesOrderHeader
WHERE
    SalesOrderID = @P1;"#,
            // this will be the value of the parameter @P1
            &[&1i32],
        )
        .await?;

    println!("Rows affected: {}", result.total());

    client.close().await?;

    Ok(())
}