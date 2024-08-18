use tiberius::{AuthMethod, Client, Config, QueryItem};
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

pub async fn create_scalar_function() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let _ = client
        .simple_query(
            r#"
create function dbo.ufnGetSalesOrderStatusText(@Status tinyint)
    returns nvarchar(15)
as
-- Returns the sales order status text representation for the status value.
begin
    declare @ret [nvarchar](15)

    SET @ret =
            CASE @Status
                WHEN 1 THEN 'In process'
                WHEN 2 THEN 'Approved'
                WHEN 3 THEN 'Backordered'
                WHEN 4 THEN 'Rejected'
                WHEN 5 THEN 'Shipped'
                WHEN 6 THEN 'Cancelled'
                ELSE '** Invalid **'
                end

    return @ret
end
    "#,
        )
        .await;

    println!("Created scalar function.");

    Ok(())
}

pub async fn call_scalar_function() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let mut query = client
        .query(
            r#"
SELECT SalesOrderID, Status, dbo.ufnGetSalesOrderStatusText(Status) AS StatusDescription
FROM dbo.SalesOrderHeader
WHERE SalesOrderID = @P1
    "#,
            &[&2], // SalesOrderID
        )
        .await?;

    // Read the rows returned by the SQL Server function
    while let Some(row) = query.try_next().await? {
        if let QueryItem::Row(r) = row {
            let sales_order_id: i32 = r.get("SalesOrderID").unwrap();
            let status: u8 = r.get("Status").unwrap();
            let status_description: &str = r.get("StatusDescription").unwrap();
            println!("Sale order created with ID: {}", sales_order_id);
            println!("Status: {}", status);
            println!("Status description: {}", status_description);
        }
    }

    Ok(())
}

pub async fn create_table_valued_function() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let _ = client
        .simple_query(
            r#"
create function dbo.ufnGetSalesOrderWithTotalDueMoreThan(@TotalDue money)
    returns table
        as
        return
        select SalesOrderID,
               SubTotal,
               TaxAmt,
               Freight,
               TotalDue
        from dbo.SalesOrderHeader
        where
            TotalDue > @TotalDue
    "#,
        )
        .await?;

    println!("Created table valued function.");

    Ok(())
}

pub async fn call_table_valued_function() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let due = -1;

    // Query the Sql Server function as if it was a normal table
    // using the `select` statement
    let mut query = client
        .query(
            r#"
select SalesOrderID,
       SubTotal,
       TaxAmt,
       Freight,
       TotalDue
from dbo.ufnGetSalesOrderWithTotalDueMoreThan(@P1)
    "#,
            &[&due], // TotalDue
        )
        .await?;

    // Iterate over the result set
    while let Some(row) = query.try_next().await? {
        if let QueryItem::Row(r) = row {
            let sales_order_id: i32 = r.get("SalesOrderID").unwrap();
            let subtotal: f64 = r.get("SubTotal").unwrap();
            let tax_amt: f64 = r.get("TaxAmt").unwrap();
            let freight: f64 = r.get("Freight").unwrap();
            let total_due: f64 = r.get("TotalDue").unwrap();
            println!("Sale order with ID: {}", sales_order_id);
            println!("Subtotal: {}", subtotal);
            println!("Tax amount: {}", tax_amt);
            println!("Freight: {}", freight);
            println!("Total due: {}", total_due);
        }
    }

    Ok(())
}