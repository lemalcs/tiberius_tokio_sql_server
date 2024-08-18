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

pub async fn create_stored_procedure() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let _ = client
        .simple_query(
            r#"
create procedure dbo.uspSaveOrderHeader @DueDate datetime,
                                        @ShipDate datetime,
                                        @CreditCardApprovalCode varchar(15),
                                        @Comment nvarchar(128) = null,
                                        @ModifiedDate datetime
as
begin
    insert dbo.SalesOrderHeader(DueDate, ShipDate, CreditCardApprovalCode, Comment, ModifiedDate)
    values (@DueDate, @ShipDate, @CreditCardApprovalCode, @Comment, @ModifiedDate)
end

    "#,
        )
        .await?;

    println!("Created stored procedure.");

    Ok(())
}

pub async fn call_stored_procedure() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    // The @Comment parameter is not set because it defaults to NULL
    // However it's still possible to set this parameter if desired
    let _ = client
        .execute(
            r#"exec dbo.uspSaveOrderHeader
        @DueDate = @P1,
        @ShipDate = @P2,
        @CreditCardApprovalCode = @P3,
        @ModifiedDate = @P4"#,
            &[
                &"2024-08-20", // DueDate
                &"2024-08-22", // ShipDate
                &"12345",      // CreditCardApprovalCode
                &"2024-08-20", // ModifiedDate
            ],
        )
        .await?;

    println!("Sale order created.");

    Ok(())
}

pub async fn create_stored_procedure_output_parameter() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    // @SalesOrderID is the output parameter of the stored procedure
    let _ = client
        .simple_query(
            r#"
create procedure dbo.uspSaveOrderHeaderGetID @DueDate datetime,
                                             @ShipDate datetime,
                                             @CreditCardApprovalCode varchar(15),
                                             @Comment nvarchar(128) = null,
                                             @ModifiedDate datetime,
                                             @SalesOrderID int output
as
begin
    insert dbo.SalesOrderHeader(DueDate, ShipDate, CreditCardApprovalCode, Comment, ModifiedDate)
    values (@DueDate, @ShipDate, @CreditCardApprovalCode, @Comment, @ModifiedDate)

    set @SalesOrderID = @@identity
end
    "#,
        )
        .await?;

    println!("Created stored procedure with output parameter.");

    Ok(())
}

pub async fn call_stored_procedure_output_parameter() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let mut result = client
        .query(
            r#"
        declare @NewSalesOrderID int
        exec dbo.uspSaveOrderHeaderGetID
        @DueDate = @P1,
        @ShipDate = @P2,
        @CreditCardApprovalCode = @P3,
        @ModifiedDate = @P4,
        @SalesOrderID = @NewSalesOrderID output

        select @NewSalesOrderID as SalesOrderID
        "#,
            &[
                &"2024-09-12", // DueDate
                &"2024-09-22", // ShipDate
                &"10045AV521", // CreditCardApprovalCode
                &"2024-09-12", // ModifiedDate
            ],
        )
        .await?;

    // Since the output parameter (@SalesOrderID) is into a result set,
    // we need to iterate over it to get the actual output value
    while let Some(row) = result.try_next().await? {
        if let QueryItem::Row(r) = row {
            let sales_order_id: i32 = r.get("SalesOrderID").unwrap();
            println!("Sale order created with ID: {}", sales_order_id);
        }
    }

    Ok(())
}

pub async fn create_procedure_returns_status_code() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    // The `return` keyword is optional in SQL Server stored procedures,
    // if not specified, the database engine returns 0
    let _ = client
        .simple_query(
            r#"
create procedure dbo.uspUpdateOrderStatus @SalesOrderID int,
                                      @Status int
as
begin
    begin try
        update SalesOrderHeader
        set Status=@Status
        where SalesOrderID = @SalesOrderID
        return 0
    end try
    begin catch
        return -2
    end catch
end
    "#,
        )
        .await?;

    println!("Created stored procedure that returns a status code.");

    Ok(())
}

pub async fn call_stored_procedure_returns_status_code() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let sales_order_id: i32 = 2;
    let status: i32 = 16;

    // It is trying to change the status of a sales order
    // valid values must be between 0 and 8
    let mut result = client
        .query(
            r#"
        declare @ReturnCode int
        exec @ReturnCode = dbo.uspUpdateOrderStatus @SalesOrderID = @P1, @Status = @P2
        select @ReturnCode as ReturnCode
        "#,
            &[
                &sales_order_id, // SalesOrderID
                &status,         // Status
            ],
        )
        .await?;

    // Check the return value
    while let Some(row) = result.try_next().await? {
        if let QueryItem::Row(r) = row {
            let return_code: i32 = r.get("ReturnCode").unwrap();

            if return_code == 0 {
                println!(
                    "Sales order with ID={} updated to {}.",
                    sales_order_id, status
                );
            } else {
                // The return code is -2, which means the stored procedure failed
                // You can fail the function as well
                println!("The status {} is not valid.", status);
            }
        }
    }

    Ok(())
}

pub async fn create_stored_procedure_returns_table() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let _ = client
        .simple_query(
            r#"
create procedure dbo.uspGetSaleOrderByID @SalesOrderID int
as
begin

    select SalesOrderID,
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
    into #saleorder
    from dbo.SalesOrderHeader
    where
        SalesOrderID=@SalesOrderID

    -- Get sale order detail
    select SalesOrderID,
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
    from #saleorder

    -- Get receipt (summary)
    select SalesOrderID,
           OrderDate,
           SubTotal,
           TaxAmt,
           Freight,
           TotalDue
    from #saleorder
end
    "#,
        )
        .await?;

    println!("Created stored procedure that returns a table.");

    Ok(())
}

pub async fn call_stored_procedure_returns_table() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect_with_host_port().await?;

    let mut result = client
        .query(
            r#"
        exec dbo.uspGetSaleOrderByID @SalesOrderID = @P1
        "#,
            &[&2i32], // SalesOrderID
        )
        .await?;

    while let Some(row) = result.try_next().await? {
        if let QueryItem::Row(r) = row {
            let number_columns = r.columns().iter().count();

            // The index identifies the result set
            // (0 for the first result set, 1 for the second, and so on)
            // If the stored procedure returns multiple result sets,
            // you can handle them accordingly
            let result_index = r.result_index();
            println!("Number of columns: {}", number_columns);
            println!("Result index: {}", result_index);

            if result_index == 0 {
                println!("Sale order details:");

                // Print each column value
                let sales_order_id: i32 = r.get("SalesOrderID").unwrap();
                let revision_number: u8 = r.get("RevisionNumber").unwrap();
                let order_date: chrono::NaiveDateTime = r.get("OrderDate").unwrap();
                let due_date: chrono::NaiveDateTime = r.get("DueDate").unwrap();
                let ship_date: chrono::NaiveDateTime = r.get("ShipDate").unwrap();
                let status: u8 = r.get("Status").unwrap();
                let sales_order_number: &str = r.get("SalesOrderNumber").unwrap();
                let credit_card_approval_code: &str = r.get("CreditCardApprovalCode").unwrap();
                let subtotal: f64 = r.get("SubTotal").unwrap();
                let tax_amt: f64 = r.get("TaxAmt").unwrap();
                let freight: f64 = r.get("Freight").unwrap();
                let total_due: f64 = r.get("TotalDue").unwrap();
                let comment: &str = r.get("Comment").unwrap_or_else(|| "");
                let rowguid: uuid::Uuid = r.get("rowguid").unwrap();
                let modified_date: chrono::NaiveDateTime = r.get("ModifiedDate").unwrap();

                println!("Sale order ID: {}", sales_order_id);
                println!("Revision number: {}", revision_number);
                println!("Order date: {}", order_date);
                println!("Due date: {}", due_date);
                println!("Ship date: {}", ship_date);
                println!("Status: {}", status);
                println!("Sales order number: {}", sales_order_number);
                println!("Credit card approval code: {}", credit_card_approval_code);
                println!("Subtotal: {}", subtotal);
                println!("Tax amount: {}", tax_amt);
                println!("Freight: {}", freight);
                println!("Total due: {}", total_due);
                println!("Comment: {}", comment);
                println!("Row GUID: {}", rowguid);
                println!("Modified date: {}", modified_date);
                println!();
            } else if result_index == 1 {
                println!("Receipt summary:");

                // Print each column value
                let sales_order_id: i32 = r.get("SalesOrderID").unwrap();
                let order_date: chrono::NaiveDateTime = r.get("OrderDate").unwrap();
                let subtotal: f64 = r.get("SubTotal").unwrap();
                let tax_amt: f64 = r.get("TaxAmt").unwrap();
                let freight: f64 = r.get("Freight").unwrap();
                let total_due: f64 = r.get("TotalDue").unwrap();

                println!("Sale order ID: {}", sales_order_id);
                println!("Order date: {}", order_date);
                println!("Subtotal: {}", subtotal);
                println!("Tax amount: {}", tax_amt);
                println!("Freight: {}", freight);
                println!("Total due: {}", total_due);
                println!();
            }
        }
    }
    Ok(())
}