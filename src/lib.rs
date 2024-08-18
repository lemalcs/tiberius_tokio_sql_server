mod connections;
mod tables;
mod stored_procedures;
mod functions;

pub use connections::*;
pub use tables::*;
pub use stored_procedures::*;
pub use functions::*;

#[tokio::test]
async fn connect_to_sql_server_using_host_port() {
    let result = connect_with_host_port().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn connect_to_sql_server_using_host_port_username_password() {
    let result = connect_with_host_port_username_password().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn connect_to_sql_server_with_sql_browser(){
    let result= connect_with_sql_browser().await;
    assert_eq!(result.is_ok(),true);
}

#[tokio::test]
async fn connect_to_sql_server_using_ado_sql_browser() {
    let result = connect_with_ado_sql_browser().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn connect_to_sql_server_using_ado_host_port() {
    let result = connect_with_ado_host_port().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn connect_to_sql_server_using_jdbc_host_port() {
    let result = connect_with_jdbc_host_port().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn connect_to_sql_server_using_jdbc_sql_browser() {
    let result = connect_with_jdbc_sql_browser().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn create_table_in_sql_server() {
    let result = create_table().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn insert_row_in_sql_server(){
    let result = insert_row().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn select_row_from_sql_server() {
    let result = select_row().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn update_row_in_sql_server() {
    let result = update_row().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn delete_row_in_sql_server() {
    let result = delete_row().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn create_stored_procedure_in_sql_server() {
    let result = create_stored_procedure().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn call_stored_procedure_in_sql_server() {
    let result = call_stored_procedure().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn create_stored_procedure_output_parameter_in_sql_server() {
    let result = create_stored_procedure_output_parameter().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn call_stored_procedure_output_parameter_in_sql_server() {
    let result = call_stored_procedure_output_parameter().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn create_stored_procedure_returns_status_code_in_sql_server() {
    let result = create_procedure_returns_status_code().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn call_stored_procedure_returns_status_code_in_sql_server() {
    let result = call_stored_procedure_returns_status_code().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn call_create_stored_procedure_returns_table_in_sql_server() {
    let result = create_stored_procedure_returns_table().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn call_stored_procedure_returns_table_in_sql_server() {
    let result = call_stored_procedure_returns_table().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn call_create_function_in_sql_server() {
    let result = create_scalar_function().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn call_scalar_function_in_sql_server() {
    let result = call_scalar_function().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn create_table_valued_function_in_sql_server() {
    let result = create_table_valued_function().await;
    assert_eq!(result.is_ok(), true);
}

#[tokio::test]
async fn call_table_valued_function_in_sql_server() {
    let result = call_table_valued_function().await;
    assert_eq!(result.is_ok(), true);
}