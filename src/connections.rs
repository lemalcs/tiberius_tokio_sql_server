use tiberius::{AuthMethod, Client, Config, SqlBrowser};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub async fn connect_with_host_port() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.authentication(AuthMethod::Integrated);
    config.host("127.0.0.1");
    config.port(22828);
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr()).await?;
    let client = Client::connect(config, tcp.compat_write()).await?;
    println!("Connected to SQL Server");
    client.close().await?;

    Ok(())
}

pub async fn connect_with_host_port_username_password() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();

    // Use SQL Server Authentication (user name and password)
    config.authentication(AuthMethod::sql_server("developer", "developer"));

    config.host("127.0.0.1");
    config.port(22828);
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr()).await?;
    let client = Client::connect(config, tcp.compat_write()).await?;
    println!("Connected to SQL Server");
    let _ = client.close().await?;

    Ok(())
}

pub async fn connect_with_sql_browser() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();

    // Use the currently logged user credentials
    config.authentication(AuthMethod::Integrated);

    config.host("127.0.0.1");

    // Port used by SQL Server Browser
    // It is not the port of the database server instance
    config.port(1434);
    config.instance_name("SQL2022D");
    config.trust_cert();

    // Enable the "sql-browser-tokio" feature of Tiberius
    // in Cargo.toml to use SQL Server Browser with Tokio
    let tcp = TcpStream::connect_named(&config).await?;
    let client = Client::connect(config, tcp.compat_write()).await?;
    println!("Connected to SQL Server");
    let _ = client.close().await?;

    Ok(())
}

pub async fn connect_with_ado_sql_browser() -> Result<(), Box<dyn std::error::Error>> {
    // It uses an ADO.NET connection string to connect to SQL Server.
    // Replace with your actual connection string
    let config = Config::from_ado_string(
        &"Server=tcp:127.0.0.1\\SQL2022D;IntegratedSecurity=true;TrustServerCertificate=true",
    )?;

    let tcp = TcpStream::connect_named(&config).await?;
    let _ = tcp.set_nodelay(true);

    let client = Client::connect(config, tcp.compat_write()).await?;
    println!("Connected to SQL Server");
    let _ = client.close().await;

    Ok(())
}

pub async fn connect_with_ado_host_port() -> Result<(), Box<dyn std::error::Error>> {
    // It uses an ADO.NET connection string to connect to SQL Server.
    // Replace with your actual connection string
    let config = Config::from_ado_string(
        &"Server=tcp:127.0.0.1,22828;IntegratedSecurity=true;TrustServerCertificate=true",
    )?;

    let tcp = TcpStream::connect(config.get_addr()).await?;
    let _ = tcp.set_nodelay(true);

    let client = Client::connect(config, tcp.compat_write()).await?;
    println!("Connected to SQL Server");
    let _ = client.close().await;

    Ok(())
}

pub async fn connect_with_jdbc_host_port() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_jdbc_string(
        &"jdbc:sqlserver://127.0.0.1:22828;integratedSecurity=true;trustServerCertificate=true",
    )?;

    let tcp = TcpStream::connect(config.get_addr()).await?;
    let _ = tcp.set_nodelay(true);

    let client = Client::connect(config, tcp.compat_write()).await?;
    println!("Connected to SQL Server");
    let _ = client.close().await;

    Ok(())
}

pub async fn connect_with_jdbc_sql_browser() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_jdbc_string(
        &"jdbc:sqlserver://127.0.0.1\\SQL2022D;integratedSecurity=true;trustServerCertificate=true",
    )?;

    // Connect to SQL Server by its instance name
    let tcp = TcpStream::connect_named(&config).await?;
    let _ = tcp.set_nodelay(true);

    let client = Client::connect(config, tcp.compat_write()).await?;
    println!("Connected to SQL Server");
    let _ = client.close().await;

    Ok(())
}