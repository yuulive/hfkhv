#[macro_use] extern crate anyhow;

use clap;
pub mod project;
pub mod database;
pub mod utils;


fn main() -> anyhow::Result<()> {

    let about = format!("{}

ENVIRONMENT VARIABLES:
    PGFINE_DIR                      project location
    PGFINE_CONNECTION_STRING        connection string for target database
    PGFINE_ADMIN_CONNECTION_STRING  connection string for admin database
    PGFINE_ROOT_CERT                path to root certificate to verify server's certificate
    PGFINE_ROLE_PREFIX              role prefix to make them unique per environment
",
        clap::crate_description!(),        
    );

    let about_str = about.as_str();

    let mut clap: clap::App = clap::App::new(clap::crate_name!())
    .version(clap::crate_version!())
    .author(clap::crate_authors!("\n"))
    .about(about_str)
    .subcommand(clap::App::new("init")
        .about("initialize new pgfine project"))
    .subcommand(clap::App::new("migrate")
        .about("update database"))
    .subcommand(clap::App::new("drop")
        .about("drop entire database")
        .arg(clap::Arg::new("no-joke") // wtf
            .long("no-joke")
            .about("confirmation")));

    let matches = clap.clone().get_matches();
    
    match matches.subcommand() {
        Some(("init", subcommand_matches)) => {
            utils::validate_environment()?;
            subcommand_init(subcommand_matches)?;
        },
        Some(("migrate", subcommand_matches)) => {
            utils::validate_environment()?;
            subcommand_migrate(subcommand_matches)?;
        },
        Some(("drop", subcommand_matches)) => {
            utils::validate_environment()?;
            subcommand_drop(subcommand_matches)?;
        },
        _ => {
            clap.print_help()?
        }
    }

    return Ok(());
}


fn subcommand_init(_matches: &clap::ArgMatches) -> anyhow::Result<()> {
    project::init()?;
    return Ok(());
}

fn subcommand_migrate(_matches: &clap::ArgMatches) -> anyhow::Result<()> {
    let database_project = project::load()?;
    database::migrate(database_project)?;
    return Ok(());
}

fn subcommand_drop(matches: &clap::ArgMatches) -> anyhow::Result<()> {
    if !matches.is_present("no-joke") {
        println!("Are you sure? Try with --no-joke argument");
    } else {
        let database_project = project::load()?;
        database::drop(database_project)?;
    }
    return Ok(());
}
