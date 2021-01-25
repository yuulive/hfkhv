#[macro_use] extern crate anyhow;
extern crate postgres;
use clap;
pub mod project;
pub mod database;
pub mod utils;


fn main() -> anyhow::Result<()> {

    let mut clap: clap::App = clap::App::new("pgfine")
    .version(clap::crate_version!())
    .author("Marius Kavaliauskas <mariuskava@gmail.com>")
    .about("Yet another database migration tool for postgres.")
    .subcommand(clap::App::new("init")
        .about("initializes new pgfine project"))
    .subcommand(clap::App::new("create")
        .about("creates fresh database"))
    .subcommand(clap::App::new("migrate")
        .about("updates database"))
    .subcommand(clap::App::new("drop")
        .about("drop entire database")
        .arg(clap::Arg::new("no-joke") // wtf
            .long("no-joke")
            .about("confirmation")));

    let matches = clap.clone().get_matches();
    
    match matches.subcommand() {
        Some(("init", subcommand_matches)) => { 
            subcommand_init(subcommand_matches)?;
        },
        Some(("create", subcommand_matches)) => { 
            subcommand_create(subcommand_matches)?;
        },
        Some(("migrate", _subcommand_matches)) => { 
            bail!("not implemented");
        },
        Some(("drop", subcommand_matches)) => { 
            subcommand_drop(subcommand_matches)?;
        },
        _ => {
            clap.print_help()?
        }
    }

    return Ok(());
}


fn subcommand_init(_matches: &clap::ArgMatches) -> anyhow::Result<()> {
    project::init("./pgfine")?;
    return Ok(());
}

fn subcommand_create(_matches: &clap::ArgMatches) -> anyhow::Result<()> {
    let database_project = project::load()?;
    database::create(database_project)?;
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
