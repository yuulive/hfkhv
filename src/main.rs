#[macro_use] extern crate anyhow;
extern crate postgres;
use clap;
pub mod project;
pub mod database;


fn main() -> anyhow::Result<()> {

    let mut clap: clap::App = clap::App::new("pgfine")
    .version("0.1.0")
    .author("Marius Kavaliauskas <mariuskava@gmail.com>")
    .about("Yet another database migration tool for postgres.")
    .subcommand(clap::App::new("init")
        .about("initializes new pgfine project"))
    .subcommand(clap::App::new("create")
        .about("creates fresh database"))
    .subcommand(clap::App::new("migrate")
        .about("updates database"))
    .subcommand(clap::App::new("truncate")
        .about("deletes all defined database objects")
        .arg(clap::Arg::new("no-joke") // wtf
            .long("no-joke")
            .about("confirmation")));

    let matches = clap.clone().get_matches();

    // more program logic goes here...

    
    match matches.subcommand() {
        Some(("init", subcommand_matches)) => { 
            subcommand_init(subcommand_matches)?;
        },
        Some(("create", _subcommand_matches)) => { 
            println!("not implemented");
        },
        Some(("migrate", _subcommand_matches)) => { 
            println!("not implemented");
        },
        Some(("truncate", subcommand_matches)) => { 
            subcommand_truncate(subcommand_matches);
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

fn subcommand_truncate(matches: &clap::ArgMatches) {
    if !matches.is_present("no-joke") {
        println!("Are you sure? Try with --no-joke argument");
    } else {
        println!("truncateing objects...");
        panic!("not implemented");
    }
}


