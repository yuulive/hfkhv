#[macro_use] extern crate anyhow;
extern crate postgres;
use clap;
mod project;

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
    .subcommand(clap::App::new("destroy")
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
        Some(("destroy", subcommand_matches)) => { 
            subcommand_destroy(subcommand_matches);
        },
        _ => {
            clap.print_help()?
        }
    }

    return Ok(());
}


fn subcommand_init(_matches: &clap::ArgMatches) -> anyhow::Result<()> {
    return project::init("./pgfine");
}


fn subcommand_destroy(matches: &clap::ArgMatches) {
    if !matches.is_present("no-joke") {
        println!("Are you sure? Try with --no-joke argument");
    } else {
        println!("destroying objects...");
        panic!("not implemented");
    }
}


