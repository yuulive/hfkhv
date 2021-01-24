

use clap;




fn main() {
    let matches = clap::App::new("pgfine")
        .version("0.1.0")
        .author("Marius Kavaliauskas <mariuskava@gmail.com>")
        .about("Yet another database migration tool")
        .subcommand(clap::App::new("init")
            .about("initializes new pgfine project"))
        .subcommand(clap::App::new("create")
            .about("creates fresh database"))
        .subcommand(clap::App::new("migrate")
            .about("updates database"))
        .subcommand(clap::App::new("destroy")
            .about("deletes all defined database objects")
            .arg(clap::Arg::new("--no-joke")
                .about("confirmation")))
        .get_matches();

    // more program logic goes here...

    
    match matches.subcommand() {
        Some(("init", _subcommand_matches)) => { 
            println!("init pgfine project");
        },
        Some(("create", _subcommand_matches)) => { 
            println!("not implemented");
        },
        Some(("migrate", _subcommand_matches)) => { 
            println!("not implemented");
        },
        Some(("destroy", _subcommand_matches)) => { 
            println!("not implemented");
        },
        _ => {
            
            unreachable!() // Assuming you've listed all direct children above, this is unreachable
        }
    }






}
