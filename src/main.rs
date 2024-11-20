use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// A simple CLI to shard and reconstruct large files
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Shard a file into smaller parts
    Shard {
        /// The source file to shard
        #[arg(short, long)]
        input: PathBuf,

        /// The target directory to save shards
        #[arg(short, long)]
        output: PathBuf,

        /// Maximum shard size in bytes (default: 4 GiB)
        #[arg(short, long, default_value_t = 4 * 1024 * 1024 * 1024)]
        size: u64,
    },
    /// Reconstruct a file from its shards
    Reconstruct {
        /// The directory containing shards
        #[arg(short, long)]
        input: PathBuf,

        /// The output file to reconstruct
        #[arg(short, long)]
        output: PathBuf,
    },
}

fn shard_file(
    source: &Path,
    target_dir: &Path,
    max_size: u64,
) -> std::io::Result<()> {
    let file = File::open(source)?;
    let mut reader = BufReader::new(file);
    let mut buffer = vec![0u8; max_size as usize];
    let mut part = 0;

    std::fs::create_dir_all(target_dir)?;

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        let shard_path = target_dir.join(format!("shard_{:04}", part));
        let mut shard_file = BufWriter::new(File::create(&shard_path)?);
        shard_file.write_all(&buffer[..bytes_read])?;
        println!("Created shard: {}", shard_path.display());
        part += 1;
    }

    Ok(())
}

fn reconstruct_file(
    shard_dir: &Path,
    output: &Path,
) -> std::io::Result<()> {
    let mut output_file = BufWriter::new(File::create(output)?);

    let mut shards: Vec<_> = std::fs::read_dir(shard_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("shard_")
        })
        .collect();

    shards.sort_by_key(|entry| entry.file_name());

    for shard in shards {
        let shard_path = shard.path();
        let mut shard_file = BufReader::new(File::open(&shard_path)?);
        let mut buffer = Vec::new();
        shard_file.read_to_end(&mut buffer)?;
        output_file.write_all(&buffer)?;
        println!("Processed shard: {}", shard_path.display());
    }

    Ok(())
}

fn main() -> std::io::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Shard {
            input,
            output,
            size,
        } => {
            println!(
                "Sharding file {} into directory {} with max shard size {} bytes",
                input.display(),
                output.display(),
                size
            );
            shard_file(input, output, *size)
        }
        Commands::Reconstruct { input, output } => {
            println!(
                "Reconstructing file from shards in directory {} to {}",
                input.display(),
                output.display()
            );
            reconstruct_file(input, output)
        }
    }
}
