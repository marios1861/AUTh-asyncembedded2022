# asyncembedded2022

Implements the embedded2022 project asyncronously in rust

## Cross compiling

All that is needed to cross compile this project is for the
[aarch64-linux-musl-cross](https://musl.cc/aarch64-linux-musl-cross.tgz)
toolkit to be included in the project root and for the target `aarch64-unknown-linux-musl`
to be added with rustup

cross compile target binary with the command:

`$ cargo build --release --target aarch64-unknown-linux-musl`

## Executing

You can get usage information for the tool with the -h or --help command

### Example Usage

`$ ./asyncembedded2022 --token <YOUR FINNHUB API TOKEN> AAPL BTC`

This commmand will start the program which will create a directory `data` with individual files for each symbol.

## Note

The task delay calculate files calculate the computation -> write delay for each symbol separately
