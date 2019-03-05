## halcyon

_A mythical bird said by ancient writers to breed in a nest floating at sea at the winter solstice, charming the wind and waves into calm._

Halcyon is a Twitter dataset processor, that builds an in-memory database of the dataset and exports certain data as GPU-friendly CSV files.

### Usage

First, get the Rust compiler: https://www.rust-lang.org/tools/install. Then run the following command to build the executable.

```
git clone git@github.com:zirkular/halcyon.git
cd halcyon
cargo build
```

To run the application on a Twitter dataset using Cargo:

```
cargo run /path/to/twitter.csv
```
or by calling the executable directly:
```
halcyon /path/to/twitter.csv
```

#### Preprocessing

Halcyon can export the first N lines of an input file (helpful for development). It creates a file named `<filename>.N`, which results in `filename.csv.1000`:

```
halcyon filename.csv --export-raw 1000
```

### Contact
```
## halcyon ##

  /\ \  
 / /\ \ 
/ /__\ \
\/____\/

https://zirkular.io
http://000.graphics

```
