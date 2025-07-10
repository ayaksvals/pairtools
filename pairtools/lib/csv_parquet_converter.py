import duckdb
import os
import copy
import subprocess
import sys
import json
import fire

UTIL_NAME = "pairtools_csv_parquet_converter"
COMMENT_CHAR = "#"
__version__ = "1.1.0"

#from pairtools.lib import append_new_pg, is_empty_header, _add_pg_to_samheader, insert_samheader, mark_header_as_sorted, _parse_pg_chains, _format_pg, extract_fields

def setup_duckdb_connection(temp_directory=None, memory_limit=None, enable_progress_bar=True, enable_profiling='json', numb_threads=16):
    """
    Sets up a DuckDB connection with specified parameters.

    Args:
        temp_directory (str): Path to the temporary directory for DuckDB.
        memory_limit (str): Memory limit for DuckDB operations (e.g., '55GB').
        enable_progress_bar (bool): Whether to enable the progress bar. Defaults to True.

    Returns:
        duckdb.DuckDBPyConnection: Configured DuckDB connection.
    """
    con = duckdb.connect(":memory:")
    
    if temp_directory is not None:
        con.execute(f"PRAGMA temp_directory='{temp_directory}';")

    if memory_limit is not None: 
        con.execute(f"PRAGMA memory_limit='{memory_limit}';")

    con.execute(f"SET enable_progress_bar = {'true' if enable_progress_bar else 'false'};")
    con.execute(f"PRAGMA enable_profiling = {enable_profiling};")

    con.execute(f"SET threads = {numb_threads};")
    return con

def get_output_file_path(input_file, output_dir, new_format='parquet'):
    """
    Generates the output file path in the specified output directory with the same name as the input file.

    Parameters:
        input_file (str): The path to the input file.
        output_dir (str): The path to the output directory.
        new_format (str): The new file format (e.g., 'parquet', 'csv', 'txt').

    Returns:
        str: The full path to the output file in the output directory.
    """
    # Extract the base name of the input file (without directory)
    input_file_name = os.path.basename(input_file)
    
    # Remove the original file extension
    base_name = os.path.splitext(input_file_name)[0]
    
    # Construct the new file name with the specified format
    new_file_name = f"{base_name}.{new_format}"
    
    # Construct the output file path
    output_file_path = os.path.join(output_dir, new_file_name)
    
    return output_file_path


def validate_output_path(output_path):
    if os.path.isdir(output_path):
        raise ValueError(f"Error: {output_path} is a directory, expected a file path.")

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(output_path):
        print(f"Warning: {output_path} already exists and will be overwritten.")




def get_header_subprocess(file_path: str, max_header_lines=1000) -> list:
    # Check if the file is gzipped by reading the first two bytes
    with open(file_path, 'rb') as f:
        is_gzipped = f.read(2) == b'\x1f\x8b'

    # Create the appropriate command based on the file type
    if is_gzipped:
        command = f"gzip -dc {file_path} | head -n {max_header_lines} | grep '^#' "
    else:
        command = f"head -n {max_header_lines} {file_path} | grep '^#' "

    # Execute the command and capture the output
    result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
    
    # Split the result into lines and return as a list
    return tuple(result.stdout.splitlines())



# From pairtools
def append_new_pg(header, ID="", PN="", VN=None, CL=None, force=False):
    header = copy.deepcopy(header)
    if is_empty_header(header):
        raise Exception("Input file is not valid .pairs, has no header or is empty.")
    samheader, other_header = extract_fields(header, "samheader", save_rest=True)
    new_samheader = _add_pg_to_samheader(samheader, ID, PN, VN, CL, force)
    new_header = insert_samheader(other_header, new_samheader)
    return new_header
    
def is_empty_header(header):
    if len(header) == 0:
        return True
    if not header[0].startswith("##"):
        return True
    else:
        return False

def _add_pg_to_samheader(samheader, ID="", PN="", VN=None, CL=None, force=False):
    """Append a @PG record to an existing sam header. If the header comes
    from a merged file and thus has multiple chains of @PG, append the
    provided PG to all of the chains, adding the numerical suffix of the
    branch to the ID.

    Parameters
    ----------

    header : list of str
    ID, PN, VN, CL : std
        The keys of a new @PG record. If absent, VN is the version of
        pairtools and CL is taken from sys.argv.
    force : bool
        If True, ignore the inconsistencies among @PG records of the existing
        header.

    Returns
    -------
    new_header : list of str
        A list of new headers lines, stripped of newline characters.
    """

    if VN is None:
        VN = __version__
    if CL is None:
        CL = " ".join(sys.argv)

    pre_pg_header = [
        line.strip()
        for line in samheader
        if line.startswith("@HD") or line.startswith("@SQ") or line.startswith("@RG")
    ]

    post_pg_header = [
        line.strip()
        for line in samheader
        if not line.startswith("@HD")
        and (not line.startswith("@SQ"))
        and (not line.startswith("@RG"))
        and (not line.startswith("@PG"))
    ]

    pg_chains = _parse_pg_chains(samheader, force=force)

    for i, br in enumerate(pg_chains):
        new_pg = {"ID": ID, "PN": PN, "VN": VN, "CL": CL}
        new_pg["PP"] = br[-1]["ID"]
        if len(pg_chains) > 1:
            new_pg["ID"] = new_pg["ID"] + "-" + str(i + 1) + "." + str(len(br) + 1)
        new_pg["raw"] = _format_pg(**new_pg)
        br.append(new_pg)

    new_header = (
        pre_pg_header + [pg["raw"] for br in pg_chains for pg in br] + post_pg_header
    )

    return new_header

def insert_samheader(header, samheader):
    """Insert samheader into header."""
    new_header = [l for l in header if not l.startswith("#columns")]
    if samheader:
        new_header += ["#samheader: " + l for l in samheader]
    new_header += [l for l in header if l.startswith("#columns")]
    return new_header

def mark_header_as_sorted(header):
    header = copy.deepcopy(header)
    if is_empty_header(header):
        raise Exception("Input file is not valid .pairs, has no header or is empty.")
    if not any([l.startswith("#sorted") for l in header]):
        if header[0].startswith("##"):
            header.insert(1, "#sorted: chr1-chr2-pos1-pos2")
        else:
            header.insert(0, "#sorted: chr1-chr2-pos1-pos2")
    for i in range(len(header)):
        if header[i].startswith("#chromosomes"):
            chroms = header[i][12:].strip().split(SEP_CHROMS)
            header[i] = "#chromosomes: {}".format(SEP_CHROMS.join(sorted(chroms)))
    return header

def _parse_pg_chains(header, force=False):

    pg_chains = []

    parsed_pgs = []
    for l in header:
        if l.startswith("@PG"):
            tag_value_pairs = l.strip().split("\t")[1:]
            if not all(":" in tvp for tvp in tag_value_pairs):
                warnings.warn(
                    f"Skipping the following @PG line, as it does not follow the SAM header standard of TAG:VALUE: {l}"
                )
                continue
            parsed_tvp = dict(
                [tvp.split(":", maxsplit=1) for tvp in tag_value_pairs if ":" in tvp]
            )
            if parsed_tvp:
                parsed_tvp["raw"] = l.strip()
                parsed_pgs.append(parsed_tvp)

    while True:
        if len(parsed_pgs) == 0:
            break

        for i in range(len(parsed_pgs)):
            pg = parsed_pgs[i]
            if "PP" not in pg:
                pg_chains.append([pg])
                parsed_pgs.pop(i)
                break
            else:
                matching_chains = [
                    branch for branch in pg_chains if branch[-1]["ID"] == pg["PP"]
                ]
                if len(matching_chains) > 1:
                    if force:
                        matching_chains[0].append(pg)
                        parsed_pgs.pop(i)
                        break

                    else:
                        raise ParseError(
                            "Multiple @PG records with the IDs identical to the PP field of another record:\n"
                            + "\n".join([br[-1]["raw"] for br in matching_chains])
                            + "\nvs\n"
                            + pg["raw"]
                        )

                if len(matching_chains) == 1:
                    matching_chains[0].append(pg)
                    parsed_pgs.pop(i)
                    break

            if force:
                pg_chains.append([pg])
                parsed_pgs.pop(i)
                break
            else:
                raise ParseError(
                    "Cannot find the parental @PG record for the @PG records:\n"
                    + "\n".join([pg["raw"] for pg in parsed_pgs])
                )

    return pg_chains

def _format_pg(**kwargs):
    out = ["@PG"] + [
        "{}:{}".format(field, kwargs[field])
        for field in ["ID", "PN", "CL", "PP", "DS", "VN"]
        if field in kwargs
    ]
    return "\t".join(out)

def extract_fields(header, field_name, save_rest=False):
    """
    Extract the specified fields from the pairs header and return
    a list of corresponding values, even if a single field was found.
    Additionally, can return the list of intact non-matching entries.
    """

    fields = []
    rest = []
    for l in header:
        if l.lstrip(COMMENT_CHAR).startswith(field_name + ":"):
            fields.append(l.split(":", 1)[1].rstrip('\n').lstrip())
        elif save_rest:
            rest.append(l)

    if save_rest:
        return fields, rest
    else:
        return fields

 

def extract_header_types(header):
    """
    Extract unique header types from a list of strings that start with '#' or '##'.

    Parameters:
        lines (list): List of strings to process.

    Returns:
        list: Sorted list of unique 'header_types' values.
    """
    header_types = set()  # To store unique values
    
    for line in header:
        header_type = line.split()[0].lstrip("#")[:-1]  # Remove # and last character
        if header_type!='': 
            header_types.add(header_type)

    # Return the unique values as a sorted list
    return list(header_types)


# Convert dict to json and reverse
def create_json_dict(header, header_types):
    """    
    return a dictionary with header types as keys and their JSON strings as values.
    Args:
        header (str): The initial header string to process.
        header_types (list): A list of header types to extract.
        
    Returns:
        dict: A dictionary where keys are header types or "format", and values are JSON strings of the extracted fields.
    """
    header_dict = {}
    header_rest = header
    for header_type in header_types:
        fields, header_rest = extract_fields(header_rest, header_type, True)
        if header_type == "columns":
            fields_in_type=fields[0].split()
        elif header_type == "chromsize":
            fields_in_type={k:int(v) for k,v in [i.split(' ') for i in fields]}
        elif header_type == "samheader":
            print("samheader", repr(fields))
            fields_in_type = fields
        elif header_type=="sorted" or header_type=="shape" or header_type=="genome_assembly":
            fields_in_type = fields[0]
        else:
            fields_in_type = fields
        
        json_header_string = json.dumps(fields_in_type)
        json_check=json.loads(json_header_string)
        header_dict[header_type] = json_header_string

    if header_rest:
        fields_in_type = header_rest[0]
        json_header_string = json.dumps(fields_in_type)
        header_dict["format"] = json_header_string

    return header_dict

# convert to key_values for kv_metadata
def generate_kv_metadata(header_dict):
    """
    Generate a formatted metadata string from a dictionary.

    Args:
        header_dict (dict): A dictionary containing key-value pairs to format.

    Returns:
        str: A string where each key-value pair is formatted as `key: 'escaped_value'`,
             joined by a comma and newline.
    """
    kv_metadata = {k.lstrip('# ').replace(':', '').replace(' ', ''):v for k,v in header_dict.items()}
    json_check=json.loads(kv_metadata["samheader"])
    print(repr(f"Json check kv samheader: {json_check}"))
    return kv_metadata



# Extract from the file neccessary things: 

def extract_chromsizes(file_path: str, max_header_lines=1000) -> list:
    # Check if the file is gzipped by reading the first two bytes
    with open(file_path, 'rb') as f:
        is_gzipped = f.read(2) == b'\x1f\x8b'

    # Create the appropriate command based on the file type
    if is_gzipped:
        command = f"gzip -dc {file_path} | head -n {max_header_lines} | grep '^#chromsize' | awk '{{print $2}}'"
    else:
        command = f"head -n {max_header_lines} {file_path} | grep '^#chromsize' | awk '{{print $2}}'"

    # Execute the command and capture the output
    result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
    
    # Split the result into lines and return as a list
    return tuple(result.stdout.splitlines())

def extract_header_length(file_path: str, max_header_lines=1000) -> int:
    # Check if the file is gzipped by reading the first two bytes
    with open(file_path, 'rb') as f:
        is_gzipped = f.read(2) == b'\x1f\x8b'

    # Create the appropriate command based on the file type
    if is_gzipped:
        command = f"gzip -dc {file_path} | head -n {max_header_lines} | grep -c '^#'"
    else:
        command = f"head -n {max_header_lines} {file_path} | grep -c '^#'"

    # Execute the command and capture the output
    result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
    
    # Split the result into lines and return as a list
    return int(result.stdout.strip())

def extract_column_names(file_path: str, max_header_lines=1000) -> list:
    # Check if the file is gzipped by reading the first two bytes
    with open(file_path, 'rb') as f:
        is_gzipped = f.read(2) == b'\x1f\x8b'

    # Create the appropriate command based on the file type
    if is_gzipped:
        command = f"gzip -dc {file_path} |  head -n 1000 | grep '^#columns' "
    else:
        command = f"head -n 1000 {file_path} | grep '^#columns' "

    # Execute the command and capture the output
    result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
    
    # Split the result into lines and return as a list
    lines = result.stdout.splitlines()
    return lines[0].split()[1:]

def get_pairs_type():
    """
    Returns a tuple of all valid pair types.

    Returns:
        tuple: A tuple containing pair type strings.
    """
    pairs__type = (
        'UU', 'Uu', 'Um', 'UM', 'UR', 'UN',
        'uU', 'uu', 'um', 'uM', 'uR', 'uN',
        'mU', 'mu', 'mm', 'mM', 'mR', 'mN',
        'MU', 'Mu', 'Mm', 'MM', 'MR', 'MN',
        'RU', 'Ru', 'Rm', 'RM', 'RR', 'RN',
        'NU', 'Nu', 'Nm', 'NM', 'NR', 'NN',
        'DD', 'WW', 'XX'
    )
    return pairs__type





def setup_duckdb_types(con, chromosom_type):
    """
    Sets up ENUM types in a DuckDB connection.

    Args:
        con (duckdb.DuckDBPyConnection): The DuckDB connection.
        chromosom_type (tuple): The ENUM values for CHROM_TYPE.
        pairs__type (tuple): The ENUM values for PAIRS_TYPE.

    Returns:
        None
    """
    # Drop existing types if they exist
    con.execute("DROP TYPE IF EXISTS CHROM_TYPE")
    con.execute("DROP TYPE IF EXISTS READS_TYPE")
    con.execute("DROP TYPE IF EXISTS STRAND_TYPE")
    con.execute("DROP TYPE IF EXISTS PAIRS_TYPE")

    pairs__type = get_pairs_type()

    # Create new ENUM types
    con.execute(f"CREATE TYPE CHROM_TYPE AS ENUM {chromosom_type};")
    # con.execute("CREATE TYPE READS_TYPE AS ENUM ('.');")  # Uncomment if needed
    con.execute("CREATE TYPE STRAND_TYPE AS ENUM ('+', '-');")
    con.execute(f"CREATE TYPE PAIRS_TYPE AS ENUM {pairs__type};")
    return con


DTYPES_PAIRSAM = {
    "readID": str,
    "chrom1": str,
    "pos1": int,
    "chrom2": str,
    "pos2": int,
    "strand1": str,
    "strand2": str,
    "pair_type": str,
    "sam1": str,
    "sam2": str,
}

DTYPES_EXTRA_COLUMNS = {
    "mapq": int,
    "pos5": int,
    "pos3": int,
    "cigar": str,
    "read_len": int,
    "matched_bp": int,
    "algn_ref_span": int,
    "algn_read_span": int,
    "dist_to_5": int,
    "dist_to_3": int,
    "seq": str,
    "mismatches": str,
    "read_side": int,
    "algn_idx": int,
    "same_side_algn_count": int,
}


def extra_columns_str_check(column):
    """
    Check whether the input column contains any substring from DTYPES_EXTRA_COLUMNS where the type is str.

    Args:
        column (str): The column name to check.

    Returns:
        bool: True if any key from DTYPES_EXTRA_COLUMNS (of type str) is a substring of the column, False otherwise.
    """
    # Extract column names from DTYPES_EXTRA_COLUMNS where the type is str
    extra_columns_str = [col for col, dtype in DTYPES_EXTRA_COLUMNS.items() if dtype == str]
    return any(extra_col in column for extra_col in extra_columns_str)


def classify_column_types_by_name(column_names):
    """
    Classify columns based on predefined rules and types.

    Args:
        column_names (list): A list of column names to classify.
        column_types (dict): An empty dictionary to store the classification.

    Returns:
        dict: The updated column_types dictionary with classifications.
    """

    column_types = {}
    for col in column_names:
        if col in ["chrom1", "chrom2"]:
            column_types[col] = "CHROM_TYPE"
        elif col in ["strand1", "strand2"]:
            column_types[col] = "STRAND_TYPE"
        elif col == "pair_type":
            column_types[col] = "PAIRS_TYPE"
        elif col in DTYPES_PAIRSAM:
            column_types[col] = "INTEGER" if DTYPES_PAIRSAM[col] == int else "STRING"
        elif extra_columns_str_check(col):
            column_types[col] = "STRING"
        else:
            column_types[col] = "INTEGER"

    return column_types


# Check metadata in new parquet
def decode_and_parse_json(blob_value): 
    """
    Decodes a string with escape sequences and attempts to parse it as JSON.
    
    Args:
        blob_value (str): The string to decode and parse.
        
    Returns:
        dict | list | str: Parsed JSON object or error message.
    """
    try:
        # Decode escape sequences (e.g., \x22 -> ")
        decoded_str = blob_value.encode('utf-8').decode('unicode_escape')
    except UnicodeDecodeError as e:
        return f"Unicode decode error: {e}, original value: {blob_value}"

    try:
        # Parse the decoded string as JSON
        return json.loads(decoded_str)
    except json.JSONDecodeError as e:
        # Log or handle the error case
        print("EROOR")
        error_message = (
            f"JSONDecodeError: {e.msg} at line {e.lineno}, column {e.colno}. "
            f"Problematic string: {decoded_str}"
        )
        # Optionally log this error instead of returning
        # e.g., logging.error(error_message)
        return error_message
def extract_and_decode_parquet_metadata_duckdb(input_file):
    # Query to extract key-value metadata
    query = f"""
    SELECT key::TEXT AS key, value::TEXT AS value
    FROM parquet_kv_metadata('{input_file}');
    """
    metadata = duckdb.query(query).fetchdf()



    # Apply decoding to all values
    metadata['parsed_value'] = metadata['value'].apply(decode_and_parse_json)

    # Display the results
    return (metadata[['key', 'parsed_value']])
    

# Example usage
def test():
    con = setup_duckdb_connection('/users/slavska.olesia/scratch/slavska.olesia', '95GB')

    input_file = '/groups/goloborodko/seqdata/hsiehTijan2021/mm10/distiller_0.3.3/results/pairs_library/Micro-C_CTCF_UT_rep1.mm10.nodups.pairs.gz' #2.7G
    #input_file = '/users/slavska.olesia/projects/lesia/Micro-C_RAD21_IAA_rep4.mm10.nodups.pairs.gz' #7.7G
    output_file = get_output_file_path(input_file, '/groups/goloborodko/projects/lesia')
    print(f"Output_file: {output_file}")


    header = get_header_subprocess(input_file)
    header_pg = append_new_pg(header, ID=UTIL_NAME, PN=UTIL_NAME)
    new_header = header_pg

    header_types = extract_header_types(new_header)

    header_json_dict=create_json_dict(new_header, header_types)
    kv_metadata=generate_kv_metadata(header_json_dict)


    chromosom_type = extract_chromsizes(input_file)
    header_length = extract_header_length(input_file)
    column_names = extract_column_names(input_file)
    #pairs__type = get_pairs_type()

    con=setup_duckdb_types(con, chromosom_type)

    column_types=classify_column_types_by_name(column_names)

    print(column_types)

    #      ,KV_METADATA {kv_dict} 

    # Order By chrom1, chrom2, pos1, pos2
            
    code = f"""
        COPY (
            SELECT *
            FROM read_csv_auto('{input_file}', skip=1000, columns = {column_types})
            ) TO '{output_file}' (
                FORMAT PARQUET, ROW_GROUP_SIZE 100000
                        );
            """

    code = f"""
        COPY (
            SELECT *
            FROM read_csv_auto('{input_file}', skip={header_length}, columns = {column_types})
            ) TO '{output_file}' (
                FORMAT PARQUET
                        );
            """

    # Perfect on 2.7G
    code = f"""
        COPY (
            SELECT *
            FROM read_csv('{input_file}', delim='\t', skip={header_length}, columns = {column_types}, header=false, auto_detect=false)
            ) TO '{output_file}' (
                FORMAT PARQUET
                        );
            """
    
    # Perfect with Metadata
    code = f"""
        COPY (
            SELECT *
            FROM read_csv('{input_file}', delim='\t', skip={header_length}, columns = {column_types}, header=false, auto_detect=false)
            ) TO '{output_file}' (
                FORMAT PARQUET, KV_METADATA {kv_metadata} 
                        );
            """
    
    con.execute(code)

def csv_parquet_converter(
    input_path: str,
    output_path: str,
    temp_directory: str = None,
    memory_limit: str=None,
    enable_progress_bar: bool = True,
    enable_profiling: str = 'no_output',
    max_header_lines: int = 1000,
    numb_threads: int = 16,
):
    # Step 1: Setup DuckDB connection
    con = setup_duckdb_connection(temp_directory, memory_limit, enable_progress_bar, enable_profiling, numb_threads)

    # Step 2: Generate output file path
    validate_output_path(output_path)

    # Step 3: Extract, modify and prepare header
    header = get_header_subprocess(input_path, max_header_lines)
    header_pg = append_new_pg(header, ID=UTIL_NAME, PN=UTIL_NAME)
    
    new_header = header_pg
    #print(new_header)
    header_types = extract_header_types(new_header)
    #print(header_types)

    header_json_dict = create_json_dict(new_header, header_types)
    #print(header_json_dict)

    kv_metadata = generate_kv_metadata(header_json_dict)
    #print(kv_metadata)

    # Step 4: Prepare data for ENUM types
    chromosom_type = extract_chromsizes(input_path)
    header_length = extract_header_length(input_path)
    column_names = extract_column_names(input_path)

    # Step 5: Setup DuckDB types
    con = setup_duckdb_types(con, chromosom_type)
    column_types = classify_column_types_by_name(column_names)

    # Step 6: Prepare and execute DuckDB query
    code = f"""
        COPY (
            SELECT *
            FROM read_csv('{input_path}', delim='\t', skip={header_length}, columns = {column_types}, header=false, auto_detect=false)
        ) TO '{output_path}' (
            FORMAT PARQUET, KV_METADATA {kv_metadata}
        );
    """
    con.execute(code)


def csv_sort_parquet_converter_default(
    input_path: str,
    output_path: str,
    temp_directory: str = None,
    memory_limit: str=None,
    enable_progress_bar: bool = True,
    enable_profiling: str = 'no_output',
    max_header_lines: int = 1000,
    numb_threads: int = 16,
):
    # Step 1: Setup DuckDB connection
    con = setup_duckdb_connection(temp_directory, memory_limit, enable_progress_bar, enable_profiling, numb_threads)

    # Step 2: Generate output file path
    validate_output_path(output_path)

    # Step 3: Extract, modify and prepare header
    header = get_header_subprocess(input_path, max_header_lines)
    header_pg = append_new_pg(header, ID=UTIL_NAME, PN=UTIL_NAME)
    
    new_header = header_pg
    #print(new_header)
    header_types = extract_header_types(new_header)
    #print(header_types)

    header_json_dict = create_json_dict(new_header, header_types)
    #print(header_json_dict)

    kv_metadata = generate_kv_metadata(header_json_dict)
    #print(kv_metadata)

    # Step 4: Prepare data for ENUM types
    chromosom_type = extract_chromsizes(input_path)
    header_length = extract_header_length(input_path)
    column_names = extract_column_names(input_path)

    # Step 5: Setup DuckDB types
    con = setup_duckdb_types(con, chromosom_type)
    column_types = classify_column_types_by_name(column_names)

    # Step 6: Prepare and execute DuckDB query
    code = f"""
        COPY (
            SELECT *
            FROM read_csv('{input_path}', delim='\t', skip={header_length}, columns = {column_types}, header=false, auto_detect=false)
            ORDER BY chrom1, chrom2, pos1, pos2
        ) TO '{output_path}' (
            FORMAT PARQUET, KV_METADATA {kv_metadata}
        );
    """
    con.execute(code)


if __name__ == "__main__":
        fire.Fire()