#!/bin/bash

# Default values
DEFAULT_OUTPUT_DIR="/home/ubuntu/data/hits/"
MAX_ROWS=10000000  # Limit the number of rows to 10 million

# Ensure at least one parameter (BATCH_SIZE) is provided
if [ $# -lt 1 ]; then
    echo -e "\nUsage: $0 BATCH_SIZE [OUTPUT_DIR]\n"
    echo "Example (default output dir): $0 10000"
    echo "Example (custom output dir): $0 10000 /custom/output/directory"
    exit 1
fi

# Assign parameters (BATCH_SIZE is mandatory, OUTPUT_DIR is optional)
BATCH_SIZE="$1"
OUTPUT_DIR="${2:-$DEFAULT_OUTPUT_DIR}"
SPLIT_DIR="${OUTPUT_DIR}/split/tabseparatedwithnames_${BATCH_SIZE}"

# Define constants
DATASET_URL="https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz"
TMP_FILE="$OUTPUT_DIR/hits_full.tsv"
TMP_CUT_FILE="$OUTPUT_DIR/hits_10M.tsv"  # New temp file for 10M random rows

# Hardcoded header (since dataset has no header)
HEADER="WatchID	JavaEnable	Title	GoodEvent	EventTime	EventDate	CounterID	ClientIP	RegionID	UserID	CounterClass	OS	UserAgent	URL	Referer	IsRefresh	RefererCategoryID	RefererRegionID	URLCategoryID	URLRegionID	ResolutionWidth	ResolutionHeight	ResolutionDepth	FlashMajor	FlashMinor	FlashMinor2	NetMajor	NetMinor	UserAgentMajor	UserAgentMinor	CookieEnable	JavascriptEnable	IsMobile	MobilePhone	MobilePhoneModel	Params	IPNetworkID	TraficSourceID	SearchEngineID	SearchPhrase	AdvEngineID	IsArtifical	WindowClientWidth	WindowClientHeight	ClientTimeZone	ClientEventTime	SilverlightVersion1	SilverlightVersion2	SilverlightVersion3	SilverlightVersion4	PageCharset	CodeVersion	IsLink	IsDownload	IsNotBounce	FUniqID	OriginalURL	HID	IsOldCounter	IsEvent	IsParameter	DontCountHits	WithHash	HitColor	LocalEventTime	Age	Sex	Income	Interests	Robotness	RemoteIP	WindowName	OpenerName	HistoryLength	BrowserLanguage	BrowserCountry	SocialNetwork	SocialAction	HTTPError	SendTiming	DNSTiming	ConnectTiming	ResponseStartTiming	ResponseEndTiming	FetchTiming	SocialSourceNetworkID	SocialSourcePage	ParamPrice	ParamOrderID	ParamCurrency 	ParamCurrencyID	OpenstatServiceName	OpenstatCampaignID	OpenstatAdID	OpenstatSourceID 	UTMSource 	UTMMedium	UTMCampaign	UTMContent	UTMTerm	FromTag	HasGCLID 	RefererHash	URLHash 	CLID"
# Ensure directories exist
mkdir -p "$OUTPUT_DIR"

# Remove SPLIT_DIR if it already exists, then recreate it
if [ -d "$SPLIT_DIR" ]; then
    echo "Removing existing directory: $SPLIT_DIR"
    rm -rf "$SPLIT_DIR"
fi

mkdir -p "$SPLIT_DIR"

echo -e "\n===================================="
echo "  Downloading Dataset"
echo "  Dataset Directory: $OUTPUT_DIR"
echo "  Batch Size: $BATCH_SIZE"
echo "===================================="

# Download the dataset if it doesn't exist
if [ ! -f "$OUTPUT_DIR/hits.tsv.gz" ]; then
    wget --no-verbose --continue -O "$OUTPUT_DIR/hits.tsv.gz" "$DATASET_URL"
else
    echo "File already exists. Skipping download."
fi

echo -e "\n===================================="
echo "  Extracting Dataset"
echo "===================================="

# Extract the TSV file if not already extracted
if [ ! -f "$TMP_FILE" ]; then
    gunzip -c "$OUTPUT_DIR/hits.tsv.gz" > "$TMP_FILE"
else
    echo "File already extracted. Skipping extraction."
fi

echo -e "\n===================================="
echo "  Selecting $MAX_ROWS Unique Random Rows"
echo "===================================="

# Check if the TMP_CUT_FILE already exists
if [ ! -f "$TMP_CUT_FILE" ]; then
    echo "Selecting 10 million unique random rows..."
    shuf "$TMP_FILE" | head -n "$MAX_ROWS" > "$TMP_CUT_FILE"
else
    echo "File $TMP_CUT_FILE already exists. Skipping row selection."
fi

echo -e "\n===================================="
echo "  Splitting File into $BATCH_SIZE-line Chunks"
echo "===================================="

# Define output file prefix
OUTPUT_PREFIX="$SPLIT_DIR/hits_part_"

# Split the extracted 10M rows file into BATCH_SIZE-sized numbered files
split -l "$BATCH_SIZE" -d -a 5 --additional-suffix=.tsv "$TMP_CUT_FILE" "$OUTPUT_PREFIX"

echo -e "\n===================================="
echo "  Prepending Hardcoded Header to Each File"
echo "===================================="

# Loop through split files and prepend the header
for file in "$OUTPUT_PREFIX"*.tsv; do
    temp_file="${file}.tmp"
    echo -e "$HEADER" > "$temp_file"
    cat "$file" >> "$temp_file"
    mv "$temp_file" "$file"
done


echo -e "\nExport completed."
echo "The full dataset is kept at: $TMP_FILE"
echo "The first 10M random rows are saved at: $TMP_CUT_FILE"
echo "Split files are saved in: $SPLIT_DIR"
echo "Example split files:"
ls -lh "$SPLIT_DIR" | head -10