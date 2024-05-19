# DR Feature Extraction tools

### loader.py
The script loads domain data the mongodb database database, apply the transformers and save the result to a file. The transformers are defined in the `transformers` directory. The `playground.ipynb` notebook contains some examples of how to use the data and train a model. Play with the notebooks or help extract data from the database. See also the README in the `transformers` directory for more information on how to write transformers. See the comments in `loader.py` for more information on how to run the loader and where to look next. Note: `.env` with Mongo connection string is required.

### lexical-processor.py
The script extracts lexical features from domain names inside a text file or multiple files. Desired label is specified by the `--label` parameter. The script can output to Parquet file, CSV, or JSON array.
Type `./lexical-processor.py -h` for details

### ngrams.py
This tool calculates ngram frequencies for predefined datasets od domain names.

### load_domains.py
This tool contact MongoDB and downloads only domain names from a given collection. The domain names are the stored into a text file of desired name.

