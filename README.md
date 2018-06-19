# Blockdiff
Fast fixed-size snapshot diff and writer / incremental backup. One pass for input read and on-fly diff + output

## Install
Required python 3.6 or higher

## Usage
Suppose exists two snaphots A and B with the same size. You have to determine which blocks are changed from A to B.

Lets output first level to .tar with 128 bytes per block resolution. Block-map we will save to 'files/level1.blockmap':
```bash
python3.6 src/cli.py -f files/1.txt -m tar -d files/level1.tar -bs 128 --output-map files/level1.blockmap --verbose 1
>
> +++++++++++++++++++++
>
> Total stats:   100.0%      0.0 MB/s
```

Now we can compare snapshot B with A and write to 'files/level2.tar' only changed blocks. Updated block-map we will save to 'files/level2.blockmap'
```bash
python3.6 src/cli.py -f files/2.txt -m tar -d files/level2.tar -bs 128 --input-map files/level1.blockmap --output-map files/level2.blockmap --verbose 1
>
> .........+.........+.
> 
> Total stats:   100.0%      0.0 MB/s
```

And so on..