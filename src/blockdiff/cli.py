import argparse
import hashlib
import sys
import time
import blockdiff.core as core


class Cli:
    def __init__(self):
        args = self.parse_args()

        self.input_map_filepath = args.input_map
        self.output_map_filename = '.blockmap'
        self.output_map_filepath = args.output_map

        self.output = None
        self.setup_output(args, self.output_map_filename)

        self.input = core.Input(filepath=args.input_file, blocksize=args.block_size)
        self.mapper = core.Mapper(self.input)
        self.processor = core.Processor(
            reader=self.input,
            mapper=self.mapper,
            hasher=lambda block: hashlib.sha1(block).hexdigest(),
            on_handle=self.on_block_handle,
        )
        self.is_verbose = args.verbose > 0
        self.start_time = time.time()

    def parse_args(self):
        p = argparse.ArgumentParser()
        p.add_argument('-f', '--input-file', required=True, help='input file')
        p.add_argument('-d', '--destination', required=True, help='output directory or tar file (depends of mode)')
        p.add_argument('-m', '--mode', default='tar', help='Mode: file, tar (default is uncompressed tar)')
        p.add_argument('-bs', '--block-size', type=int, default=65536, help='block size in bytes (default is 64kB)')
        p.add_argument('--input-map', help='input map file')
        p.add_argument('--output-map', help='output map file')
        p.add_argument('--verbose', type=int, default=0, help='verbose output')
        return p.parse_args()

    def setup_output(self, args, mapname):
        if args.mode == 'files':
            output_dir = args.destination.rstrip('/')
            self.input_map_filepath = args.input_map_filepath or f'{output_dir}/{mapname}'
            self.output_map_filepath = args.output_map_filepath or f'{output_dir}/{mapname}'
            self.output = core.FileOutput(outdir=output_dir, filenamer=lambda index: f'{index:010}.block')
        elif args.mode == 'tar':
            self.output = core.TarOutput(args.destination, filenamer=lambda index: f'{index:010}.block')
        else:
            raise ValueError(f'Unsupported mode "{args.mode}"')

    def exec(self):
        self.start_time = time.time()
        self.mapper.load_input_map(self.input_map_filepath)
        self.processor.handle_blocks()
        self.mapper.try_write_to_file(self.output_map_filepath)
        self.output.write_map(self.output_map_filename, self.mapper.serialize().encode('utf-8'))
        self.output.close()

        print('\n\nTotal stats:', end='')
        self.print_stat()

    def on_block_handle(self, block, index, is_changed):
        if is_changed:
            self.output.write_block(index, block)

        if self.is_verbose:
            ch = '+' if is_changed else '.'
            print(ch, end='')
            if (index + 1) % 80 == 0:
                self.print_stat()
            sys.stdout.flush()

    def print_stat(self):
        perc = 100 * self.input.get_progress()
        bps = self.input.bytes_read / max(1, time.time() - self.start_time)
        mbps = round(bps / (1024 * 1024), 2)
        print(f'{perc:>8.1f}% {mbps:>8} MB/s')


def run():
    Cli().exec()


if __name__ == '__main__':
    run()
