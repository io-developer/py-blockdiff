import json
import io
import os
import math
import tarfile


class Input:
    def __init__(self, filepath: str, blocksize: int):
        self.filepath = filepath
        self.blocksize = blocksize
        self.bytes_total = os.stat(self.filepath).st_size
        self.bytes_read = 0
        pass

    def get_progress(self):
        return self.bytes_read / max(1, self.bytes_total)

    def get_blocks_count(self):
        return math.floor(self.bytes_total / self.blocksize)

    def read_blocks(self):
        self.bytes_read = 0
        with open(self.filepath, 'rb') as f:
            while True:
                block = f.read(self.blocksize)
                self.bytes_read += self.blocksize
                self.bytes_read = min(self.bytes_read, self.bytes_total)
                if block:
                    yield block
                else:
                    return


class Output:
    def __init__(self):
        pass

    def write_block(self, index, data):
        pass

    def close(self):
        pass


class FileOutput(Output):
    def __init__(self, outdir: str, filenamer):
        super(FileOutput, self).__init__()
        self.outdir = outdir.rstrip('/')
        self.block_namer = filenamer

    def write_block(self, index, data):
        path = f'{self.outdir}/{self.block_namer(index)}'
        with open(path, 'wb') as f:
            f.write(data)

    def write_map(self, filename, data):
        path = f'{self.outdir}/{filename}'
        with open(path, 'wb') as f:
            f.write(data)


class TarOutput(Output):
    def __init__(self, outfile: str, filenamer):
        super(TarOutput, self).__init__()
        self.filenamer = filenamer
        self.tar = tarfile.open(name=outfile, mode='x')

    def write_block(self, index, data):
        info = tarfile.TarInfo(self.filenamer(index))
        info.size = len(data)
        self.tar.addfile(info, io.BytesIO(data))

    def write_map(self, filename, data):
        info = tarfile.TarInfo(filename)
        info.size = len(data)
        self.tar.addfile(info, io.BytesIO(data))

    def close(self):
        self.tar.close()


class Mapper:
    def __init__(self, reader: Input):
        self.reader = reader
        self.input_map = None
        self.map = {
            'version': 1,
            'blocksize': reader.blocksize,
            'totalbytes': reader.bytes_total,
            'hashes': {},
        }

    def validate(self):
        if not self.input_map:
            return
        if self.input_map['version'] != self.map['version']:
            raise ValueError('Input and output map "version" mismatch')
        if self.input_map['blocksize'] != self.map['blocksize']:
            raise ValueError('Input and output map "blocksize" mismatch')
        if self.input_map['totalbytes'] != self.map['totalbytes']:
            raise ValueError('Input and output map "totalbytes" mismatch')

    def load_input_map(self, filepath):
        self.input_map = None
        if filepath and os.path.isfile(filepath):
            with open(filepath, 'r') as f:
                self.input_map = json.load(f)
        self.validate()

    def try_write_to_file(self, filepath):
        if filepath:
            self.write_to_file(filepath)

    def write_to_file(self, filepath):
        with open(filepath, 'w') as f:
            f.write(self.serialize())

    def serialize(self):
        return json.dumps(self.map)

    def update_block_hash(self, index, hashstr):
        k = str(index)
        self.map['hashes'][k] = hashstr
        src_hash = self.input_map['hashes'].get(k) if self.input_map else None
        return src_hash != hashstr


class Processor:
    def __init__(self, reader: Input, mapper: Mapper, hasher, on_handle):
        self.reader = reader
        self.mapper = mapper
        self.hasher = hasher
        self.on_handle = on_handle
        pass

    def handle_blocks(self):
        index = -1
        for block in self.reader.read_blocks():
            index += 1
            hashstr = self.hasher(block)
            is_changed = self.mapper.update_block_hash(index, hashstr)
            self.on_handle(block, index, is_changed)
