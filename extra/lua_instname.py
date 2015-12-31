#! /usr/bin/env python
def uleb128_encode(v):
    res = []
    while v > 0x80:
        res.append(chr(0x80|(v&0x7f)))
        v = v >> 7
    res.append(chr(v))
    return ''.join(res)

def uleb128_decode(s):
    v,k = 0,1
    for ofs, c in enumerate(s):
        c = ord(c)
        v += k * (c&0x7f)
        k = k << 7
        if c < 0x80:
            break
    return v, s[ofs+1:]

def luajit_bc_is_stripped(flags):
    return flags & 0x02

def luajit_bc_split(bc):
    header = bc[:4]
    flags, more = uleb128_decode(bc[4:])
    if luajit_bc_is_stripped(flags):
        src_name, body = None, more
    else:
        src_name_len, more = uleb128_decode(more)
        src_name = more[:src_name_len]
        body = more[src_name_len:]
    return header, flags, src_name, body

def luajit_bc_join(header, flags, src_name, body):
    if luajit_bc_is_stripped(flags):
        return ''.join((header, uleb128_encode(flags), body))
    else:
        return ''.join((
            header, uleb128_encode(flags),
            uleb128_encode(len(src_name)),
            src_name, body))

if __name__ == '__main__':
    import sys, argparse

    parser = argparse.ArgumentParser(
        description='Change source file name in compiled Lua bytecode')

    parser.add_argument('filename', metavar='FILE')
    parser.add_argument('-r', dest='new_name')

    args = parser.parse_args()

    if args.new_name:
        with open(args.filename, 'rb') as f:
            header, flags, src_name, body = luajit_bc_split(f.read())

        src_name = args.new_name

        with open(args.filename, 'wb') as f:
            f.truncate()
            f.write(luajit_bc_join(header, flags, src_name, body))
